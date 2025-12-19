#!/usr/bin/env python3
"""
Backfill Unix timestamp date fields into existing Pinecone records.

Why:
- Pinecone range filters ($gt/$gte/$lt/$lte) only work on numeric metadata.
- We now upsert *_ts fields for new records, but existing records need a one-time backfill.

What it does:
- Iterates all vector IDs in a namespace via index.list() (safe pagination).
- Fetches metadata for each ID batch.
- Computes:
    - deal_creation_date_ts from deal_creation_date
    - contract_start_ts from contract_start
    - contract_end_ts from contract_end
- Writes only missing timestamp fields using index.update(set_metadata=...).

Usage:
  python scripts/backfill_date_timestamps.py \
    --index npi-deal-data \
    --namespace sf-export-aug15-2025 \
    --batch-size 200 \
    --resume

Dry run (no writes):
  python scripts/backfill_date_timestamps.py --index npi-deal-data --namespace sf-export-aug15-2025 --dry-run --limit 1000
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from dotenv import load_dotenv

# Ensure project modules are importable
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config.progress_logger import ProcessingProgressLogger  # type: ignore
from connectors.pinecone_client import PineconeDocumentClient, _parse_date_to_unix_ts  # type: ignore


load_dotenv()


@dataclass
class BackfillStats:
    processed: int = 0
    updated: int = 0
    skipped_already_has_ts: int = 0
    skipped_no_parseable_dates: int = 0
    errors: int = 0


def _default_state_path(index_name: str, namespace: str) -> Path:
    safe = f"{index_name}__{namespace}".replace("/", "_").replace("-", "_")
    return ROOT_DIR / f"date_ts_backfill_state_{safe}.json"


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)


def _collect_ids(pc_client: PineconeDocumentClient, namespace: str, limit: int) -> List[str]:
    collected: List[str] = []
    for id_or_ids in pc_client.index.list(namespace=namespace, limit=100):
        if isinstance(id_or_ids, str):
            collected.append(id_or_ids)
        else:
            try:
                collected.extend(list(id_or_ids))
            except Exception:
                # best-effort
                collected.append(str(id_or_ids))
        if limit > 0 and len(collected) >= limit:
            return collected[:limit]
    return collected if limit == 0 else collected[:limit]


def _normalize_deal_id(raw: Any) -> str:
    s = (str(raw).strip() if raw is not None else "").strip()
    if not s or s.lower() in ("none", "nan", "null"):
        return ""
    return s


def _fetch_batch(
    pc_client: PineconeDocumentClient,
    namespace: str,
    ids: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    if not ids:
        return {}
    # NOTE: fetch() uses a GET request under the hood in this SDK; too many IDs can
    # trigger a 414 "Request-URI Too Large". Keep batches small.
    FETCH_MAX_IDS = 50
    out: Dict[str, Dict[str, Any]] = {}

    for start in range(0, len(ids), FETCH_MAX_IDS):
        sub_ids = list(ids[start:start + FETCH_MAX_IDS])
        fetched = pc_client.index.fetch(ids=sub_ids, namespace=namespace)
        vectors_map = getattr(fetched, "vectors", None) or fetched.get("vectors", {})
        for vid in sub_ids:
            v = vectors_map.get(vid, {})
            md = getattr(v, "metadata", None) or v.get("metadata", {}) or {}
            out[vid] = md

    return out


def _extract_deal_keys_from_metadata(md: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (deal_id, salesforce_deal_id) from metadata.
    Note: In this namespace, deal_id can be numeric; salesforce_deal_id starts with a0W...
    """
    deal_id = _normalize_deal_id(md.get("deal_id"))
    sf_id = _normalize_deal_id(md.get("salesforce_deal_id"))
    return deal_id, sf_id


def _sample_deals_from_namespace(
    pc_client: PineconeDocumentClient,
    namespace: str,
    *,
    sample_deals: int,
    scan_ids_limit: int = 5000,
) -> List[str]:
    """
    Find N distinct deals by scanning a small number of vectors (fast, no full scan).
    We prefer salesforce_deal_id when present.
    """
    deals: List[str] = []
    seen: Set[str] = set()

    ids = _collect_ids(pc_client, namespace, scan_ids_limit)
    for i in range(0, len(ids), 200):
        batch = ids[i:i + 200]
        md_map = _fetch_batch(pc_client, namespace, batch)
        for md in md_map.values():
            deal_id, sf_id = _extract_deal_keys_from_metadata(md)
            key = sf_id or deal_id
            if not key:
                continue
            if key not in seen:
                seen.add(key)
                deals.append(key)
                if len(deals) >= sample_deals:
                    return deals

    return deals


def _vector_ids_for_deal(
    pc_client: PineconeDocumentClient,
    namespace: str,
    deal_key: str,
    *,
    max_ids: int = 5000,
) -> List[str]:
    """
    Collect vector IDs for a deal using the best available method:
    - Prefer fetch_by_metadata (API 2025-10; not always available in SDK/env).
    - Fallback: filter-only query with zero-vector (returns top_k matches under filter).

    Note: Fallback is sufficient for small testing but is not guaranteed to retrieve
    *every* chunk for very large deals.
    """
    deal_key = _normalize_deal_id(deal_key)
    if not deal_key:
        return []

    # Schema-adaptive: treat a0W* as salesforce_deal_id, otherwise deal_id.
    filter_dict: Dict[str, Any]
    if deal_key.startswith("a0W"):
        filter_dict = {"salesforce_deal_id": {"$eq": deal_key}}
    else:
        filter_dict = {"deal_id": {"$eq": deal_key}}

    # Try early-access fetch_by_metadata if available on the index object
    fetch_by_md = getattr(pc_client.index, "fetch_by_metadata", None)
    if callable(fetch_by_md):
        try:
            res = fetch_by_md(namespace=namespace, filter=filter_dict, limit=min(max_ids, 10000))
            vectors_map = getattr(res, "vectors", None) or res.get("vectors", {})
            return list(vectors_map.keys())
        except Exception:
            # Fall back to query
            pass

    # Fallback: filter-only query. Zero vector on dotproduct makes scores all 0 → arbitrary subset.
    try:
        q = pc_client.index.query(
            namespace=namespace,
            vector=[0.0] * 1024,
            top_k=max_ids,
            include_metadata=False,
            filter=filter_dict,
        )
        matches = getattr(q, "matches", None) or q.get("matches", [])
        ids: List[str] = []
        for m in matches:
            mid = getattr(m, "id", None) or (m.get("id") if isinstance(m, dict) else None)
            if mid:
                ids.append(str(mid))
        return ids
    except Exception:
        return []


def _build_ts_updates(md: Dict[str, Any]) -> Dict[str, int]:
    updates: Dict[str, int] = {}

    # Skip if already present (don’t overwrite unless asked later)
    if "deal_creation_date_ts" not in md:
        ts = _parse_date_to_unix_ts(md.get("deal_creation_date"))
        if ts is not None:
            updates["deal_creation_date_ts"] = int(ts)

    if "contract_start_ts" not in md:
        ts = _parse_date_to_unix_ts(md.get("contract_start"))
        if ts is not None:
            updates["contract_start_ts"] = int(ts)

    if "contract_end_ts" not in md:
        ts = _parse_date_to_unix_ts(md.get("contract_end"))
        if ts is not None:
            updates["contract_end_ts"] = int(ts)

    return updates


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Pinecone date *_ts metadata fields.")
    parser.add_argument("--index", default=os.getenv("PINECONE_INDEX_NAME", "npi-deal-data"))
    parser.add_argument("--namespace", default=os.getenv("PINECONE_NAMESPACE", "sf-export-aug15-2025"))
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--limit", type=int, default=0, help="Max vectors to process (0 = all)")
    parser.add_argument(
        "--sample-deals",
        type=int,
        default=0,
        help="If set, only process vectors for N sampled deals (fast test mode).",
    )
    parser.add_argument(
        "--deal-keys",
        type=str,
        default="",
        help='Comma-separated deal keys to process (prefer salesforce_deal_id like "a0W..."; else deal_id).',
    )
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between update calls")

    args = parser.parse_args()

    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        raise SystemExit("PINECONE_API_KEY not set in environment (.env)")

    state_path = _default_state_path(args.index, args.namespace)
    state: Dict[str, Any] = _load_state(state_path) if args.resume else {}
    start_index = int(state.get("next_index", 0)) if state else 0

    pc_client = PineconeDocumentClient(api_key=api_key, index_name=args.index)

    # Optional targeting: 10 deals test mode
    targeted_ids: List[str] = []
    targeted_deals: List[str] = []

    if args.deal_keys.strip():
        targeted_deals = [x.strip() for x in args.deal_keys.split(",") if x.strip()]
    elif args.sample_deals and args.sample_deals > 0:
        targeted_deals = _sample_deals_from_namespace(pc_client, args.namespace, sample_deals=args.sample_deals)

    if targeted_deals:
        # Collect vector IDs for each deal, dedupe
        all_ids: List[str] = []
        for dk in targeted_deals:
            deal_ids = _vector_ids_for_deal(pc_client, args.namespace, dk, max_ids=5000)
            all_ids.extend(deal_ids)
        targeted_ids = sorted(set(all_ids))
        ids = targeted_ids if (args.limit == 0) else targeted_ids[: args.limit]
        print(f"\n🎯 Target deals ({len(targeted_deals)}):")
        for d in targeted_deals:
            print(f"  - {d}")
        print(f"📦 Target vectors: {len(ids):,}\n")
    else:
        ids = _collect_ids(pc_client, args.namespace, args.limit)
    total = len(ids)

    operation_name = f"date_ts_backfill__{args.index}__{args.namespace}"
    progress_logger = ProcessingProgressLogger(
        operation_name=operation_name,
        total_items=total,
        dataset_name="vectors",
    )

    stats = BackfillStats(
        processed=int(state.get("processed", 0) or 0),
        updated=int(state.get("updated", 0) or 0),
        skipped_already_has_ts=int(state.get("skipped_already_has_ts", 0) or 0),
        skipped_no_parseable_dates=int(state.get("skipped_no_parseable_dates", 0) or 0),
        errors=int(state.get("errors", 0) or 0),
    )

    if start_index:
        progress_logger.update_progress(
            increment=min(start_index, total),
            custom_message=f"Resuming at index {start_index:,}/{total:,}",
        )
    if targeted_deals:
        progress_logger.log_processing_detail(
            f"🎯 Targeting {len(targeted_deals)} deals → {total:,} vectors (sample_deals={args.sample_deals})"
        )

    i = start_index
    while i < total:
        batch_ids = ids[i:i + args.batch_size]
        try:
            md_map = _fetch_batch(pc_client, args.namespace, batch_ids)
        except Exception as e:
            stats.errors += 1
            progress_logger.log_processing_detail(f"❌ fetch failed at i={i}: {e}")
            # skip batch
            i += len(batch_ids)
            continue

        for vid in batch_ids:
            md = md_map.get(vid, {}) or {}
            updates = _build_ts_updates(md)

            if not updates:
                # Already has all *_ts OR nothing parseable
                already = all(
                    k in md for k in ("deal_creation_date_ts", "contract_start_ts", "contract_end_ts")
                )
                if already:
                    stats.skipped_already_has_ts += 1
                else:
                    stats.skipped_no_parseable_dates += 1
                stats.processed += 1
                continue

            if not args.dry_run:
                try:
                    pc_client.index.update(
                        id=vid,
                        namespace=args.namespace,
                        set_metadata=updates,
                    )
                    if args.sleep > 0:
                        time.sleep(args.sleep)
                except Exception as e:
                    stats.errors += 1
                    progress_logger.log_processing_detail(f"❌ update failed id={vid}: {e}")
                    stats.processed += 1
                    continue

            stats.updated += 1
            stats.processed += 1

        i += len(batch_ids)
        progress_logger.update_progress(
            increment=len(batch_ids),
            custom_message=(
                f"processed={stats.processed:,} updated={stats.updated:,} "
                f"skipped_has_ts={stats.skipped_already_has_ts:,} "
                f"skipped_no_dates={stats.skipped_no_parseable_dates:,} errors={stats.errors:,}"
            ),
        )

        if args.resume:
            _save_state(
                state_path,
                {
                    "next_index": i,
                    "processed": stats.processed,
                    "updated": stats.updated,
                    "skipped_already_has_ts": stats.skipped_already_has_ts,
                    "skipped_no_parseable_dates": stats.skipped_no_parseable_dates,
                    "errors": stats.errors,
                    "index": args.index,
                    "namespace": args.namespace,
                },
            )

    progress_logger.log_processing_detail(
        f"✅ Done. processed={stats.processed:,} updated={stats.updated:,} "
        f"skipped_has_ts={stats.skipped_already_has_ts:,} "
        f"skipped_no_dates={stats.skipped_no_parseable_dates:,} errors={stats.errors:,} "
        f"dry_run={args.dry_run}"
    )
    print(
        "\n✅ Backfill complete\n"
        f"- processed: {stats.processed:,}\n"
        f"- updated: {stats.updated:,}\n"
        f"- skipped (already has *_ts): {stats.skipped_already_has_ts:,}\n"
        f"- skipped (no parseable dates): {stats.skipped_no_parseable_dates:,}\n"
        f"- errors: {stats.errors:,}\n"
        f"- dry_run: {args.dry_run}\n"
    )


if __name__ == "__main__":
    main()


