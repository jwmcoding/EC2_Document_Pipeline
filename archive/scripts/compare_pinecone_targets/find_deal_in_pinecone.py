#!/usr/bin/env python3
"""
Find a deal in a Pinecone index/namespace using metadata filters (no full scans).

Why: Pinecone filters are exact-match; "deal-68624" might be stored as:
- deal_id: "DEAL-68624" (or similar)
- deal_number: "68624"
- deal_name: "Deal-68624"

This helper tries a set of likely variants and returns matching records (unique by file_name/deal_id).
"""

import argparse
import os
import sys
from typing import Any, Dict, List, Optional

# Add src to path (script is in scripts/compare_pinecone_targets/, need to go up 2 levels)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(script_dir, "..", "..")
sys.path.insert(0, project_root)
sys.path.insert(0, script_dir)

from dotenv import load_dotenv

try:
    load_dotenv()
except PermissionError:
    pass

from src.connectors.pinecone_client import PineconeDocumentClient, _sanitize_str


def sanitize_str(v: Any, default: str = "") -> str:
    return _sanitize_str(v, default)


def dummy_vector_for_index(client: PineconeDocumentClient) -> List[float]:
    try:
        stats = client.get_index_stats() or {}
        dim = int(stats.get("dimension") or 1024)
    except Exception:
        dim = 1024
    return [0.0] * dim


def build_filters_for_deal(deal_input: str) -> List[Dict[str, Any]]:
    """
    Create a small set of candidate filters (exact match only).
    """
    raw = sanitize_str(deal_input).strip()
    raw_upper = raw.upper()
    raw_lower = raw.lower()

    # Extract trailing digits if present (for deal_number)
    digits = "".join(ch for ch in raw if ch.isdigit())

    # Common encodings in this repo
    candidates = []
    for x in [raw, raw_upper, raw_lower]:
        if x:
            candidates.append(x)
    if digits:
        candidates.append(digits)
    if digits and not raw_upper.startswith("DEAL-"):
        candidates.append(f"DEAL-{digits}")
    if digits and not raw.startswith("Deal-"):
        candidates.append(f"Deal-{digits}")

    # De-dupe while preserving order
    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            uniq.append(c)

    # Build $or filters across likely fields
    # Note: We keep these separate (one filter per query) to simplify debugging when a field is absent.
    filters = []
    for val in uniq:
        filters.append({"deal_id": {"$eq": val}})
        filters.append({"deal_number": {"$eq": val}})
        filters.append({"deal_name": {"$eq": val}})
        filters.append({"salesforce_deal_id": {"$eq": val}})
    return filters


def run_filtered_query(
    client: PineconeDocumentClient,
    namespace: str,
    filter_obj: Dict[str, Any],
    top_k: int,
) -> List[Dict[str, Any]]:
    vec = dummy_vector_for_index(client)
    results = client.index.query(
        namespace=namespace,
        vector=vec,
        top_k=top_k,
        include_metadata=True,
        filter=filter_obj,
    )

    out: List[Dict[str, Any]] = []
    for m in results.matches:
        md = m.metadata if hasattr(m, "metadata") else {}
        md = md or {}
        out.append(
            {
                "id": getattr(m, "id", ""),
                "file_name": sanitize_str(md.get("file_name", "")),
                "file_type": sanitize_str(md.get("file_type", "")),
                "deal_id": sanitize_str(md.get("deal_id", "")),
                "deal_number": sanitize_str(md.get("deal_number", "")),
                "deal_name": sanitize_str(md.get("deal_name", "")),
                "salesforce_deal_id": sanitize_str(md.get("salesforce_deal_id", "")),
                "vendor_name": sanitize_str(md.get("vendor_name", "")),
                "client_name": sanitize_str(md.get("client_name", "")),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Find a deal in Pinecone using metadata filters (no scans).")
    parser.add_argument("--index", required=True, help="Pinecone index name")
    parser.add_argument("--namespace", required=True, help="Pinecone namespace")
    parser.add_argument("--deal", required=True, help="Deal identifier (e.g., deal-68624, DEAL-68624, 68624)")
    parser.add_argument("--top-k", type=int, default=50, help="top_k per query (default: 50)")
    parser.add_argument("--max-results", type=int, default=25, help="Max unique results to print (default: 25)")
    args = parser.parse_args()

    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        raise SystemExit("❌ PINECONE_API_KEY not found in environment")

    client = PineconeDocumentClient(api_key=api_key, index_name=args.index)

    filters = build_filters_for_deal(args.deal)
    unique = {}
    total_matches = 0

    for f in filters:
        matches = run_filtered_query(client, args.namespace, f, top_k=args.top_k)
        total_matches += len(matches)
        for m in matches:
            key = (m.get("deal_id", ""), m.get("file_name", ""), m.get("salesforce_deal_id", ""))
            if key not in unique:
                unique[key] = m

    results = list(unique.values())
    # Prefer PDFs first
    results.sort(key=lambda r: (r.get("file_type", "").lower() != ".pdf", r.get("file_name", "")))

    print(f"Index: {args.index} | Namespace: {args.namespace}")
    print(f"Deal input: {args.deal}")
    print(f"Total raw matches across filter attempts: {total_matches}")
    print(f"Unique docs returned: {len(results)}")
    print("")

    for r in results[: args.max_results]:
        print(
            f"- file_name={r.get('file_name')} | file_type={r.get('file_type')} | "
            f"deal_id={r.get('deal_id')} | deal_number={r.get('deal_number')} | "
            f"deal_name={r.get('deal_name')} | salesforce_deal_id={r.get('salesforce_deal_id')}"
        )

    if not results:
        print("No matches found via exact filters. Next step: we may need to search a different deal field or fall back to scanning.")


if __name__ == "__main__":
    main()


