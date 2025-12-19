#!/usr/bin/env python3
"""
Test harness: sample 25 deals (from a source Pinecone index/namespace) that have PDFs,
then retrieve + reconstruct the corresponding PDFs from a target index/namespace.

Primary use case:
- Source (existing): business-documents / SF-Files-2020-8-15-25
- Target (new Mistral extraction): npi-deal-data / sf-export-aug15-2025

Outputs:
- manifest.json: sampled deals + per-PDF reconstruction status + diagnostics
- summary.md: human-readable summary
- reconstructed/*.txt: reconstructed text per (deal_id, file_name)
"""

import argparse
import json
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


# Add src to path (script is in scripts/compare_pinecone_targets/, need to go up 2 levels)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(script_dir, "..", "..")
sys.path.insert(0, project_root)
sys.path.insert(0, script_dir)

from dotenv import load_dotenv

try:
    load_dotenv()
except PermissionError:
    # In some sandboxed environments (or when .env is protected), reading .env can fail.
    # That's OK as long as required env vars are already set in the shell.
    pass

from src.connectors.pinecone_client import PineconeDocumentClient, _sanitize_str

# Reuse comparison utilities (chunk fetch + reconstruction + diagnostics)
from compare_pinecone_targets import ChunkFetcher, TextDiagnostics, TextReconstructor


def sanitize_str(value: Any, default: str = "") -> str:
    return _sanitize_str(value, default)


def _is_pdf(metadata: Dict[str, Any]) -> bool:
    file_type = sanitize_str(metadata.get("file_type", "")).lower()
    file_name = sanitize_str(metadata.get("file_name", "")).lower()
    if file_type == ".pdf":
        return True
    if file_name.endswith(".pdf"):
        return True
    return False


def _safe_filename(s: str, max_len: int = 160) -> str:
    s = sanitize_str(s)
    s = s.replace(os.sep, "_")
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("._-")
    if not s:
        return "unknown"
    return s[:max_len]


def scan_source_for_pdf_deals(
    source_client: PineconeDocumentClient,
    source_namespace: str,
    pool_size: int,
    max_ids: int,
    batch_size: int,
) -> Tuple[Dict[str, Set[str]], Dict[str, Dict[str, Any]]]:
    """
    Enumerate source namespace to find deals that have PDFs.

    Returns:
      - deal_to_pdf_files: {deal_key: {file_name, ...}}
      - deal_sample_metadata: {deal_key: {vendor_name, client_name, ...}} (best-effort)

    deal_key is chosen to support cross-index comparisons:
      - Prefer salesforce_deal_id when present
      - Else if deal_id looks like a Salesforce ID (e.g., starts with "a0W"), use deal_id
      - Else fallback to deal_id
    """
    deal_to_pdf_files: Dict[str, Set[str]] = {}
    deal_sample_metadata: Dict[str, Dict[str, Any]] = {}

    index = source_client.index
    processed = 0

    for vector_ids_batch in index.list(namespace=source_namespace, limit=batch_size):
        if not vector_ids_batch:
            break

        if isinstance(vector_ids_batch, str):
            batch_ids = [vector_ids_batch]
        else:
            batch_ids = list(vector_ids_batch)

        fetch_results = index.fetch(ids=batch_ids, namespace=source_namespace)
        vectors_map = getattr(fetch_results, "vectors", None) or fetch_results.get("vectors", {})

        for vid in batch_ids:
            vdata = vectors_map.get(vid, {})
            metadata = getattr(vdata, "metadata", None)
            if metadata is None and isinstance(vdata, dict):
                metadata = vdata.get("metadata", {})
            metadata = metadata or {}

            if not _is_pdf(metadata):
                continue

            deal_id = sanitize_str(metadata.get("deal_id", ""))
            salesforce_deal_id = sanitize_str(metadata.get("salesforce_deal_id", ""))
            file_name = sanitize_str(metadata.get("file_name", ""))
            if not deal_id or not file_name:
                continue

            # Cross-index join key
            if salesforce_deal_id:
                deal_key = salesforce_deal_id
            elif deal_id.startswith("a0W"):
                deal_key = deal_id
            else:
                deal_key = deal_id

            deal_to_pdf_files.setdefault(deal_key, set()).add(file_name)

            if deal_key not in deal_sample_metadata:
                deal_sample_metadata[deal_key] = {
                    "deal_key": deal_key,
                    "source_deal_id": deal_id,
                    "source_salesforce_deal_id": salesforce_deal_id,
                    "vendor_name": sanitize_str(metadata.get("vendor_name", "")),
                    "client_name": sanitize_str(metadata.get("client_name", "")),
                    "deal_name": sanitize_str(metadata.get("deal_name", "")),
                    "deal_subject": sanitize_str(metadata.get("deal_subject", "")),
                }

        processed += len(batch_ids)
        if processed % 5000 == 0:
            print(
                f"  Scanned {processed:,} ids | pdf-deals={len(deal_to_pdf_files):,} | pool_target={pool_size:,}"
            )

        if processed >= max_ids:
            break

        if len(deal_to_pdf_files) >= pool_size:
            break

    return deal_to_pdf_files, deal_sample_metadata


def _truncate_for_llm(text: str, max_chars: int) -> str:
    """
    Truncate long documents for LLM judging while preserving both beginning and end.
    """
    if not text:
        return ""
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    head = int(max_chars * 0.75)
    tail = max_chars - head
    return (
        text[:head]
        + "\n\n[...TRUNCATED...]\n\n"
        + text[-tail:]
    )


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    Best-effort JSON object extraction from a model response.
    """
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass

    # Try to find a JSON object substring
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            return None
    return None


def llm_judge_pair(
    openai_client: Any,
    model: str,
    source_text: str,
    target_text: str,
    deal_id: str,
    file_name: str,
    max_chars_per_doc: int,
    timeout_s: int,
    max_output_tokens: int,
) -> Dict[str, Any]:
    """
    LLM-as-judge: compare two reconstructions for LLM-usefulness.
    Returns a dict with at least: winner, comparison_summary, key_differences, red_flags.
    """
    from openai import OpenAI  # import locally to avoid import cost when not used

    s_txt = _truncate_for_llm(source_text, max_chars_per_doc)
    t_txt = _truncate_for_llm(target_text, max_chars_per_doc)

    prompt = f"""
You are judging two text reconstructions of the SAME PDF, intended to be used as LLM context.

Goal: Decide which reconstruction is better for an LLM to answer questions and extract facts reliably.

You MUST return ONLY valid JSON with this schema:
{{
  "winner": "source" | "target" | "tie",
  "comparison_summary": string,
  "key_differences": [string, ...],
  "red_flags": [string, ...],
  "notes": string
}}

Important:
- Do NOT use numeric scores.
- Prefer the version with clearer structure, fewer OCR artifacts, fewer missing sections, and better table/list readability.
- If both are bad or both are excellent, use "tie" and explain.

deal_id: {deal_id}
file_name: {file_name}

--- SOURCE RECONSTRUCTION START ---
{s_txt}
--- SOURCE RECONSTRUCTION END ---

--- TARGET RECONSTRUCTION START ---
{t_txt}
--- TARGET RECONSTRUCTION END ---
""".strip()

    content = ""
    try:
        if model.startswith("gpt-5"):
            resp = openai_client.responses.create(
                model=model,
                input=prompt,
                max_output_tokens=max_output_tokens,
                timeout=timeout_s,
            )
            content = getattr(resp, "output_text", "") or ""
            if not content:
                out = getattr(resp, "output", None)
                if isinstance(out, list) and out:
                    parts = []
                    for item in out:
                        segment_list = getattr(item, "content", None)
                        if isinstance(segment_list, list):
                            for seg in segment_list:
                                if isinstance(seg, dict) and seg.get("type") in ("output_text", "text"):
                                    parts.append(str(seg.get("text", "")))
                    content = "".join(parts)
        else:
            resp = openai_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                max_completion_tokens=max_output_tokens,
                timeout=timeout_s,
            )
            content = getattr(resp.choices[0].message, "content", "") or ""
    except Exception as e:
        return {
            "winner": "tie",
            "comparison_summary": f"LLM judge call failed: {e}",
            "key_differences": [],
            "red_flags": ["llm_call_failed"],
            "notes": "",
            "raw_response": content,
        }

    parsed = _extract_json_object(content)
    if not parsed:
        return {
            "winner": "tie",
            "comparison_summary": "LLM returned non-JSON output; unable to parse.",
            "key_differences": [],
            "red_flags": ["non_json_output"],
            "notes": "",
            "raw_response": content,
        }

    # Minimal normalization
    winner = str(parsed.get("winner", "tie")).strip().lower()
    if winner not in ("source", "target", "tie"):
        winner = "tie"
    parsed["winner"] = winner
    return parsed


def compute_pricing_signals(text: str) -> Dict[str, Any]:
    """
    Deterministic heuristics that correlate with answering price/trend/comparison questions.
    These do NOT measure correctness, but do measure whether the extraction likely preserved:
    - currency amounts
    - percentages
    - year references / timelines
    - table-like blocks / pricing terms keywords
    """
    if not text:
        return {
            "total_chars": 0,
            "lines": 0,
            "currency_amount_count": 0,
            "currency_line_count": 0,
            "percent_count": 0,
            "year_count": 0,
            "number_token_count": 0,
            "trend_keyword_hits": 0,
        }

    lines = text.splitlines()
    total_chars = len(text)

    # Currency patterns: $1,234.56, $ 1234, USD 1234
    currency_amount_count = len(re.findall(r"(?:\\$\\s*\\d[\\d,]*(?:\\.\\d{1,4})?|\\bUSD\\s*\\d[\\d,]*(?:\\.\\d{1,4})?)", text))
    currency_line_count = sum(1 for ln in lines if "$" in ln or "USD" in ln)

    percent_count = len(re.findall(r"\\b\\d{1,3}(?:\\.\\d+)?\\s*%\\b", text))
    year_count = len(re.findall(r"\\b(?:19|20)\\d{2}\\b", text))

    # Numeric tokens (helps detect presence of price tables even if $ missing)
    number_token_count = len(re.findall(r"\\b\\d[\\d,]*(?:\\.\\d+)?\\b", text))

    trend_keywords = [
        "increase", "decrease", "growth", "baseline", "forecast", "trend", "yoy",
        "cpi", "inflation", "renewal", "uplift", "escalat", "annual", "year", "month",
        "pricing", "price", "rate", "fee", "credit", "discount", "subtotal", "total",
        "unit", "per user", "per seat", "per month", "per year",
    ]
    lower = text.lower()
    trend_keyword_hits = sum(1 for kw in trend_keywords if kw in lower)

    return {
        "total_chars": total_chars,
        "lines": len(lines),
        "currency_amount_count": currency_amount_count,
        "currency_line_count": currency_line_count,
        "percent_count": percent_count,
        "year_count": year_count,
        "number_token_count": number_token_count,
        "trend_keyword_hits": trend_keyword_hits,
    }


def llm_judge_pricing_pair(
    openai_client: Any,
    model: str,
    source_text: str,
    target_text: str,
    deal_key: str,
    file_name: str,
    max_chars_per_doc: int,
    timeout_s: int,
    max_output_tokens: int,
) -> Dict[str, Any]:
    """
    LLM-as-judge (pricing-focused): decide which reconstruction better supports
    price lookups, comparisons, and trends over time.
    """
    s_txt = _truncate_for_llm(source_text, max_chars_per_doc)
    t_txt = _truncate_for_llm(target_text, max_chars_per_doc)

    prompt = f"""
You are judging two text reconstructions of the SAME document, specifically for answering:
- exact price questions (unit price, totals, discounts, credits)
- price comparisons (Option A vs B, SKU A vs SKU B)
- price trends over time (year-by-year tables, baseline vs forecast, renewal increases)

Choose the version that is more reliable for numeric extraction:
- tables should be readable and aligned enough to avoid mis-attributing numbers
- currency amounts, units, and time periods should be explicit
- fewer OCR artifacts and fewer missing sections wins

Return ONLY valid JSON with this schema:
{{
  "winner": "source" | "target" | "tie",
  "pricing_summary": string,
  "price_table_quality": string,
  "trend_support": string,
  "key_differences": [string, ...],
  "numeric_risks": [string, ...],
  "notes": string
}}

deal_key: {deal_key}
file_name: {file_name}

--- SOURCE RECONSTRUCTION START ---
{s_txt}
--- SOURCE RECONSTRUCTION END ---

--- TARGET RECONSTRUCTION START ---
{t_txt}
--- TARGET RECONSTRUCTION END ---
""".strip()

    content = ""
    try:
        if model.startswith("gpt-5"):
            resp = openai_client.responses.create(
                model=model,
                input=prompt,
                max_output_tokens=max_output_tokens,
                timeout=timeout_s,
            )
            content = getattr(resp, "output_text", "") or ""
            if not content:
                out = getattr(resp, "output", None)
                if isinstance(out, list) and out:
                    parts = []
                    for item in out:
                        segment_list = getattr(item, "content", None)
                        if isinstance(segment_list, list):
                            for seg in segment_list:
                                if isinstance(seg, dict) and seg.get("type") in ("output_text", "text"):
                                    parts.append(str(seg.get("text", "")))
                    content = "".join(parts)
        else:
            resp = openai_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                max_completion_tokens=max_output_tokens,
                timeout=timeout_s,
            )
            content = getattr(resp.choices[0].message, "content", "") or ""
    except Exception as e:
        return {
            "winner": "tie",
            "pricing_summary": f"Pricing judge call failed: {e}",
            "price_table_quality": "",
            "trend_support": "",
            "key_differences": [],
            "numeric_risks": ["llm_call_failed"],
            "notes": "",
            "raw_response": content,
        }

    parsed = _extract_json_object(content)
    if not parsed:
        return {
            "winner": "tie",
            "pricing_summary": "LLM returned non-JSON output; unable to parse.",
            "price_table_quality": "",
            "trend_support": "",
            "key_differences": [],
            "numeric_risks": ["non_json_output"],
            "notes": "",
            "raw_response": content,
        }

    winner = str(parsed.get("winner", "tie")).strip().lower()
    if winner not in ("source", "target", "tie"):
        winner = "tie"
    parsed["winner"] = winner
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Harness: sample 25 deals with PDFs from a source index/namespace, reconstruct PDFs from a target namespace."
    )
    parser.add_argument("--source-index", default="business-documents", help="Source Pinecone index (where we detect deals with PDFs)")
    parser.add_argument("--source-namespace", default="SF-Files-2020-8-15-25", help="Source namespace to scan")
    parser.add_argument("--target-index", default="npi-deal-data", help="Target Pinecone index (where we reconstruct PDFs)")
    parser.add_argument("--target-namespace", default="sf-export-aug15-2025", help="Target namespace to reconstruct from")

    parser.add_argument("--sample-size", type=int, default=25, help="Number of deals to sample (default: 25)")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed for reproducible sampling")
    parser.add_argument(
        "--max-pdfs-per-deal",
        type=int,
        default=2,
        help="Max PDFs to reconstruct per sampled deal (default: 2)",
    )

    parser.add_argument(
        "--pool-size",
        type=int,
        default=250,
        help="How many unique pdf-deals to collect before sampling (default: 250)",
    )
    parser.add_argument(
        "--scan-max-ids",
        type=int,
        default=50000,
        help="Max vector IDs to scan in the source namespace (default: 50000)",
    )
    parser.add_argument(
        "--scan-batch-size",
        type=int,
        default=100,
        help="Batch size for list+fetch scanning (default: 100)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Base output directory (default: output/)",
    )

    # Single-document mode (bypass sampling + scanning)
    parser.add_argument(
        "--deal-key",
        type=str,
        default=None,
        help="If provided, bypass sampling and run on this deal key (prefer Salesforce id like a0W...).",
    )
    parser.add_argument(
        "--file-name-exact",
        type=str,
        default=None,
        help="If provided with --deal-key, run only on this exact file_name.",
    )
    parser.add_argument(
        "--query-top-k",
        type=int,
        default=5000,
        help="Query top_k for fast filtered chunk retrieval (default: 5000).",
    )

    # LLM-as-judge mode
    parser.add_argument(
        "--llm-judge",
        action="store_true",
        help="If set, run GPT judge to compare source vs target reconstructions (writes verdicts to manifest + summary).",
    )
    parser.add_argument(
        "--llm-model",
        default=os.getenv("OPENAI_JUDGE_MODEL", "gpt-5.2"),
        help="OpenAI model for judge (default: OPENAI_JUDGE_MODEL env var or gpt-5.2)",
    )
    parser.add_argument(
        "--llm-timeout",
        type=int,
        default=60,
        help="LLM request timeout seconds (default: 60)",
    )
    parser.add_argument(
        "--llm-max-output-tokens",
        type=int,
        default=1500,
        help="Max output tokens for judge response (default: 1500)",
    )
    parser.add_argument(
        "--llm-max-chars-per-doc",
        type=int,
        default=20000,
        help="Max characters per doc passed to judge (default: 20000)",
    )

    # Pricing eval / judge
    parser.add_argument(
        "--pricing-eval",
        action="store_true",
        help="If set, compute pricing-focused signal metrics and include them in manifest + summary.",
    )
    parser.add_argument(
        "--pricing-judge",
        action="store_true",
        help="If set, run a pricing-focused GPT judge (requires --llm-judge and OPENAI_API_KEY).",
    )

    args = parser.parse_args()

    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    if not pinecone_api_key:
        raise SystemExit("❌ PINECONE_API_KEY not found in environment")

    openai_client = None
    if args.llm_judge or args.pricing_judge:
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise SystemExit("❌ OPENAI_API_KEY not found in environment (required for --llm-judge)")
        from openai import OpenAI

        openai_client = OpenAI(api_key=openai_api_key)

    # Clients
    source_client = PineconeDocumentClient(api_key=pinecone_api_key, index_name=args.source_index)
    target_client = PineconeDocumentClient(api_key=pinecone_api_key, index_name=args.target_index)

    # Either scan+sample, or run a single document directly.
    if args.deal_key:
        if not args.file_name_exact:
            raise SystemExit("❌ --deal-key requires --file-name-exact")
        deal_key = sanitize_str(args.deal_key)
        file_name_exact = sanitize_str(args.file_name_exact)
        deal_to_pdfs = {deal_key: {file_name_exact}}
        deal_meta = {
            deal_key: {
                "deal_key": deal_key,
                "source_deal_id": "",  # unknown; we will use schema-adaptive matching
                "source_salesforce_deal_id": deal_key if deal_key.startswith("a0W") else "",
                "vendor_name": "",
                "client_name": "",
                "deal_name": "",
                "deal_subject": "",
            }
        }
        print(f"🎯 Single-document mode: deal_key={deal_key} file_name={file_name_exact}")
    else:
        print(
            f"🔎 Scanning source for pdf deals: {args.source_index}/{args.source_namespace} "
            f"(max_ids={args.scan_max_ids:,}, batch={args.scan_batch_size})"
        )
        deal_to_pdfs, deal_meta = scan_source_for_pdf_deals(
            source_client=source_client,
            source_namespace=args.source_namespace,
            pool_size=args.pool_size,
            max_ids=args.scan_max_ids,
            batch_size=args.scan_batch_size,
        )

    if not deal_to_pdfs:
        raise SystemExit("❌ No deals with PDFs found in source scan (try increasing --scan-max-ids)")

    all_deals = sorted(deal_to_pdfs.keys())
    rng = random.Random(args.seed)
    if len(all_deals) <= args.sample_size:
        sampled_deals = all_deals
    else:
        sampled_deals = rng.sample(all_deals, args.sample_size)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.deal_key:
        out_dir = Path(args.output_dir) / f"harness_single_{_safe_filename(args.deal_key)}_{timestamp}"
    else:
        out_dir = Path(args.output_dir) / f"harness_sample_{args.sample_size}_deals_{timestamp}"
    recon_dir = out_dir / "reconstructed"
    recon_dir.mkdir(parents=True, exist_ok=True)

    source_fetcher = ChunkFetcher(source_client, args.source_namespace)
    target_fetcher = ChunkFetcher(target_client, args.target_namespace)

    manifest: Dict[str, Any] = {
        "run": {
            "timestamp": timestamp,
            "source": {"index": args.source_index, "namespace": args.source_namespace},
            "target": {"index": args.target_index, "namespace": args.target_namespace},
            "sample_size": args.sample_size,
            "seed": args.seed,
            "max_pdfs_per_deal": args.max_pdfs_per_deal,
            "scan_max_ids": args.scan_max_ids,
            "scan_batch_size": args.scan_batch_size,
            "pool_size": args.pool_size,
            "llm_judge": bool(args.llm_judge),
            "llm_model": args.llm_model if args.llm_judge else None,
        },
        "sampled_deals": [],
        "stats": {
            "deals_scanned_with_pdfs": len(all_deals),
            "sampled_deals": len(sampled_deals),
            "pdfs_attempted": 0,
            "source_pdfs_reconstructed": 0,
            "target_pdfs_reconstructed": 0,
            "source_pdfs_missing": 0,
            "target_pdfs_missing": 0,
            "source_pdfs_hit_chunk_limit_200": 0,
            "target_pdfs_hit_chunk_limit_200": 0,
            "llm_judgments_completed": 0,
            "llm_judgments_failed": 0,
            "pricing_judgments_completed": 0,
            "pricing_judgments_failed": 0,
        },
    }

    print(f"🎲 Sampled {len(sampled_deals)} deals. Reconstructing PDFs from target...")

    for deal_key in sampled_deals:
        pdf_files = sorted(deal_to_pdfs.get(deal_key, []))
        if args.max_pdfs_per_deal > 0:
            pdf_files = pdf_files[: args.max_pdfs_per_deal]

        meta = deal_meta.get(deal_key, {}) or {}
        source_deal_id = sanitize_str(meta.get("source_deal_id", ""))
        source_salesforce_deal_id = sanitize_str(meta.get("source_salesforce_deal_id", ""))

        # For cross-index, deal_key is intended to be salesforce_deal_id when possible
        effective_salesforce_deal_id = deal_key if deal_key.startswith("a0W") else source_salesforce_deal_id

        deal_entry: Dict[str, Any] = {
            "deal_key": deal_key,
            "source_deal_id": source_deal_id,
            "source_salesforce_deal_id": source_salesforce_deal_id,
            "effective_salesforce_deal_id": effective_salesforce_deal_id,
            "source_pdf_files": pdf_files,
            "source_deal_metadata": meta,
            "target_reconstructions": [],
        }

        for file_name in pdf_files:
            manifest["stats"]["pdfs_attempted"] += 1

            source_chunks = source_fetcher.fetch_all_chunks(
                document_key=f"{source_deal_id or deal_key}::{file_name}",
                deal_id=(source_deal_id or (deal_key if not effective_salesforce_deal_id else None)),
                salesforce_deal_id=(source_salesforce_deal_id if source_salesforce_deal_id else None),
                file_name=file_name,
                query_top_k=args.query_top_k,
            )
            target_chunks = target_fetcher.fetch_all_chunks(
                document_key=f"{deal_key}::{file_name}",
                # IMPORTANT: in npi-deal-data, deal_id may be numeric while salesforce_deal_id carries the a0W... id
                salesforce_deal_id=effective_salesforce_deal_id if effective_salesforce_deal_id else None,
                deal_id=None if effective_salesforce_deal_id else deal_key,
                file_name=file_name,
                query_top_k=args.query_top_k,
            )

            source_text = TextReconstructor.reconstruct_text(source_chunks)
            target_text = TextReconstructor.reconstruct_text(target_chunks)
            source_diag = TextDiagnostics.compute_diagnostics(source_text, source_chunks)
            target_diag = TextDiagnostics.compute_diagnostics(target_text, target_chunks)
            source_pricing = compute_pricing_signals(source_text) if args.pricing_eval else None
            target_pricing = compute_pricing_signals(target_text) if args.pricing_eval else None

            source_hit_limit = len(source_chunks) >= 200
            target_hit_limit = len(target_chunks) >= 200
            if source_hit_limit:
                manifest["stats"]["source_pdfs_hit_chunk_limit_200"] += 1
            if target_hit_limit:
                manifest["stats"]["target_pdfs_hit_chunk_limit_200"] += 1

            # Save texts (raw)
            deal_folder = recon_dir / _safe_filename(deal_key)
            deal_folder.mkdir(parents=True, exist_ok=True)

            source_text_path = None
            target_text_path = None

            if source_text.strip():
                source_text_path = deal_folder / f"{_safe_filename(file_name)}.source.txt"
                source_text_path.write_text(source_text, encoding="utf-8")
                manifest["stats"]["source_pdfs_reconstructed"] += 1
                source_status = "reconstructed"
            else:
                manifest["stats"]["source_pdfs_missing"] += 1
                source_status = "missing_or_empty"

            if target_text.strip():
                target_text_path = deal_folder / f"{_safe_filename(file_name)}.target.txt"
                target_text_path.write_text(target_text, encoding="utf-8")
                manifest["stats"]["target_pdfs_reconstructed"] += 1
                target_status = "reconstructed"
            else:
                manifest["stats"]["target_pdfs_missing"] += 1
                target_status = "missing_or_empty"

            llm_judgment = None
            if args.llm_judge and openai_client is not None:
                judge = llm_judge_pair(
                    openai_client=openai_client,
                    model=args.llm_model,
                    source_text=source_text,
                    target_text=target_text,
                    deal_id=deal_key,
                    file_name=file_name,
                    max_chars_per_doc=args.llm_max_chars_per_doc,
                    timeout_s=args.llm_timeout,
                    max_output_tokens=args.llm_max_output_tokens,
                )
                llm_judgment = judge
                if judge.get("red_flags") and "llm_call_failed" in judge.get("red_flags", []):
                    manifest["stats"]["llm_judgments_failed"] += 1
                else:
                    manifest["stats"]["llm_judgments_completed"] += 1

            pricing_judgment = None
            if args.pricing_judge:
                if not args.llm_judge:
                    raise SystemExit("❌ --pricing-judge requires --llm-judge (OpenAI client init)")
                if openai_client is None:
                    raise SystemExit("❌ OpenAI client not initialized (required for --pricing-judge)")
                pj = llm_judge_pricing_pair(
                    openai_client=openai_client,
                    model=args.llm_model,
                    source_text=source_text,
                    target_text=target_text,
                    deal_key=deal_key,
                    file_name=file_name,
                    max_chars_per_doc=args.llm_max_chars_per_doc,
                    timeout_s=args.llm_timeout,
                    max_output_tokens=args.llm_max_output_tokens,
                )
                pricing_judgment = pj
                if pj.get("numeric_risks") and "llm_call_failed" in pj.get("numeric_risks", []):
                    manifest["stats"]["pricing_judgments_failed"] += 1
                else:
                    manifest["stats"]["pricing_judgments_completed"] += 1

            deal_entry["target_reconstructions"].append(
                {
                    "file_name": file_name,
                    "source": {
                        "status": source_status,
                        "chunks_retrieved": len(source_chunks),
                        "hit_chunk_limit_200": source_hit_limit,
                        "diagnostics": source_diag,
                        "pricing_signals": source_pricing,
                        "output_text_path": str(source_text_path) if source_text_path else None,
                    },
                    "target": {
                        "status": target_status,
                        "chunks_retrieved": len(target_chunks),
                        "hit_chunk_limit_200": target_hit_limit,
                        "diagnostics": target_diag,
                        "pricing_signals": target_pricing,
                        "output_text_path": str(target_text_path) if target_text_path else None,
                    },
                    "llm_judgment": llm_judgment,
                    "pricing_judgment": pricing_judgment,
                }
            )

        manifest["sampled_deals"].append(deal_entry)

    # Write outputs
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Summary markdown
    summary_lines: List[str] = []
    summary_lines.append(f"# PDF Reconstruction Harness (sample={manifest['run']['sample_size']})")
    summary_lines.append("")
    summary_lines.append("## Targets")
    summary_lines.append(f"- **Source**: `{args.source_index}` / `{args.source_namespace}`")
    summary_lines.append(f"- **Target**: `{args.target_index}` / `{args.target_namespace}`")
    summary_lines.append("")
    summary_lines.append("## Stats")
    for k, v in manifest["stats"].items():
        summary_lines.append(f"- **{k}**: {v}")
    summary_lines.append("")

    if args.pricing_eval:
        # Aggregate pricing signals deltas (target - source)
        deltas = {
            "currency_amount_count": [],
            "percent_count": [],
            "year_count": [],
            "number_token_count": [],
            "trend_keyword_hits": [],
        }
        for deal in manifest["sampled_deals"]:
            for rec in deal.get("target_reconstructions", []):
                sps = (rec.get("source", {}) or {}).get("pricing_signals") or {}
                tps = (rec.get("target", {}) or {}).get("pricing_signals") or {}
                if not sps or not tps:
                    continue
                for key in deltas:
                    try:
                        deltas[key].append(float(tps.get(key, 0)) - float(sps.get(key, 0)))
                    except Exception:
                        pass

        summary_lines.append("## Pricing signal deltas (target - source)")
        for key, vals in deltas.items():
            if not vals:
                continue
            avg = sum(vals) / max(len(vals), 1)
            summary_lines.append(f"- **avg {key} delta**: {avg:.2f}")
        summary_lines.append("")

    if args.pricing_judge:
        # Count pricing judge winners
        counts = {"source": 0, "target": 0, "tie": 0}
        for deal in manifest["sampled_deals"]:
            for rec in deal.get("target_reconstructions", []):
                w = ((rec.get("pricing_judgment") or {}).get("winner") or "").lower()
                if w in counts:
                    counts[w] += 1
        summary_lines.append("## Pricing judge winners")
        summary_lines.append(f"- **target**: {counts['target']}")
        summary_lines.append(f"- **source**: {counts['source']}")
        summary_lines.append(f"- **tie**: {counts['tie']}")
        summary_lines.append("")
    summary_lines.append("## Sampled deals")
    for deal in manifest["sampled_deals"]:
        summary_lines.append(f"### Deal `{deal.get('deal_key')}`")
        md = deal.get("source_deal_metadata", {})
        if md.get("client_name") or md.get("vendor_name"):
            summary_lines.append(f"- **client_name**: {md.get('client_name','')}")
            summary_lines.append(f"- **vendor_name**: {md.get('vendor_name','')}")
        for recon in deal["target_reconstructions"]:
            s = recon.get("source", {})
            t = recon.get("target", {})
            judge = recon.get("llm_judgment") or {}
            judge_winner = judge.get("winner")
            judge_note = f" | **judge**: {judge_winner}" if args.llm_judge and judge_winner else ""
            summary_lines.append(
                f"- `{recon['file_name']}`: "
                f"source=**{s.get('status')}**(chunks={s.get('chunks_retrieved')}, hit_200={s.get('hit_chunk_limit_200')}) "
                f"target=**{t.get('status')}**(chunks={t.get('chunks_retrieved')}, hit_200={t.get('hit_chunk_limit_200')})"
                f"{judge_note}"
            )
        summary_lines.append("")

    (out_dir / "summary.md").write_text("\n".join(summary_lines).strip() + "\n", encoding="utf-8")

    print(f"✅ Done. Outputs written to: {out_dir}")


if __name__ == "__main__":
    main()


