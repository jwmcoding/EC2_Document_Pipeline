#!/usr/bin/env python3
"""
Select a small, type-balanced test set of documents from a discovery JSON.

This utility is designed for **table extraction testing**:
- Input is a full discovery file (e.g. `test_raw_salesforce_discovery.json`)
  produced by `discover_documents.py` against a Salesforce raw export.
- It selects a small, diverse subset of documents by file type
  (Excel, Word, PDF) with a bias toward larger files that are
  more likely to contain rich tables.
- Output is a new discovery JSON that preserves the same schema
  (`discovery_metadata` + `discovery_progress` + `documents`),
  but includes only the selected documents.

Typical usage:

    # From project root
    python scripts/select_table_test_documents.py \
        --input test_raw_salesforce_discovery.json \
        --output table_extraction_test_discovery.json \
        --num-pdf 15 \
        --num-docx 5 \
        --num-xlsx 3

You can then feed the output file directly into:

    process_discovered_documents.py --input table_extraction_test_discovery.json ...

Notes on chunking terminology
-----------------------------
In the current codebase, there is some confusing naming:

- `SemanticChunker` in `src/chunking/semantic_chunker.py` is actually a
  **business-aware, structure-aware chunker** (character-limited with
  business section awareness and table detection), **not** embedding-
  similarity-based "semantic" chunking.

- The `ChunkerFactory` exposes two strategies:
    * `business_aware`  -> always uses `SemanticChunker`
    * `semantic`        -> tries a LangChain-based semantic chunker via
                           `LangchainChunkerAdapter`; if that adapter
                           is unavailable, it **falls back** to the same
                           `SemanticChunker` (i.e., business-aware).

For table extraction tests, it's useful to:
- Run once with `--chunking-strategy business_aware`
- Optionally run again with `--chunking-strategy semantic`
  to see whether the LangChain adapter is active or if both modes
  are effectively using the same business-aware logic.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


def load_discovery(path: Path) -> Dict[str, Any]:
    """Load a discovery JSON file into memory.

    This expects the schema produced by `DiscoveryPersistence`:
    {
        "discovery_metadata": {...},
        "discovery_progress": {...},
        "documents": [ ... ]
    }
    """
    if not path.exists():
        raise FileNotFoundError(f"Discovery file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        # allow_nan=True (default) so existing NaN values from pandas are preserved
        data = json.load(f)

    if "documents" not in data or not isinstance(data["documents"], list):
        raise ValueError(f"Invalid discovery file (missing 'documents' list): {path}")

    return data


def _collect_by_type(
    documents: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split documents into (pdf_docs, docx_docs, excel_docs) lists.

    File types are determined from `doc["file_info"]["file_type"]`.
    """
    pdf_docs: List[Dict[str, Any]] = []
    docx_docs: List[Dict[str, Any]] = []
    excel_docs: List[Dict[str, Any]] = []

    for doc in documents:
        file_info = doc.get("file_info", {})
        file_type = (file_info.get("file_type") or "").lower()

        if file_type == ".pdf":
            pdf_docs.append(doc)
        elif file_type in {".docx", ".doc"}:
            docx_docs.append(doc)
        elif file_type in {".xlsx", ".xls", ".xlsm"}:
            excel_docs.append(doc)

    return pdf_docs, docx_docs, excel_docs


def _sort_by_size_desc(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort documents in descending order by file size (bytes).

    This biases selection toward larger documents, which are more
    likely to contain rich tables and multi-page content.
    """
    def size(doc: Dict[str, Any]) -> int:
        file_info = doc.get("file_info", {})
        return int(file_info.get("size") or 0)

    return sorted(docs, key=size, reverse=True)


def select_type_balanced_subset(
    documents: List[Dict[str, Any]],
    num_pdf: int,
    num_docx: int,
    num_xlsx: int,
) -> List[Dict[str, Any]]:
    """Select a type-balanced subset of documents.

    Selection strategy:
    - Partition documents into PDF, DOCX, and Excel buckets.
    - Within each bucket, sort by size descending.
    - Take up to the requested count from each bucket.
    - Preserve original document objects (no mutation).

    Returns:
        A list of selected document dictionaries.
    """
    pdf_docs, docx_docs, excel_docs = _collect_by_type(documents)

    pdf_sorted = _sort_by_size_desc(pdf_docs)
    docx_sorted = _sort_by_size_desc(docx_docs)
    excel_sorted = _sort_by_size_desc(excel_docs)

    selected: List[Dict[str, Any]] = []
    seen_ids = set()

    def take(docs: List[Dict[str, Any]], limit: int) -> None:
        for doc in docs:
            if len(selected) >= len(documents):  # safety, shouldn't happen
                break
            # Use file path as a stable identity key
            file_info = doc.get("file_info", {})
            key = (file_info.get("path") or file_info.get("name") or "").lower()
            if not key or key in seen_ids:
                continue
            selected.append(doc)
            seen_ids.add(key)
            if len([d for d in selected if d in docs]) >= limit:
                break

    take(pdf_sorted, num_pdf)
    take(docx_sorted, num_docx)
    take(excel_sorted, num_xlsx)

    return selected


def build_subset_discovery(
    base_data: Dict[str, Any],
    selected_docs: List[Dict[str, Any]],
    override_source_type: str | None = None,
    override_source_path: str | None = None,
) -> Dict[str, Any]:
    """Build a new discovery JSON structure with only the selected documents.

    - Copies `discovery_metadata` and `discovery_progress` from the base file.
    - Updates `total_documents` and `documents_discovered` to match the subset.
    - Leaves batch-processing metadata intact (but with zero jobs by default).
    """
    new_data = dict(base_data)  # shallow copy of top-level dict

    total = len(selected_docs)
    discovery_meta = new_data.setdefault("discovery_metadata", {})
    discovery_progress = new_data.setdefault("discovery_progress", {})

    # Update counts
    discovery_meta["total_documents"] = total
    discovery_progress["documents_discovered"] = total

    # Optionally override source type/path to match how the processing
    # pipeline will access content (e.g., treat a Salesforce export as
    # a local filesystem tree rooted at EXPORT_DIR).
    if override_source_type is not None:
        discovery_meta["source_type"] = override_source_type
    if override_source_path is not None:
        discovery_meta["source_path"] = override_source_path

    # This is a logically "complete" subset; mark discovery as completed now.
    discovery_meta["discovery_completed"] = (
        discovery_meta.get("discovery_completed")
        or discovery_meta.get("discovery_started")
    )

    # Replace documents with subset
    new_data["documents"] = selected_docs

    return new_data


def save_discovery(path: Path, data: Dict[str, Any]) -> None:
    """Write the discovery JSON to disk with pretty formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Select a small, type-balanced test set from a discovery JSON "
                    "for table extraction testing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=str,
        default="test_raw_salesforce_discovery.json",
        help="Input discovery JSON file (full discovery).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="table_extraction_test_discovery.json",
        help="Output discovery JSON file (subset for table tests).",
    )
    parser.add_argument(
        "--num-pdf",
        type=int,
        default=15,
        help="Target number of PDF documents to select.",
    )
    parser.add_argument(
        "--num-docx",
        type=int,
        default=5,
        help="Target number of DOC/DOCX documents to select.",
    )
    parser.add_argument(
        "--num-xlsx",
        type=int,
        default=3,
        help="Target number of Excel documents to select.",
    )
    parser.add_argument(
        "--override-source-type",
        type=str,
        choices=["local", "dropbox", "salesforce", "salesforce_raw"],
        default=None,
        help="Optional override for discovery_metadata.source_type in the subset.",
    )
    parser.add_argument(
        "--override-source-path",
        type=str,
        default=None,
        help="Optional override for discovery_metadata.source_path in the subset "
             "(e.g., the root of a Salesforce export when treating it as local).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for selecting table-extraction test documents."""
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    print(f"📂 Loading discovery file: {input_path}")
    data = load_discovery(input_path)
    documents = data.get("documents", [])
    print(f"   Total documents available: {len(documents):,}")

    selected_docs = select_type_balanced_subset(
        documents=documents,
        num_pdf=args.num_pdf,
        num_docx=args.num_docx,
        num_xlsx=args.num_xlsx,
    )

    print(f"✅ Selected {len(selected_docs):,} documents "
          f"(PDF≤{args.num_pdf}, DOCX≤{args.num_docx}, XLSX≤{args.num_xlsx})")

    new_data = build_subset_discovery(
        data,
        selected_docs,
        override_source_type=args.override_source_type,
        override_source_path=args.override_source_path,
    )

    print(f"💾 Writing subset discovery to: {output_path}")
    save_discovery(output_path, new_data)
    print("🎉 Done. Use this file with process_discovered_documents.py for table extraction tests.")


if __name__ == "__main__":
    main()


