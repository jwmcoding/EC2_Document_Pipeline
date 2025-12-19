#!/usr/bin/env python3
"""
Local redaction harness: generate review artifacts for the PII/NER redaction stage.

Goal:
- For a sampled set of PDFs, emit artifacts that make it easy to compare:
  - the original PDF bytes used for extraction
  - the extracted markdown before/after redaction
  - redaction stats + strict-mode validation results

Notes:
- This script runs locally (on-disk PDFs, local output folder), but PERSON/ORG
  span detection uses OpenAI when enabled (e.g., GPT-5 mini). It is not an
  offline/local model unless you wire one in.

Output structure:
output/redaction_harness_<timestamp>/
  manifest.json
  REDACTION_REVIEW_INDEX.md
  docs/<doc_id>/
    original.pdf
    original.md          (optional)
    redacted.md          (required)
    redaction.json
"""

from __future__ import annotations

import argparse
import logging
import json
import os
import random
import re
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    # Safe to ignore if dotenv isn't available or .env can't be read.
    pass

# Ensure repo imports work when running as a script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.connectors.local_filesystem_client import LocalFilesystemClient
from src.models.document_models import DocumentMetadata
from src.parsers.document_converter import DocumentConverter
from src.parsers.pdfplumber_parser import PDFPlumberParser

try:
    from src.parsers.docling_parser import DoclingParser, is_docling_available
except Exception:
    DoclingParser = None  # type: ignore[assignment]

    def is_docling_available() -> bool:  # type: ignore[no-redef]
        return False

try:
    from src.parsers.mistral_parser import MistralParser, is_mistral_available
except Exception:
    MistralParser = None  # type: ignore[assignment]

    def is_mistral_available() -> bool:  # type: ignore[no-redef]
        return False

from src.redaction.client_registry import ClientRegistry
from src.redaction.redaction_context import RedactionContext
from src.redaction.redaction_service import RedactionService

__all__ = ["run", "main"]


def _safe_filename(s: str, max_len: int = 180) -> str:
    s = (s or "").strip()
    s = s.replace(os.sep, "_")
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("._-")
    return (s or "unknown")[:max_len]


def _is_pdf(doc: Dict[str, Any]) -> bool:
    file_info = doc.get("file_info", {}) or {}
    name = str(file_info.get("name", "") or "")
    file_type = str(file_info.get("file_type", "") or "")
    if file_type.lower() == ".pdf":
        return True
    return name.lower().endswith(".pdf")


def _get_file_extension(doc: Dict[str, Any]) -> str:
    """Best-effort extension for a discovery doc (uses file_type then filename suffix)."""
    file_info = doc.get("file_info", {}) or {}
    name = str(file_info.get("name", "") or "")
    file_type = str(file_info.get("file_type", "") or "")
    ext = (file_type or Path(name).suffix or "").strip()
    if ext and not ext.startswith("."):
        ext = "." + ext
    return ext.lower()


def _matches_file_types(doc: Dict[str, Any], allowed_types: List[str]) -> bool:
    """Return True if doc matches any extension in allowed_types (case-insensitive)."""
    if not allowed_types:
        return False
    allowed = []
    for t in allowed_types:
        tt = (t or "").strip().lower()
        if not tt:
            continue
        if not tt.startswith("."):
            tt = "." + tt
        allowed.append(tt)
    if not allowed:
        return False
    return _get_file_extension(doc) in allowed


def _normalize_markdown_for_review(text: str) -> str:
    """
    Make output stable and easy to diff:
    - Normalize newlines
    - Collapse excessive blank lines
    - Fence table-like blocks (heuristic)
    """
    if not text:
        return ""

    txt = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in txt.split("\n")]

    # Collapse 2+ blank lines to a single blank line
    collapsed: List[str] = []
    blank_streak = 0
    for ln in lines:
        if ln.strip() == "":
            blank_streak += 1
            if blank_streak <= 1:
                collapsed.append("")
        else:
            blank_streak = 0
            collapsed.append(ln)

    # Block-level heuristics for table fencing
    blocks: List[List[str]] = []
    cur: List[str] = []
    for ln in collapsed:
        if ln.strip() == "":
            if cur:
                blocks.append(cur)
                cur = []
            blocks.append([""])  # preserve a single blank line separator
        else:
            cur.append(ln)
    if cur:
        blocks.append(cur)

    def looks_like_table(block_lines: List[str]) -> bool:
        if len(block_lines) < 2:
            return False
        joined = "\n".join(block_lines)
        if "```" in joined:
            return False
        if any("TABLE_P" in ln or "=== TABLE" in ln for ln in block_lines):
            return True
        pipe_lines = sum(1 for ln in block_lines if ln.count("|") >= 2)
        if pipe_lines >= max(2, int(len(block_lines) * 0.6)):
            return True
        # Columnar whitespace heuristic: 2+ occurrences of 2+ spaces on many lines
        ws_cols = sum(1 for ln in block_lines if len(re.findall(r"\s{2,}", ln)) >= 2)
        if ws_cols >= max(2, int(len(block_lines) * 0.6)):
            return True
        return False

    rendered: List[str] = []
    for block in blocks:
        if block == [""]:
            rendered.append("")
            continue
        if looks_like_table(block):
            rendered.append("```")
            rendered.extend(block)
            rendered.append("```")
        else:
            rendered.extend(block)
    return "\n".join(rendered).strip() + "\n"


def _build_markdown_header(header: Dict[str, Any]) -> str:
    # YAML-ish block without requiring PyYAML (keeps deps minimal)
    lines: List[str] = ["---"]
    for k, v in header.items():
        if v is None:
            v_str = ""
        elif isinstance(v, (dict, list)):
            v_str = json.dumps(v, ensure_ascii=False)
        else:
            v_str = str(v)
        lines.append(f"{k}: {v_str}")
    lines.append("---")
    return "\n".join(lines) + "\n"


def _select_parser(parser_backend: str, docling_kwargs: Dict[str, Any]) -> Tuple[Any, str]:
    backend = (parser_backend or "pdfplumber").lower().strip()
    if backend == "docling":
        if DoclingParser is None or not is_docling_available():
            return PDFPlumberParser(), "pdfplumber"
        # DoclingParser accepts ocr_mode/timeout_seconds; keep kwargs minimal and safe
        return DoclingParser(**docling_kwargs), "docling"
    if backend == "mistral":
        if MistralParser is None or not is_mistral_available():
            return PDFPlumberParser(), "pdfplumber"
        return MistralParser(), "mistral"
    return PDFPlumberParser(), "pdfplumber"


def _extract_doc_metadata(doc: Dict[str, Any]) -> DocumentMetadata:
    file_info = doc.get("file_info", {}) or {}
    deal_meta = doc.get("deal_metadata", {}) or {}

    # Keep required fields safe with defaults
    name = str(file_info.get("name", "") or "unknown.pdf")
    path = str(file_info.get("path", "") or "")
    file_type = str(file_info.get("file_type", "") or Path(name).suffix or "")
    modified_time = str(file_info.get("modified_time", "") or "")
    size = int(file_info.get("size", 0) or 0)

    # Build DocumentMetadata with lots of optional fields (dataclass tolerates missing if we pass only required + known)
    return DocumentMetadata(
        path=path,
        name=name,
        size=size,
        size_mb=round(size / (1024 * 1024), 2) if size else 0.0,
        file_type=file_type,
        modified_time=modified_time,
        # Salesforce-ish
        deal_id=str(deal_meta.get("deal_id") or "") or None,
        salesforce_deal_id=str(deal_meta.get("salesforce_deal_id") or "") or None,
        salesforce_client_id=str(deal_meta.get("salesforce_client_id") or "") or None,
        client_name=str(deal_meta.get("client_name") or "") or None,
        vendor_name=str(deal_meta.get("vendor_name") or "") or None,
    )


def _doc_id_for(doc_meta: DocumentMetadata) -> str:
    parts = []
    if doc_meta.salesforce_deal_id:
        parts.append(_safe_filename(doc_meta.salesforce_deal_id, 40))
    elif doc_meta.deal_id:
        parts.append(_safe_filename(doc_meta.deal_id, 40))
    parts.append(_safe_filename(doc_meta.name, 120))
    return "__".join([p for p in parts if p])[:220]


def run(args: argparse.Namespace) -> int:
    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"ERROR: discovery file not found: {input_path}")
        return 2

    discovery = json.loads(input_path.read_text(encoding="utf-8"))
    meta = discovery.get("discovery_metadata", {}) or {}
    docs = discovery.get("documents", []) or []

    source_type = str(meta.get("source_type", "") or "")
    source_path = str(meta.get("source_path", "") or "")
    if not source_path:
        print("ERROR: discovery_metadata.source_path is required to locate files on disk.")
        return 2

    selected_candidates = [d for d in docs if _matches_file_types(d, args.file_types)]
    if not selected_candidates:
        print(f"ERROR: no matching files found in discovery file for file_types={args.file_types}")
        return 2

    seed = int(args.seed)
    rnd = random.Random(seed)
    rnd.shuffle(selected_candidates)
    selected = selected_candidates[: int(args.limit)] if args.limit else selected_candidates

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.output_root).resolve() / f"redaction_harness_{timestamp}"
    docs_dir = run_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    # Ensure span-detector diagnostics (and any other errors) are persisted for later grep.
    # By default, Python logging may only show to stderr (and can be missed if you don't capture terminal output).
    log_path = run_dir / "redaction_harness.log"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(log_path) for h in root_logger.handlers):
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        root_logger.addHandler(fh)

    filesystem = LocalFilesystemClient(base_path=source_path, openai_api_key=os.getenv("OPENAI_API_KEY"))
    converter = DocumentConverter()
    parser, effective_parser_backend = _select_parser(args.parser_backend, docling_kwargs={
        "ocr_mode": args.docling_ocr_mode,
        "timeout_seconds": args.docling_timeout_seconds,
    })

    llm_detector = None
    if args.enable_llm_person_redaction:
        api_key = os.getenv("OPENAI_API_KEY") or ""
        if api_key.strip():
            from src.redaction.llm_span_detector import LLMSpanDetector

            llm_detector = LLMSpanDetector(api_key=api_key, model=args.redaction_model)
        else:
            print("WARNING: OPENAI_API_KEY not set; disabling LLM PERSON/ORG redaction.")

    client_registry = ClientRegistry(args.client_redaction_csv)
    redaction_service = RedactionService(
        client_registry=client_registry,
        llm_span_detector=llm_detector,
        strict_mode=bool(args.strict_mode),
    )

    manifest: Dict[str, Any] = {
        "run": {
            "timestamp": timestamp,
            "discovery_file": str(input_path),
            "source_type": source_type,
            "source_path": source_path,
            "sample_size": len(selected),
            "seed": seed,
            "parser_backend": args.parser_backend,
            "effective_parser_backend": effective_parser_backend,
            "llm_person_redaction_enabled": bool(llm_detector is not None),
            "redaction_model": args.redaction_model if llm_detector else None,
            "strict_mode": bool(args.strict_mode),
            "include_header": bool(args.include_header),
            "log_file": str(log_path),
        },
        "documents": [],
        "stats": {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "validation_failed": 0,
            "empty_extractions": 0,
        },
    }

    for doc in selected:
        manifest["stats"]["attempted"] += 1
        doc_meta = _extract_doc_metadata(doc)
        doc_id = _doc_id_for(doc_meta)
        out_doc_dir = docs_dir / doc_id
        out_doc_dir.mkdir(parents=True, exist_ok=True)

        # Resolve and copy exact bytes used
        try:
            file_bytes = filesystem.download_file(doc_meta.path)
        except Exception as e:
            manifest["stats"]["failed"] += 1
            manifest["documents"].append(
                {
                    "doc_id": doc_id,
                    "file_name": doc_meta.name,
                    "relative_path": doc_meta.path,
                    "success": False,
                    "errors": [f"download_failed: {e}"],
                }
            )
            continue

        # Persist original file bytes with correct extension for reviewer context
        original_ext = (doc_meta.file_type or Path(doc_meta.name).suffix or "").strip().lower()
        if original_ext and not original_ext.startswith("."):
            original_ext = "." + original_ext
        if not original_ext:
            original_ext = ".bin"
        original_filename = f"original{original_ext}"
        (out_doc_dir / original_filename).write_bytes(file_bytes)

        # Extract
        processed_content, content_type = converter.convert_to_processable_content(
            doc_meta.path, file_bytes, doc_meta.name
        )
        # Determine the *actual* content parser used for this file type.
        # Note: for non-PDFs, DocumentConverter has already done the real parsing (e.g., python-docx),
        # and the PDF parser is not relevant (and is often misleading in reporting).
        content_parser = "unknown"
        if original_ext == ".pdf":
            content_parser = effective_parser_backend
        elif original_ext in [".xlsx", ".xls", ".csv"]:
            content_parser = "pandas_openpyxl"
        elif original_ext == ".docx":
            content_parser = "python_docx"
        elif original_ext == ".doc":
            content_parser = "docx2txt"
        elif original_ext == ".msg":
            content_parser = "extract_msg"
        elif original_ext == ".pptx":
            content_parser = "python_pptx"
        elif original_ext in [".png", ".jpg", ".jpeg"]:
            content_parser = f"image_to_pdf_{effective_parser_backend}"
        elif original_ext == ".txt":
            content_parser = "direct_text"

        # Parse
        if content_type == "text":
            # The converter returns UTF-8 text bytes. Avoid routing through Docling/PDFPlumber because
            # it obscures the real parser used for Word/Excel/MSG/PPTX.
            try:
                original_text = processed_content.decode("utf-8")
            except Exception:
                original_text = processed_content.decode("utf-8", errors="replace")
        else:
            parsed = parser.parse(processed_content, asdict(doc_meta), content_type)
            original_text = parsed.text or ""
        extracted_chars = len(original_text)
        extracted_words = len(original_text.split()) if original_text else 0
        extraction_empty = extracted_words == 0

        # Redact
        # IMPORTANT: keep this harness isolated from production behavior.
        # RedactionContext.has_client_info currently requires an industry_label;
        # for test harness usage we populate it from the ClientRegistry when possible.
        industry_label = None
        if doc_meta.salesforce_client_id:
            client_info = client_registry.get_client_info(doc_meta.salesforce_client_id)
            if client_info:
                industry_label = client_info.get("industry_label") or None

        redaction_context = RedactionContext(
            salesforce_client_id=doc_meta.salesforce_client_id,
            client_name=doc_meta.client_name,
            industry_label=industry_label,
            vendor_name=doc_meta.vendor_name,
            file_type=doc_meta.file_type,
            document_type=doc_meta.document_type,
        )
        redaction_result = redaction_service.redact(original_text, redaction_context)

        # Build docs + json
        redaction_json = {
            "doc_id": doc_id,
            "file_name": doc_meta.name,
            "relative_path": doc_meta.path,
            "deal_id": doc_meta.deal_id,
            "salesforce_deal_id": doc_meta.salesforce_deal_id,
            "salesforce_client_id": doc_meta.salesforce_client_id,
            "client_name": doc_meta.client_name,
            "vendor_name": doc_meta.vendor_name,
            "parser_backend": effective_parser_backend,
            "content_parser": content_parser,
            "redaction_model": redaction_result.model_used,
            "strict_mode": bool(args.strict_mode),
            "extraction": {
                "text_chars": extracted_chars,
                "word_count": extracted_words,
                "empty_extraction": extraction_empty,
            },
            "counts": {
                "client": redaction_result.client_replacements,
                "person": redaction_result.person_replacements,
                "email": redaction_result.email_replacements,
                "phone": redaction_result.phone_replacements,
                "address": redaction_result.address_replacements,
                "total": redaction_result.total_replacements(),
            },
            "validation": {
                "passed": bool(redaction_result.validation_passed),
                "failures": redaction_result.validation_failures,
            },
            "warnings": redaction_result.warnings,
            "errors": redaction_result.errors,
        }
        (out_doc_dir / "redaction.json").write_text(
            json.dumps(redaction_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        # Markdown artifacts
        header = {
            "deal_id": doc_meta.deal_id or "",
            "salesforce_deal_id": doc_meta.salesforce_deal_id or "",
            "salesforce_client_id": doc_meta.salesforce_client_id or "",
            "client_name": doc_meta.client_name or "",
            "vendor_name": doc_meta.vendor_name or "",
            "file_name": doc_meta.name,
            "relative_path": doc_meta.path,
            "parser_backend": effective_parser_backend,
            "content_parser": content_parser,
            "redaction_model": redaction_result.model_used or "",
            "strict_mode": bool(args.strict_mode),
            "replacement_counts": redaction_json["counts"],
            "validation_passed": bool(redaction_result.validation_passed),
            "validation_failures": redaction_result.validation_failures,
            "errors": redaction_result.errors,
        }

        if args.emit_original_md:
            original_md_body = _normalize_markdown_for_review(original_text)
            original_md = ""
            if args.include_header:
                original_md += _build_markdown_header({**header, "stage": "original"})
            original_md += "# Document\n\n## Extracted Content\n\n" + original_md_body
            (out_doc_dir / "original.md").write_text(original_md, encoding="utf-8")

        redacted_md_body = _normalize_markdown_for_review(redaction_result.redacted_text)
        redacted_md = ""
        if args.include_header:
            redacted_md += _build_markdown_header({**header, "stage": "redacted"})
        redacted_md += "# Document\n\n## Extracted Content\n\n" + redacted_md_body
        (out_doc_dir / "redacted.md").write_text(redacted_md, encoding="utf-8")

        doc_entry = {
            "doc_id": doc_id,
            "file_name": doc_meta.name,
            "relative_path": doc_meta.path,
            "deal_id": doc_meta.deal_id,
            "salesforce_deal_id": doc_meta.salesforce_deal_id,
            "salesforce_client_id": doc_meta.salesforce_client_id,
            "client_name": doc_meta.client_name,
            "vendor_name": doc_meta.vendor_name,
            "content_parser": content_parser,
            "success": bool(redaction_result.success),
            "validation_passed": bool(redaction_result.validation_passed),
            "extraction": {
                "text_chars": extracted_chars,
                "word_count": extracted_words,
                "empty_extraction": extraction_empty,
            },
            "counts": redaction_json["counts"],
            "artifacts": {
                "original_file": f"docs/{doc_id}/{original_filename}",
                "original_md": f"docs/{doc_id}/original.md" if args.emit_original_md else None,
                "redacted_md": f"docs/{doc_id}/redacted.md",
                "redaction_json": f"docs/{doc_id}/redaction.json",
            },
        }
        manifest["documents"].append(doc_entry)

        if redaction_result.success:
            manifest["stats"]["succeeded"] += 1
        else:
            manifest["stats"]["failed"] += 1
        if not redaction_result.validation_passed:
            manifest["stats"]["validation_failed"] += 1
        if extraction_empty:
            manifest["stats"]["empty_extractions"] += 1

    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Review index
    index_lines: List[str] = []
    index_lines.append("# Redaction Review Index\n")
    index_lines.append("## Run Metadata\n")
    index_lines.append(f"- **timestamp**: {timestamp}")
    index_lines.append(f"- **discovery_file**: `{input_path}`")
    index_lines.append(f"- **source_type**: `{source_type}`")
    index_lines.append(f"- **source_path**: `{source_path}`")
    index_lines.append(f"- **parser_backend**: `{args.parser_backend}` (effective: `{effective_parser_backend}`)")
    index_lines.append(f"- **strict_mode**: `{bool(args.strict_mode)}`")
    index_lines.append(f"- **llm_person_redaction_enabled**: `{bool(llm_detector is not None)}`")
    index_lines.append("")

    index_lines.append("## Documents\n")
    index_lines.append("| doc_id | type | content_parser | original | redacted.md | words | client | person | email | phone | address | validation | verdict | notes |")
    index_lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|")

    for d in manifest["documents"]:
        doc_id = d["doc_id"]
        a = d.get("artifacts", {}) or {}
        counts = d.get("counts", {}) or {}
        extraction = d.get("extraction", {}) or {}
        validation = "PASS" if d.get("validation_passed") else "FAIL"
        words = extraction.get("word_count", 0) or 0
        original_link = a.get("original_file") or a.get("original_pdf") or ""
        file_ext = (Path(d.get("file_name") or "").suffix or "").lower()
        content_parser = d.get("content_parser") or ""
        index_lines.append(
            "| "
            + f"`{doc_id}`"
            + f" | `{file_ext or ''}`"
            + f" | `{content_parser}`"
            + f" | [original]({original_link})"
            + f" | [redacted.md]({a.get('redacted_md')})"
            + f" | {words}"
            + f" | {counts.get('client', 0)}"
            + f" | {counts.get('person', 0)}"
            + f" | {counts.get('email', 0)}"
            + f" | {counts.get('phone', 0)}"
            + f" | {counts.get('address', 0)}"
            + f" | **{validation}**"
            + " | "
            + " | "
            + " |"
        )

    (run_dir / "REDACTION_REVIEW_INDEX.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

    print(f"✅ Wrote run folder: {run_dir}")
    print(f"   - manifest.json")
    print(f"   - REDACTION_REVIEW_INDEX.md")
    print(f"   - docs/*/original.pdf + redacted.md + redaction.json")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Local redaction harness (PDF → extracted markdown → redacted markdown)")
    p.add_argument("--input", required=True, help="Path to discovery JSON (from discover_documents.py)")
    p.add_argument("--output-root", default="output", help="Base output directory (default: output)")
    p.add_argument("--limit", type=int, default=10, help="Max documents to process (default: 10)")
    p.add_argument("--seed", type=int, default=1337, help="Sampling seed (default: 1337)")
    p.add_argument(
        "--file-types",
        nargs="+",
        default=[".pdf"],
        help="File extensions to include (default: .pdf). Example: --file-types .docx",
    )

    p.add_argument(
        "--parser-backend",
        choices=["pdfplumber", "docling", "mistral"],
        default="docling",
        help="Parser backend (default: docling; falls back to pdfplumber if unavailable). For .docx/.xlsx/.msg this is treated as a text parser.",
    )
    p.add_argument("--docling-ocr-mode", choices=["on", "off", "auto"], default="on")
    p.add_argument("--docling-timeout-seconds", type=int, default=240)

    p.add_argument("--client-redaction-csv", required=True, help="Client registry CSV (SF-Cust-Mapping.csv is OK)")
    p.add_argument("--redaction-model", default="gpt-5-mini-2025-08-07", help="OpenAI model for PERSON/ORG spans")
    p.add_argument("--strict-mode", action="store_true", default=False, help="Fail on validation failures (recommended)")
    p.add_argument(
        "--enable-llm-person-redaction",
        action="store_true",
        default=False,
        help="Enable LLM PERSON/ORG span detection via OpenAI (requires OPENAI_API_KEY).",
    )

    p.add_argument("--emit-original-md", action="store_true", default=False, help="Write original.md per doc")
    p.add_argument("--include-header", action="store_true", default=True, help="Include metadata header in markdown")
    p.add_argument("--no-include-header", action="store_false", dest="include_header")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()


