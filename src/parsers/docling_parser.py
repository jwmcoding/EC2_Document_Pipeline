"""
Docling-based PDF parser for the document processing pipeline.

This parser uses Granite Docling's ``DocumentConverter`` to extract markdown
and structured tables from PDFs and maps them into the same ``ParsedContent``
shape as the existing ``PDFPlumberParser``. Tables are normalized and then
formatted via the unified ``table_formatter`` so that the
``SemanticChunker`` can detect and preserve them as single chunks.

Design goals:
- Production-ready: timeouts, logging, and graceful fallbacks.
- Optional dependency: if Docling is not installed, the module degrades
  gracefully and callers can fall back to PDFPlumber.
- Schema-compatible: returns ``ParsedContent`` from ``pdfplumber_parser``
  so downstream pipeline code does not need to change.
"""

from __future__ import annotations

import json
import logging
import platform
import re
import signal
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from src.parsers.pdfplumber_parser import ParsedContent
from src.parsers.table_formatter import format_table_for_chunking

logger = logging.getLogger(__name__)


try:
    # Core Docling converter
    from docling.document_converter import DocumentConverter as DoclingDocumentConverter
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
    from docling.document_converter import PdfFormatOption

    DOCLING_AVAILABLE = True
except Exception as exc:  # pragma: no cover - import-time guard
    DoclingDocumentConverter = None  # type: ignore[assignment]
    DOCLING_AVAILABLE = False
    logger.warning(
        "Docling is not available. Install 'docling>=2.54.0 transformers>=4.56.2 torch>=2.0.0' "
        "to enable DoclingParser. Error: %s",
        exc,
    )


@dataclass
class _NormalizedTable:
    """Internal normalized table representation derived from Docling JSON."""

    page: int
    table_index: int
    data: List[List[str]]
    row_count: int
    col_count: int
    contains_currency: bool


def is_docling_available() -> bool:
    """Public helper so callers can check for Docling support."""

    return DOCLING_AVAILABLE


def _compute_quality_metrics(
    text: str, table_count: int
) -> Dict[str, Any]:
    """
    Compute quality metrics for pass1 output.
    
    Returns:
        Dict with text_chars, word_count, alnum_ratio, table_count
    """
    text_chars = len(text)
    words = re.findall(r'\b\w+\b', text)
    word_count = len(words)
    
    # Compute alphanumeric ratio (alnum chars / total chars, excluding whitespace)
    text_no_ws = re.sub(r'\s+', '', text)
    if text_no_ws:
        alnum_chars = len(re.findall(r'[a-zA-Z0-9]', text_no_ws))
        alnum_ratio = alnum_chars / len(text_no_ws)
    else:
        alnum_ratio = 0.0
    
    return {
        "text_chars": text_chars,
        "word_count": word_count,
        "alnum_ratio": alnum_ratio,
        "table_count": table_count,
    }


def _check_pass1_quality(
    text: str,
    table_count: int,
    min_text_chars: int = 800,
    min_word_count: int = 150,
    alnum_threshold: float = 0.5,
) -> bool:
    """
    Evaluate whether pass1 output is sufficient (no OCR needed).
    
    Checks:
    1. Minimum content volume: require at least min_text_chars OR min_word_count
    2. Text quality ratio: require alnum_ratio >= threshold
    3. Table signal: if table_count == 0 AND text volume is low, treat as failed
    
    Returns:
        True if pass1 is sufficient, False if OCR fallback is needed
    """
    metrics = _compute_quality_metrics(text, table_count)
    
    # Check 1: Minimum content volume
    has_min_content = (
        metrics["text_chars"] >= min_text_chars
        or metrics["word_count"] >= min_word_count
    )
    
    # Check 2: Text quality ratio
    has_good_quality = metrics["alnum_ratio"] >= alnum_threshold
    
    # Check 3: Table signal (if no tables and low text, likely scanned)
    has_table_signal = (
        metrics["table_count"] > 0
        or metrics["text_chars"] >= min_text_chars
        or metrics["word_count"] >= min_word_count
    )
    
    # Pass if all checks succeed
    return has_min_content and has_good_quality and has_table_signal


class DoclingParser:
    """
    PDF parser backed by Docling's ``DocumentConverter``.

    For ``content_type == "pdf"`` this uses Docling to extract markdown and
    structured tables. Tables are normalized and then passed through
    ``format_table_for_chunking`` so they are preserved during chunking.

    For ``content_type == "text"`` this behaves like the text-path of
    ``PDFPlumberParser``, simply decoding UTF-8 bytes into text and wrapping
    them in ``ParsedContent``. That keeps the DocumentProcessor API uniform.
    """

    def __init__(
        self,
        ocr_mode: Optional[Literal["auto", "on", "off"]] = None,
        ocr: Optional[bool] = None,  # Backward compatibility
        timeout_seconds: int = 240,
        min_text_chars: int = 800,
        min_word_count: int = 150,
        alnum_threshold: float = 0.5,
    ) -> None:
        """
        Initialize the Docling-backed parser.

        Args:
            ocr_mode: OCR behavior mode:
                - "auto": Try non-OCR first, fallback to OCR if quality checks fail (default)
                - "on": Always use OCR (legacy behavior)
                - "off": Never use OCR
            ocr: DEPRECATED - use ocr_mode instead. If provided, maps to ocr_mode:
                - True -> "on"
                - False -> "off"
            timeout_seconds: Total timeout cap for PDF conversion (split between passes in AUTO mode).
            min_text_chars: Minimum character count for pass1 success (AUTO mode).
            min_word_count: Minimum word count for pass1 success (AUTO mode).
            alnum_threshold: Minimum alphanumeric ratio for pass1 success (AUTO mode).
        """
        # Backward compatibility: convert old `ocr` bool to `ocr_mode`
        if ocr_mode is None:
            if ocr is not None:
                # Legacy parameter provided
                ocr_mode = "on" if ocr else "off"
                self.logger.warning(
                    "DoclingParser: 'ocr' parameter is deprecated, use 'ocr_mode' instead. "
                    "Mapping ocr=%s to ocr_mode='%s'",
                    ocr,
                    ocr_mode,
                )
            else:
                # Default to AUTO mode
                ocr_mode = "auto"

        self.logger = logging.getLogger(__name__)

        if not DOCLING_AVAILABLE:
            raise RuntimeError(
                "Docling is not installed. Please install: "
                "pip install 'docling>=2.54.0' 'transformers>=4.56.2' 'torch>=2.0.0'"
            )

        self.timeout_seconds = timeout_seconds
        self.ocr_mode = ocr_mode
        self.min_text_chars = min_text_chars
        self.min_word_count = min_word_count
        self.alnum_threshold = alnum_threshold

        # Create converter without OCR (for pass1 in AUTO mode, or when ocr_mode="off")
        self.converter_no_ocr = self._create_converter(ocr_enabled=False)

        # Create converter with OCR (for pass2 in AUTO mode, or when ocr_mode="on")
        if ocr_mode in ("auto", "on"):
            self.converter_ocr = self._create_converter(ocr_enabled=True)
        else:
            self.converter_ocr = None

        # For backward compatibility: if ocr_mode="on", use OCR converter as primary
        if ocr_mode == "on":
            self.converter = self.converter_ocr
        else:
            self.converter = self.converter_no_ocr

        self.logger.info(
            "DoclingParser initialized with ocr_mode=%s (timeout=%ss)",
            ocr_mode,
            timeout_seconds,
        )

    def _create_converter(self, ocr_enabled: bool) -> Any:
        """Create a Docling converter with specified OCR settings."""
        try:
            pipeline_options = PdfPipelineOptions()
            
            # Always enable TableFormer ACCURATE for table fidelity
            pipeline_options.do_table_structure = True
            pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
            pipeline_options.table_structure_options.do_cell_matching = True
            
            if ocr_enabled:
                pipeline_options.do_ocr = True
                pipeline_options.images_scale = 2.0  # 2x scale for better OCR
                
                # OCR settings for scanned documents
                # Try EasyOCR first (better accuracy), then Tesseract, then default
                try:
                    from docling.datamodel.pipeline_options import EasyOcrOptions
                    pipeline_options.ocr_options = EasyOcrOptions(
                        lang=["en"],
                        force_full_page_ocr=True,
                        confidence_threshold=0.5,
                    )
                    self.logger.debug("DoclingParser: Using EasyOCR backend")
                except ImportError:
                    try:
                        from docling.datamodel.pipeline_options import TesseractCliOcrOptions
                        pipeline_options.ocr_options = TesseractCliOcrOptions(
                            lang=["eng"],
                            force_full_page_ocr=True,
                        )
                        self.logger.debug("DoclingParser: Using Tesseract OCR backend")
                    except ImportError:
                        self.logger.warning(
                            "DoclingParser: No OCR options available (install easyocr for better results)"
                        )

            return DoclingDocumentConverter(
                format_options={PdfFormatOption: pipeline_options}
            )
        except Exception as e:
            self.logger.warning(
                "DoclingParser: failed to configure pipeline options (%s). "
                "Falling back to default converter.",
                e,
            )
            return DoclingDocumentConverter()

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    def parse(
        self, content: bytes, metadata: Dict[str, Any], content_type: str = "pdf"
    ) -> ParsedContent:
        """
        Parse content via Docling or direct text decoding.

        Args:
            content: Raw bytes (PDF for Docling; UTF-8 text for ``content_type='text'``).
            metadata: Document metadata dict (usually ``DocumentMetadata`` asdict).
            content_type: Either ``'pdf'`` or ``'text'``. Only PDFs are routed
                through Docling; text content is decoded directly.
        """

        if content_type == "text":
            return self._parse_text_content(content, metadata)
        return self._parse_pdf_content(content, metadata)

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #

    def _parse_text_content(
        self, content: bytes, metadata: Dict[str, Any]
    ) -> ParsedContent:
        """Mirror the text path from ``PDFPlumberParser`` for consistency."""

        try:
            text = content.decode("utf-8")
        except Exception as e:  # pragma: no cover - very unlikely for UTF-8 text path
            self.logger.error("DoclingParser text parsing failed: %s", e)
            raise

        enhanced_metadata = {
            **metadata,
            "parser": "docling_text",
            "text_length": len(text),
            "processing_method": "direct_text",
        }

        return ParsedContent(text=text, metadata=enhanced_metadata)

    def _parse_pdf_content(
        self, content: bytes, metadata: Dict[str, Any]
    ) -> ParsedContent:
        """
        Parse PDF content using Docling with two-pass AUTO OCR support.
        
        In AUTO mode:
        - Pass1: Try without OCR (fast path for digitally-native PDFs)
        - Pass2: Fallback to OCR if pass1 quality checks fail
        
        Timeout is split between passes to maintain total cap.
        """
        
        # Handle non-AUTO modes directly
        if self.ocr_mode == "off":
            return self._parse_single_pass(content, metadata, use_ocr=False, pass_num=1)
        elif self.ocr_mode == "on":
            return self._parse_single_pass(content, metadata, use_ocr=True, pass_num=1)
        
        # AUTO mode: two-pass with quality checks
        # Split timeout: pass1 gets 20s (fast path for digitally-native PDFs), pass2 gets remainder
        # If Pass1 takes >20s, it's likely scanned/complex and will need OCR anyway
        pass1_timeout = min(20, int(self.timeout_seconds * 0.15))  # 15% of total, max 20s
        pass2_timeout = self.timeout_seconds - pass1_timeout
        
        # Pass1: Try without OCR
        self.logger.debug(
            "DoclingParser AUTO: Pass1 (OCR off) for '%s'",
            metadata.get("name") or metadata.get("path") or "unknown",
        )
        pass1_result = self._parse_single_pass(
            content, metadata, use_ocr=False, pass_num=1, timeout_seconds=pass1_timeout
        )
        
        # Check if Pass1 resulted in timeout/error - if so, skip quality check and go straight to Pass2
        if pass1_result.metadata.get("parser", "").startswith("docling_timeout") or \
           pass1_result.metadata.get("parser", "").startswith("docling_fallback") or \
           pass1_result.metadata.get("error"):
            self.logger.info(
                "DoclingParser AUTO: Pass1 timed out or failed, trying Pass2 with OCR"
            )
            # Skip quality checks and go straight to Pass2
            if self.converter_ocr is None:
                self.logger.warning(
                    "DoclingParser AUTO: Pass1 failed but OCR converter not available, returning pass1 result"
                )
                pass1_result.metadata.update({
                    "docling_ocr_mode": "auto",
                    "docling_ocr_used": False,
                    "docling_pass_used": 1,
                    "docling_ocr_fallback_failed": True,
                })
                return pass1_result
            
            pass2_result = self._parse_single_pass(
                content, metadata, use_ocr=True, pass_num=2, timeout_seconds=pass2_timeout
            )
            
            pass2_text = pass2_result.text
            pass2_table_count = len(pass2_result.tables) if pass2_result.tables else 0
            pass2_metrics = _compute_quality_metrics(pass2_text, pass2_table_count)
            
            pass2_result.metadata.update({
                "docling_ocr_mode": "auto",
                "docling_ocr_used": True,
                "docling_pass_used": 2,
                "docling_text_chars": pass2_metrics["text_chars"],
                "docling_word_count": pass2_metrics["word_count"],
                "docling_alnum_ratio": pass2_metrics["alnum_ratio"],
                "docling_table_count": pass2_metrics["table_count"],
            })
            
            return pass2_result
        
        # Check pass1 quality (only if Pass1 didn't timeout/error)
        pass1_text = pass1_result.text or ""
        pass1_table_count = len(pass1_result.tables) if pass1_result.tables else 0
        pass1_metrics = _compute_quality_metrics(pass1_text, pass1_table_count)
        
        if _check_pass1_quality(
            pass1_text,
            pass1_table_count,
            self.min_text_chars,
            self.min_word_count,
            self.alnum_threshold,
        ):
            # Pass1 succeeded - return with metrics
            self.logger.info(
                "DoclingParser AUTO: Pass1 sufficient (chars=%d, words=%d, tables=%d, alnum=%.2f)",
                pass1_metrics["text_chars"],
                pass1_metrics["word_count"],
                pass1_metrics["table_count"],
                pass1_metrics["alnum_ratio"],
            )
            pass1_result.metadata.update({
                "docling_ocr_mode": "auto",
                "docling_ocr_used": False,
                "docling_pass_used": 1,
                "docling_text_chars": pass1_metrics["text_chars"],
                "docling_word_count": pass1_metrics["word_count"],
                "docling_alnum_ratio": pass1_metrics["alnum_ratio"],
                "docling_table_count": pass1_metrics["table_count"],
            })
            return pass1_result
        
        # Pass1 failed quality checks - try pass2 with OCR
        if self.converter_ocr is None:
            self.logger.warning(
                "DoclingParser AUTO: Pass1 failed but OCR converter not available, returning pass1 result"
            )
            pass1_result.metadata.update({
                "docling_ocr_mode": "auto",
                "docling_ocr_used": False,
                "docling_pass_used": 1,
                "docling_text_chars": pass1_metrics["text_chars"],
                "docling_word_count": pass1_metrics["word_count"],
                "docling_alnum_ratio": pass1_metrics["alnum_ratio"],
                "docling_table_count": pass1_metrics["table_count"],
                "docling_ocr_fallback_failed": True,
            })
            return pass1_result
        
        self.logger.info(
            "DoclingParser AUTO: Pass1 insufficient (chars=%d, words=%d, tables=%d, alnum=%.2f), trying Pass2 with OCR",
            pass1_metrics["text_chars"],
            pass1_metrics["word_count"],
            pass1_metrics["table_count"],
            pass1_metrics["alnum_ratio"],
        )
        
        pass2_result = self._parse_single_pass(
            content, metadata, use_ocr=True, pass_num=2, timeout_seconds=pass2_timeout
        )
        
        pass2_text = pass2_result.text
        pass2_table_count = len(pass2_result.tables)
        pass2_metrics = _compute_quality_metrics(pass2_text, pass2_table_count)
        
        pass2_result.metadata.update({
            "docling_ocr_mode": "auto",
            "docling_ocr_used": True,
            "docling_pass_used": 2,
            "docling_text_chars": pass2_metrics["text_chars"],
            "docling_word_count": pass2_metrics["word_count"],
            "docling_alnum_ratio": pass2_metrics["alnum_ratio"],
            "docling_table_count": pass2_metrics["table_count"],
        })
        
        return pass2_result

    def _parse_single_pass(
        self,
        content: bytes,
        metadata: Dict[str, Any],
        use_ocr: bool,
        pass_num: int,
        timeout_seconds: Optional[int] = None,
    ) -> ParsedContent:
        """Parse PDF content using a single Docling pass (with or without OCR)."""

        @contextmanager
        def timeout_context(timeout_sec: int):
            """Unix-only timeout guard; no-op on Windows."""

            if platform.system() == "Windows":
                self.logger.debug(
                    "DoclingParser: Windows detected - timeout guard disabled"
                )
                yield
                return

            def timeout_handler(signum, frame):  # pragma: no cover - signal path
                raise TimeoutError(
                    f"Docling PDF processing exceeded {timeout_sec} seconds"
                )

            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_sec)
            try:
                yield
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

        timeout_sec = timeout_seconds or self.timeout_seconds
        converter = self.converter_ocr if use_ocr else self.converter_no_ocr
        tmp_path: Optional[Path] = None

        try:
            # Write bytes to a temporary file for Docling.
            with tempfile.NamedTemporaryFile(
                suffix=".pdf", delete=False
            ) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                tmp_path = Path(tmp_file.name)

            tables: List[Dict[str, Any]] = []
            page_info: List[Dict[str, Any]] = []

            try:
                with timeout_context(timeout_sec):
                    start_ts = time.time()

                    self.logger.debug(
                        "DoclingParser: Pass%d converting PDF '%s' (OCR=%s)",
                        pass_num,
                        metadata.get("name") or metadata.get("path") or tmp_path.name,
                        use_ocr,
                    )
                    result = converter.convert(str(tmp_path))
                    doc = result.document

                    # Primary text representation
                    markdown_text: str = doc.export_to_markdown() or ""

                    # Lossless DocTags JSON for structural information (tables, pages, etc.)
                    lossless_json: Dict[str, Any] = {}
                    try:
                        # Some Docling versions expose export_to_dict_json, others export_to_dict
                        if hasattr(doc, "export_to_dict_json"):
                            lossless_json = json.loads(doc.export_to_dict_json())  # type: ignore[attr-defined]
                        elif hasattr(doc, "export_to_dict"):
                            lossless_json = doc.export_to_dict()  # type: ignore[attr-defined]
                    except Exception as e:
                        self.logger.warning(
                            "DoclingParser: failed to export lossless JSON: %s", e
                        )
                        lossless_json = {}

                    # Extract page count from JSON if available
                    page_count = self._extract_page_count(lossless_json)

                    # Normalize tables from structural JSON; if nothing found, we
                    # still return markdown-only content.
                    normalized_tables = self._normalize_tables_from_lossless(lossless_json)

                    # Build tables list for ParsedContent (schema-compatible with PDFPlumberParser)
                    table_dicts: List[Dict[str, Any]] = []
                    for tbl in normalized_tables:
                        table_dicts.append(
                            {
                                "page": tbl.page,
                                "table_index": tbl.table_index,
                                "data": tbl.data,
                                "rows": tbl.row_count,
                                "cols": tbl.col_count,
                                "contains_currency": tbl.contains_currency,
                            }
                        )

                    # Format tables using the unified formatter so the chunker
                    # recognizes them. We append them at the end of the markdown
                    # to avoid disrupting Docling's own layout while still
                    # ensuring tables are preserved as dedicated table sections.
                    text_parts: List[str] = [markdown_text] if markdown_text else []
                    if table_dicts:
                        text_parts.append("")  # Blank line before tables block
                        text_parts.append("=== EXTRACTED TABLES (Docling) ===")
                        for tbl in normalized_tables:
                            table_name = f"TABLE_P{tbl.page}_{tbl.table_index + 1}"
                            formatted = format_table_for_chunking(
                                tbl.data, table_name, page_num=tbl.page
                            )
                            if formatted:
                                text_parts.append("")
                                text_parts.append(formatted)

                    full_text = "\n".join(text_parts) if text_parts else ""

                    # Minimal page_info based on page_count (we do not have width/height here).
                    if page_count is not None:
                        for p in range(1, page_count + 1):
                            page_info.append(
                                {
                                    "page_number": p,
                                    "text_length": None,
                                    "table_count": len(
                                        [t for t in table_dicts if t["page"] == p]
                                    ),
                                }
                            )

                    processing_time = time.time() - start_ts
                    if processing_time > 10.0:
                        self.logger.warning(
                            "DoclingParser: Pass%d processing took %.1fs",
                            pass_num,
                            processing_time,
                        )

                    enhanced_metadata = {
                        **metadata,
                        "parser": "docling",
                        "total_pages": page_count or len(page_info) or 0,
                        "total_tables": len(table_dicts),
                        "text_length": len(full_text),
                        "processing_method": f"docling_extraction_pass{pass_num}",
                    }

                    return ParsedContent(
                        text=full_text,
                        metadata=enhanced_metadata,
                        tables=table_dicts,
                        page_info=page_info,
                    )

            except TimeoutError as e:
                self.logger.error(
                    "DoclingParser Pass%d timeout: %s", pass_num, e
                )
                # Return sentinel ParsedContent for timeout
                return ParsedContent(
                    text=(
                        f"Docling PDF processing (Pass{pass_num}) timed out after "
                        f"{timeout_sec} seconds. "
                        f"File may be corrupt or extremely complex: "
                        f"{metadata.get('name', 'unknown')}"
                    ),
                    metadata={
                        **metadata,
                        "parser": f"docling_timeout_pass{pass_num}",
                        "error": str(e),
                        "processing_time": timeout_sec,
                        "docling_ocr_mode": self.ocr_mode,
                        "docling_ocr_used": use_ocr,
                        "docling_pass_used": pass_num,
                    },
                )
            except Exception as e:
                self.logger.error(
                    "DoclingParser Pass%d parsing failed: %s", pass_num, e
                )
                # Return fallback ParsedContent
                fallback_text = (
                    f"Docling extraction (Pass{pass_num}) failed: {str(e)}\n\n"
                    f"File: {metadata.get('name', 'unknown')}\n"
                    "This may indicate a corrupted or extremely complex PDF."
                )
                return ParsedContent(
                    text=fallback_text,
                    metadata={
                        **metadata,
                        "parser": f"docling_fallback_pass{pass_num}",
                        "error": str(e),
                        "docling_ocr_mode": self.ocr_mode,
                        "docling_ocr_used": use_ocr,
                        "docling_pass_used": pass_num,
                    },
                )
        finally:
            # Clean up temp file if we created one.
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except Exception:
                    # Non-fatal; just log at debug level.
                    self.logger.debug(
                        "DoclingParser: failed to remove temp file %s", tmp_path
                    )

    # ------------------------------------------------------------------ #
    # Lossless JSON table normalization helpers
    # ------------------------------------------------------------------ #

    def _extract_page_count(self, lossless_json: Dict[str, Any]) -> Optional[int]:
        """Best-effort page count extraction from Docling JSON."""

        try:
            pages = lossless_json.get("pages") or lossless_json.get(
                "document", {}
            ).get("pages")
            if isinstance(pages, list):
                return len(pages)
        except Exception:
            return None
        return None

    def _normalize_tables_from_lossless(
        self, lossless_json: Dict[str, Any]
    ) -> List[_NormalizedTable]:
        """
        Walk Docling's lossless JSON tree and extract table-like nodes.

        This mirrors the proven pattern from the Contract IQ Docling integration,
        but returns an internal ``_NormalizedTable`` that we later map to the
        pipeline's table schema and to the unified table formatter.
        """

        tables: List[_NormalizedTable] = []
        if not lossless_json:
            return tables

        def walk(node: Any, page_hint: Optional[int] = None) -> None:
            if isinstance(node, dict):
                node_type = node.get("type") or node.get("kind")

                # Table-like node detection
                if (
                    node_type
                    and str(node_type).lower()
                    in ("table", "table_block", "tablecellset")
                ) or ("cells" in node and isinstance(node["cells"], list)):
                    page_no = (
                        node.get("page")
                        or node.get("page_number")
                        or node.get("pageIndex")
                        or page_hint
                        or 1
                    )
                    cell_rows = self._extract_cells(node)
                    if cell_rows:
                        contains_currency = self._contains_currency(cell_rows)
                        current_index = len(
                            [t for t in tables if t.page == int(page_no)]
                        )
                        tables.append(
                            _NormalizedTable(
                                page=int(page_no),
                                table_index=current_index,
                                data=cell_rows,
                                row_count=len(cell_rows),
                                col_count=len(cell_rows[0]) if cell_rows else 0,
                                contains_currency=contains_currency,
                            )
                        )

                # Recurse into structural children
                for key, value in node.items():
                    if key in (
                        "children",
                        "blocks",
                        "elements",
                        "content",
                        "items",
                        "nodes",
                        "pages",
                    ):
                        if isinstance(value, list):
                            for child in value:
                                walk(
                                    child,
                                    page_hint=(
                                        node.get("page")
                                        or node.get("page_number")
                                        or page_hint
                                    ),
                                )
                        else:
                            walk(
                                value,
                                page_hint=(
                                    node.get("page")
                                    or node.get("page_number")
                                    or page_hint
                                ),
                            )

            elif isinstance(node, list):
                for child in node:
                    walk(child, page_hint=page_hint)

        walk(lossless_json)
        return tables

    def _extract_cells(self, node: Dict[str, Any]) -> List[List[str]]:
        """
        Extract a 2D array of cell strings from a generic table node.

        Handles common Docling shapes:
        - ``{"cells": [[...], ...]}``
        - ``{"rows": [{"cells": [...]}, ...]}``
        """

        cells = node.get("cells")
        if isinstance(cells, list) and cells and isinstance(cells[0], list):
            return [[self._cell_text(c) for c in row] for row in cells]

        rows = node.get("rows")
        if isinstance(rows, list):
            normalized: List[List[str]] = []
            for r in rows:
                r_cells = r.get("cells") if isinstance(r, dict) else None
                if isinstance(r_cells, list):
                    normalized.append([self._cell_text(c) for c in r_cells])
            if normalized:
                return normalized

        return []

    def _cell_text(self, cell: Any) -> str:
        """Extract plain text from a Docling cell structure."""

        if isinstance(cell, dict):
            if "text" in cell:
                return str(cell.get("text") or "").strip()
            if "content" in cell and isinstance(cell["content"], list):
                parts: List[str] = []
                for item in cell["content"]:
                    if isinstance(item, dict) and "text" in item:
                        parts.append(str(item.get("text") or "").strip())
                return " ".join([p for p in parts if p])
        return str(cell or "").strip()

    def _contains_currency(self, table_data: List[List[str]]) -> bool:
        """Heuristic: detect if a table appears to contain currency amounts."""

        if not table_data:
            return False
        currency_markers = ["$", "€", "£", "¥", "USD", "EUR", "GBP"]
        joined = " ".join(" ".join(row) for row in table_data).upper()
        return any(marker in joined for marker in currency_markers)

