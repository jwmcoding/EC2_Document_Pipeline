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
import signal
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.parsers.pdfplumber_parser import ParsedContent
from src.parsers.table_formatter import format_table_for_chunking

logger = logging.getLogger(__name__)


try:
    # Core Docling converter
    from docling.document_converter import DocumentConverter as DoclingDocumentConverter
    from docling.datamodel.pipeline_options import PdfPipelineOptions, OcrOptions
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

    def __init__(self, ocr: bool = True, ocr_mode: Optional[str] = None, timeout_seconds: int = 240, **kwargs) -> None:
        """
        Initialize the Docling-backed parser.

        Args:
            ocr: Enable Docling OCR for scanned PDFs (recommended for contracts).
            ocr_mode: Optional OCR mode string ("on", "off", "auto") - if provided, overrides ocr boolean.
            timeout_seconds: Hard timeout for an individual PDF conversion.
            **kwargs: Ignored for backward compatibility.
        """
        # Handle ocr_mode parameter for backward compatibility
        if ocr_mode is not None:
            ocr = ocr_mode.lower() in ("on", "auto")

        self.logger = logging.getLogger(__name__)

        if not DOCLING_AVAILABLE:
            raise RuntimeError(
                "Docling is not installed. Please install: "
                "pip install 'docling>=2.54.0' 'transformers>=4.56.2' 'torch>=2.0.0'"
            )

        self.timeout_seconds = timeout_seconds

        # Configure Docling PDF pipeline with robust OCR defaults when enabled.
        # NOTE: GPU acceleration is automatic in Docling - it detects and uses
        # MPS (Apple Silicon) or CUDA when available.
        if ocr:
            try:
                pipeline_options = PdfPipelineOptions()
                pipeline_options.do_ocr = True
                pipeline_options.images_scale = 2.0  # 2x scale for better OCR
                
                # Enable TableFormer ACCURATE mode for high-quality table extraction
                try:
                    from docling.datamodel.pipeline_options import TableFormerMode
                    pipeline_options.do_table_structure = True
                    pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
                    pipeline_options.table_structure_options.do_cell_matching = True
                    self.logger.info(
                        "DoclingParser: TableFormer ACCURATE mode enabled "
                        "(do_table_structure=%s, mode=%s, do_cell_matching=%s)",
                        pipeline_options.do_table_structure,
                        pipeline_options.table_structure_options.mode,
                        pipeline_options.table_structure_options.do_cell_matching,
                    )
                except (ImportError, AttributeError) as e:
                    # TableFormer options not available in this Docling version
                    self.logger.warning(
                        "DoclingParser: TableFormer options not available (%s). "
                        "Tables may not be extracted correctly.",
                        e,
                    )
                
                pipeline_options.ocr_options = OcrOptions(
                    lang=["en"],
                    force_full_page_ocr=True,
                    bitmap_area_threshold=0.0,
                    use_gpu=True,
                )

                # Prefer EasyOCR backend when available for tough scans.
                try:
                    from docling.backend.ocr_backend import EasyOcrBackend  # type: ignore[import]

                    pipeline_options.ocr_options.ocr_backend = EasyOcrBackend()
                    self.logger.info("DoclingParser: Using EasyOCR backend for OCR")
                except Exception:
                    # Fallback to the default OCR backend configured in Docling.
                    self.logger.info("DoclingParser: Using default OCR backend")

                self.converter = DoclingDocumentConverter(
                    format_options={PdfFormatOption: pipeline_options}
                )
                self.logger.info(
                    "DoclingParser initialized with FULL-PAGE OCR enabled (timeout=%ss)",
                    timeout_seconds,
                )
            except Exception as e:
                self.logger.warning(
                    "DoclingParser: failed to configure OCR options (%s). "
                    "Falling back to default converter without custom pipeline.",
                    e,
                )
                self.converter = DoclingDocumentConverter()
        else:
            self.converter = DoclingDocumentConverter()
            self.logger.info(
                "DoclingParser initialized without OCR (timeout=%ss)", timeout_seconds
            )

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
        """Parse PDF content using Docling with a hard timeout."""

        @contextmanager
        def timeout_context(timeout_seconds: int):
            """Unix-only timeout guard; no-op on Windows."""

            if platform.system() == "Windows":
                # Windows does not support SIGALRM; skip timeout but log it.
                self.logger.debug(
                    "DoclingParser: Windows detected - timeout guard disabled"
                )
                yield
                return

            def timeout_handler(signum, frame):  # pragma: no cover - signal path
                raise TimeoutError(
                    f"Docling PDF processing exceeded {timeout_seconds} seconds"
                )

            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            try:
                yield
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

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

            with timeout_context(self.timeout_seconds):
                start_ts = time.time()

                self.logger.info(
                    "DoclingParser: converting PDF '%s'",
                    metadata.get("name") or metadata.get("path") or tmp_path.name,
                )
                result = self.converter.convert(str(tmp_path))
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
                
                # Debug logging for table extraction
                self.logger.debug(
                    "DoclingParser: Extracted %d tables from lossless JSON (lossless_json keys: %s)",
                    len(normalized_tables),
                    list(lossless_json.keys()) if lossless_json else "empty",
                )
                if normalized_tables:
                    for i, tbl in enumerate(normalized_tables):
                        self.logger.debug(
                            "DoclingParser: Table %d: page=%d, rows=%d, cols=%d, "
                            "sample_data=%s",
                            i + 1,
                            tbl.page,
                            tbl.row_count,
                            tbl.col_count,
                            str(tbl.data[0][:3]) if tbl.data and tbl.data[0] else "empty",
                        )

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
                        # Use improved table formatting with better handling of merged cells
                        formatted = format_table_for_chunking(
                            tbl.data, table_name, page_num=tbl.page, deduplicate_merged=True
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
                        "DoclingParser: PDF processing took %.1fs - consider tuning or splitting document",
                        processing_time,
                    )

                enhanced_metadata = {
                    **metadata,
                    "parser": "docling",
                    "total_pages": page_count or len(page_info) or 0,
                    "total_tables": len(table_dicts),
                    "text_length": len(full_text),
                    "processing_method": "docling_extraction",
                }

                return ParsedContent(
                    text=full_text,
                    metadata=enhanced_metadata,
                    tables=table_dicts,
                    page_info=page_info,
                )

        except TimeoutError as e:
            self.logger.error("DoclingParser PDF processing timeout: %s", e)
            # Return a sentinel ParsedContent so upstream logging and error
            # handling has something coherent to work with.
            return ParsedContent(
                text=(
                    "Docling PDF processing timed out after "
                    f"{self.timeout_seconds} seconds. "
                    f"File may be corrupt or extremely complex: "
                    f"{metadata.get('name', 'unknown')}"
                ),
                metadata={
                    **metadata,
                    "parser": "docling_timeout",
                    "error": str(e),
                    "processing_time": self.timeout_seconds,
                },
            )
        except Exception as e:
            self.logger.error("DoclingParser PDF parsing failed: %s", e)
            # Align with PDFPlumberParser behavior: return a basic fallback
            # text + error metadata instead of raising.
            fallback_text = (
                f"Docling extraction failed: {str(e)}\n\n"
                f"File: {metadata.get('name', 'unknown')}\n"
                "This may indicate a corrupted or extremely complex PDF."
            )
            return ParsedContent(
                text=fallback_text,
                metadata={
                    **metadata,
                    "parser": "docling_fallback",
                    "error": str(e),
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
        
        Docling stores tables in two places:
        1. Top-level 'tables' array (preferred, structured format)
        2. Nested in 'body'/'children' (legacy format)
        """

        tables: List[_NormalizedTable] = []
        if not lossless_json:
            return tables

        # First, check top-level 'tables' array (Docling's preferred format)
        if 'tables' in lossless_json and isinstance(lossless_json['tables'], list):
            for table_idx, table_node in enumerate(lossless_json['tables']):
                if not isinstance(table_node, dict):
                    continue
                
                # Extract page number
                page_no = (
                    table_node.get("page")
                    or table_node.get("page_number")
                    or table_node.get("pageIndex")
                    or 1
                )
                
                # Extract table data - Docling uses 'data' key with 'table_cells' or 'grid'
                table_data = table_node.get('data', {})
                if isinstance(table_data, dict):
                    # Prefer 'grid' format (already 2D, better structure)
                    grid = table_data.get('grid')
                    if isinstance(grid, list) and grid and len(grid) > 0:
                        cell_rows = self._extract_cells_from_grid(grid)
                        if cell_rows:
                            # Post-process to merge multi-line descriptions
                            # cell_rows = self._merge_multiline_descriptions(cell_rows)  # Disabled - keeping grid format only
                            contains_currency = self._contains_currency(cell_rows)
                            tables.append(
                                _NormalizedTable(
                                    page=int(page_no),
                                    table_index=table_idx,
                                    data=cell_rows,
                                    row_count=len(cell_rows),
                                    col_count=len(cell_rows[0]) if cell_rows else 0,
                                    contains_currency=contains_currency,
                                )
                            )
                            self.logger.debug(
                                "DoclingParser: Extracted table %d from 'grid' format "
                                "(page=%d, rows=%d, cols=%d)",
                                table_idx + 1,
                                page_no,
                                len(cell_rows),
                                len(cell_rows[0]) if cell_rows else 0,
                            )
                            continue
                    
                    # Fallback to 'table_cells' (list of cell objects with row/col indices)
                    table_cells = table_data.get('table_cells')
                    if isinstance(table_cells, list) and table_cells:
                        cell_rows = self._extract_cells_from_table_cells(table_cells)
                        if cell_rows:
                            # Post-process to merge multi-line descriptions
                            # cell_rows = self._merge_multiline_descriptions(cell_rows)  # Disabled - keeping grid format only
                            contains_currency = self._contains_currency(cell_rows)
                            tables.append(
                                _NormalizedTable(
                                    page=int(page_no),
                                    table_index=table_idx,
                                    data=cell_rows,
                                    row_count=len(cell_rows),
                                    col_count=len(cell_rows[0]) if cell_rows else 0,
                                    contains_currency=contains_currency,
                                )
                            )
                            self.logger.debug(
                                "DoclingParser: Extracted table %d from 'table_cells' format "
                                "(page=%d, rows=%d, cols=%d)",
                                table_idx + 1,
                                page_no,
                                len(cell_rows),
                                len(cell_rows[0]) if cell_rows else 0,
                            )
                            continue

        # Fallback: walk the tree for nested tables (legacy format)
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

    def _extract_cells_from_grid(self, grid: List[List[Dict[str, Any]]]) -> List[List[str]]:
        """
        Extract 2D cell array from Docling's 'grid' format.
        
        Grid is a 2D array where each cell is a dict with 'text' key.
        Grid format is preferred as it's already structured and handles merged cells.
        """
        if not grid or not isinstance(grid, list):
            return []
        
        cell_rows = []
        for row in grid:
            if not isinstance(row, list):
                continue
            row_cells = []
            for cell in row:
                cell_text = self._cell_text(cell)
                row_cells.append(cell_text)
            if row_cells:  # Only add non-empty rows
                cell_rows.append(row_cells)
        
        return cell_rows

    def _merge_multiline_descriptions(self, cell_rows: List[List[str]]) -> List[List[str]]:
        """
        Merge multi-line descriptions that span multiple rows.
        
        Conservative heuristic: Only merge if:
        1. Current row has ONLY one non-empty column (likely continuation)
        2. Previous row has text in that same column (description column)
        3. Previous row has MULTIPLE non-empty columns (not a section header)
        4. Continuation text doesn't look like a section header (no ":" or "Total" keywords)
        
        This handles cases like:
        Row 1: ["TA-BASE01-0001", "Kofax TotalAgility Base Configuration...", "1", ...]
        Row 2: ["", "Transformation + 4 Full Users", "", "", ...]
        ->
        Row 1: ["TA-BASE01-0001", "Kofax TotalAgility Base Configuration... Transformation + 4 Full Users", "1", ...]
        
        But avoids merging legitimate single-column rows like:
        - Section headers: ["", "Kofax TotalAgility", "", ...]
        - Subtotals: ["", "", "", "License Sub-Total:", ...]
        """
        if not cell_rows or len(cell_rows) < 2:
            return cell_rows
        
        # Keywords that suggest a row is NOT a continuation (section headers, totals, etc.)
        section_keywords = ['total', 'subtotal', 'sub-total', 'summary', 'section', 'header']
        
        result_rows = []
        i = 0
        while i < len(cell_rows):
            row = cell_rows[i][:]
            
            # Check if this is a continuation row (only one column with text)
            non_empty_cols = [j for j, cell in enumerate(row) if cell and cell.strip()]
            
            # Conservative merge conditions
            if len(non_empty_cols) == 1 and i > 0:
                col_idx = non_empty_cols[0]
                continuation_text = row[col_idx].strip()
                
                # Skip if continuation text looks like a section header
                continuation_lower = continuation_text.lower()
                if any(keyword in continuation_lower for keyword in section_keywords):
                    # Likely a section header, don't merge
                    pass
                elif ':' in continuation_text and len(continuation_text) < 50:
                    # Likely a label (e.g., "Sub-Total:"), don't merge
                    pass
                else:
                    # Check if previous row has text in that same column AND has multiple columns
                    prev_row = cell_rows[i - 1]
                    prev_non_empty_cols = [j for j, cell in enumerate(prev_row) if cell and cell.strip()]
                    
                    if (col_idx < len(prev_row) and 
                        prev_row[col_idx] and prev_row[col_idx].strip() and
                        len(prev_non_empty_cols) > 1):  # Previous row must have multiple columns
                        # Merge the continuation text into previous row in result_rows
                        if len(result_rows) > 0:
                            result_rows[-1][col_idx] = f"{result_rows[-1][col_idx]} {continuation_text}".strip()
                        # Skip adding this row since we merged it
                        i += 1
                        continue
            
            # Only add non-empty rows
            if any(cell and cell.strip() for cell in row):
                result_rows.append(row)
            
            i += 1
        
        return result_rows

    def _extract_cells_from_table_cells(
        self, table_cells: List[Dict[str, Any]]
    ) -> List[List[str]]:
        """
        Extract 2D cell array from Docling's 'table_cells' format.
        
        Table_cells is a flat list of cell objects with row/col indices:
        - start_row_offset_idx, end_row_offset_idx
        - start_col_offset_idx, end_col_offset_idx
        - text, row_span, col_span
        - row_section (for subtotals/sections)
        
        Improvements:
        1. Properly handle merged cells using row_span/col_span
        2. Merge multi-line descriptions that span rows
        3. Better handling of merged columns
        """
        if not table_cells or not isinstance(table_cells, list):
            return []
        
        # Find max row and col indices
        max_row = 0
        max_col = 0
        for cell in table_cells:
            if isinstance(cell, dict):
                max_row = max(max_row, cell.get("end_row_offset_idx", 0))
                max_col = max(max_col, cell.get("end_col_offset_idx", 0))
        
        if max_row == 0 or max_col == 0:
            return []
        
        # Initialize 2D grid - store text directly, handle merged cells properly
        grid = [[""] * max_col for _ in range(max_row)]
        
        # Process cells sorted by position to handle merged cells correctly
        sorted_cells = sorted(
            table_cells,
            key=lambda c: (
                c.get("start_row_offset_idx", 0),
                c.get("start_col_offset_idx", 0),
            ),
        )
        
        for cell in sorted_cells:
            if not isinstance(cell, dict):
                continue
            
            row_start = cell.get("start_row_offset_idx", 0)
            col_start = cell.get("start_col_offset_idx", 0)
            col_span = cell.get("col_span", 1)
            
            cell_text = self._cell_text(cell)
            
            # Place text in origin cell (top-left of merged cell)
            if row_start < max_row and col_start < max_col:
                # Only fill if empty (to avoid overwriting)
                if not grid[row_start][col_start]:
                    grid[row_start][col_start] = cell_text
                    # For merged columns, mark spanned columns as processed
                    # (we'll handle display in the formatting step)
                    for c in range(col_start + 1, min(col_start + col_span, max_col)):
                        grid[row_start][c] = None  # Mark as merged continuation
        
        # Post-process: merge multi-line descriptions
        # If a row has only one non-empty column and next row has same pattern in same column,
        # merge them into a single row
        result_rows = []
        r = 0
        while r < max_row:
            row = grid[r][:]
            # Replace None markers with empty strings
            row = [cell if cell is not None else "" for cell in row]
            
            # Check if this is a continuation row (mostly empty, one column with text)
            non_empty_cols = [i for i, cell in enumerate(row) if cell and cell.strip()]
            
            # Try to merge with next row if it's a continuation
            if len(non_empty_cols) == 1 and r < max_row - 1:
                col_idx = non_empty_cols[0]
                next_row = grid[r + 1][:]
                next_row = [cell if cell is not None else "" for cell in next_row]
                next_non_empty_cols = [i for i, cell in enumerate(next_row) if cell and cell.strip()]
                
                # If next row is also a continuation in the same column, merge them
                if len(next_non_empty_cols) == 1 and next_non_empty_cols[0] == col_idx:
                    continuation_text = next_row[col_idx].strip()
                    if continuation_text:
                        # Merge the continuation text
                        row[col_idx] = f"{row[col_idx]} {continuation_text}".strip()
                        # Skip the next row since we merged it
                        r += 1
            
            # Only add non-empty rows
            if any(cell and cell.strip() for cell in row):
                result_rows.append(row)
            
            r += 1
        
        return result_rows

    def _contains_currency(self, table_data: List[List[str]]) -> bool:
        """Heuristic: detect if a table appears to contain currency amounts."""

        if not table_data:
            return False
        currency_markers = ["$", "€", "£", "¥", "USD", "EUR", "GBP"]
        joined = " ".join(" ".join(row) for row in table_data).upper()
        return any(marker in joined for marker in currency_markers)


