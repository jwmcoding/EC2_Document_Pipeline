"""
Mistral OCR-based PDF parser for the document processing pipeline.

This parser uses the Mistral OCR API to extract markdown/text from PDFs and maps
the result into the existing ``ParsedContent`` shape so downstream components
remain unchanged. Tables are passed through as plain text; when markdown tables
are present, they are formatted for chunker preservation.
"""

from __future__ import annotations

import logging
import io
import os
import signal
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from mistralai import Mistral
from pypdf import PdfReader, PdfWriter

from src.parsers.pdfplumber_parser import ParsedContent
from src.parsers.table_formatter import format_table_for_chunking

logger = logging.getLogger(__name__)

LARGE_PDF_SPLIT_THRESHOLD_MB_DEFAULT: float = 20.0
LARGE_PDF_SPLIT_PAGES_PER_CHUNK_DEFAULT: int = 10


def is_mistral_available() -> bool:
    """Check whether Mistral OCR can be used (package + API key present)."""
    return bool(os.getenv("MISTRAL_API_KEY"))


class MistralParser:
    """
    PDF parser backed by the Mistral OCR API.

    For ``content_type == "pdf"`` this uploads the PDF to Mistral, runs OCR, and
    returns combined text (and markdown tables when provided). For
    ``content_type == "text"`` it mirrors the text path of PDFPlumber to keep
    a uniform API.
    """

    def __init__(
        self,
        model: str = "mistral-ocr-latest",
        timeout_seconds: int = 240,
        include_image_base64: bool = False,
        split_large_pdfs_over_mb: float = LARGE_PDF_SPLIT_THRESHOLD_MB_DEFAULT,
        split_pages_per_chunk: int = LARGE_PDF_SPLIT_PAGES_PER_CHUNK_DEFAULT,
    ) -> None:
        """
        Initialize the Mistral OCR parser.

        Args:
            model: Mistral OCR model name (default: mistral-ocr-latest).
            timeout_seconds: Hard timeout for an individual PDF conversion.
            include_image_base64: Whether to request base64 images (disabled to
                reduce payload size).
            split_large_pdfs_over_mb: If a PDF is larger than this threshold (MB),
                split it into page-range chunks and OCR each chunk. This avoids
                wedging workers on very large PDFs.
            split_pages_per_chunk: Page count per chunk when splitting large PDFs.
        """

        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            raise RuntimeError(
                "Mistral OCR is not configured. Set MISTRAL_API_KEY in the environment."
            )

        # IMPORTANT: configure request-level timeout on the SDK client. A signal alarm
        # does not reliably interrupt blocking HTTP requests inside the SDK.
        self.client = Mistral(api_key=api_key, timeout_ms=int(timeout_seconds * 1000))
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.include_image_base64 = include_image_base64
        self.split_large_pdfs_over_mb = float(split_large_pdfs_over_mb)
        self.split_pages_per_chunk = int(split_pages_per_chunk)
        self.logger = logging.getLogger(__name__)
        
        # Validate API key works with a quick test
        self._validate_api_key()

    def _validate_api_key(self) -> None:
        """
        Test the Mistral API key with a minimal API call.
        
        Raises:
            RuntimeError: If API key is invalid or API is unreachable
        """
        try:
            # Try to list files (minimal API call that requires valid auth)
            # This will fail fast if the API key is invalid
            self.client.files.list()
            self.logger.info("✅ Mistral API key validated successfully")
        except Exception as e:
            error_msg = str(e)
            if "401" in error_msg or "Unauthorized" in error_msg:
                raise RuntimeError(
                    f"❌ Mistral API key is invalid or unauthorized. "
                    f"Check MISTRAL_API_KEY in .env file. Error: {error_msg}"
                )
            elif "403" in error_msg or "Forbidden" in error_msg:
                raise RuntimeError(
                    f"❌ Mistral API access forbidden. "
                    f"Check your API key permissions. Error: {error_msg}"
                )
            else:
                raise RuntimeError(
                    f"❌ Failed to validate Mistral API connection: {error_msg}"
                )

    def parse(
        self, content: bytes, metadata: Dict[str, Any], content_type: str = "pdf"
    ) -> ParsedContent:
        """
        Parse content via Mistral OCR or direct text decoding.

        Args:
            content: Raw bytes (PDF for OCR; UTF-8 text for ``content_type='text'``).
            metadata: Document metadata dict (usually ``DocumentMetadata`` asdict).
            content_type: Either ``'pdf'`` or ``'text'``.
        """

        if content_type == "text":
            return self._parse_text_content(content, metadata)
        return self._parse_pdf_content(content, metadata)

    def _parse_text_content(
        self, content: bytes, metadata: Dict[str, Any]
    ) -> ParsedContent:
        """Mirror the text path from PDFPlumberParser for consistency."""

        try:
            text = content.decode("utf-8")
        except Exception as e:  # pragma: no cover - unlikely for UTF-8 text path
            self.logger.error("MistralParser text parsing failed: %s", e)
            raise

        enhanced_metadata = {
            **metadata,
            "parser": "mistral_text",
            "text_length": len(text),
            "processing_method": "direct_text",
        }

        return ParsedContent(text=text, metadata=enhanced_metadata)

    def _parse_pdf_content(
        self, content: bytes, metadata: Dict[str, Any]
    ) -> ParsedContent:
        """Parse PDF content using Mistral OCR with a hard timeout."""

        @contextmanager
        def timeout_context(timeout_seconds: int):
            """Unix-only timeout guard; no-op on Windows."""

            if os.name == "nt":
                # Windows does not support SIGALRM; skip timeout but log it.
                self.logger.debug(
                    "MistralParser: Windows detected - timeout guard disabled"
                )
                yield
                return

            def timeout_handler(signum, frame):  # pragma: no cover - signal path
                raise TimeoutError(
                    f"Mistral OCR processing exceeded {timeout_seconds} seconds"
                )

            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            try:
                yield
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

        # Large PDF guard: split into smaller PDFs and OCR each chunk.
        threshold_bytes = int(self.split_large_pdfs_over_mb * 1024 * 1024)
        if threshold_bytes > 0 and len(content) > threshold_bytes:
            return self._parse_large_pdf_in_chunks(content=content, metadata=metadata)

        tmp_path: Optional[Path] = None

        try:
            with tempfile.NamedTemporaryFile(
                suffix=".pdf", delete=False
            ) as tmp_file:
                tmp_file.write(content)
                tmp_file.flush()
                tmp_path = Path(tmp_file.name)

            with timeout_context(self.timeout_seconds):
                start_ts = time.time()

                self.logger.info(
                    "MistralParser: uploading PDF '%s'",
                    metadata.get("name") or metadata.get("path") or tmp_path.name,
                )
                with tmp_path.open("rb") as f:
                    uploaded_file = self.client.files.upload(
                        file={"file_name": tmp_path.name, "content": f}, purpose="ocr"
                    )

                signed_url = self.client.files.get_signed_url(file_id=uploaded_file.id)

                ocr_response = self.client.ocr.process(
                    model=self.model,
                    document={
                        "type": "document_url",
                        "document_url": signed_url.url,
                    },
                    include_image_base64=self.include_image_base64,
                )

                # Parse response: prefer page-level markdown, then text/content fallbacks
                text_parts: List[str] = []
                tables: List[Dict[str, Any]] = []
                page_info: List[Dict[str, Any]] = []

                if hasattr(ocr_response, "pages") and ocr_response.pages:
                    for idx, page in enumerate(ocr_response.pages, 1):
                        page_markdown = getattr(page, "markdown", None)
                        if page_markdown:
                            text_parts.append(f"=== Page {idx} ===")
                            text_parts.append(page_markdown)
                            text_parts.append("")  # blank line between pages
                        page_info.append(
                            {
                                "page_number": idx,
                                "text_length": len(page_markdown) if page_markdown else 0,
                                "table_count": 0,  # Mistral response does not expose structured tables
                            }
                        )
                elif hasattr(ocr_response, "text"):
                    text_parts.append(str(getattr(ocr_response, "text")))
                elif hasattr(ocr_response, "content"):
                    text_parts.append(str(getattr(ocr_response, "content")))
                else:
                    text_parts.append(str(ocr_response))

                # Basic table handling: if markdown contains tables, format for chunker
                formatted_tables = self._extract_tables_from_markdown(text_parts)
                if formatted_tables:
                    tables = formatted_tables["tables"]
                    text_parts.extend(formatted_tables["formatted_blocks"])

                full_text = "\n".join(text_parts).strip()

                processing_time = time.time() - start_ts
                if processing_time > 10.0:
                    self.logger.warning(
                        "MistralParser: PDF processing took %.1fs - consider tuning or splitting document",
                        processing_time,
                    )

                enhanced_metadata = {
                    **metadata,
                    "parser": "mistral_ocr",
                    "total_pages": len(page_info) if page_info else None,
                    "total_tables": len(tables),
                    "text_length": len(full_text),
                    "processing_method": "mistral_extraction",
                }

                return ParsedContent(
                    text=full_text,
                    metadata=enhanced_metadata,
                    tables=tables,
                    page_info=page_info,
                )

        except TimeoutError as e:
            # Fail the document rather than returning an "error text" payload that could
            # be chunked + embedded + upserted into Pinecone.
            self.logger.error("MistralParser PDF processing timeout: %s", e)
            raise TimeoutError(
                "Mistral OCR processing timed out after "
                f"{self.timeout_seconds} seconds for file "
                f"{metadata.get('name') or metadata.get('path') or 'unknown'}"
            ) from e
        except Exception as e:
            error_str = str(e)
            error_type = self._classify_api_error(error_str)
            
            # Log with appropriate severity
            if error_type in ["rate_limit", "spending_limit", "service_unavailable"]:
                self.logger.error(
                    "🚨 MistralParser API ERROR: %s - %s", 
                    error_type.replace("_", " ").title(), 
                    error_str
                )
            else:
                self.logger.error("MistralParser PDF parsing failed: %s", e)
            
            # Create actionable error message and fail the document (do not upsert it)
            msg = self._create_error_message(error_type, error_str, metadata)
            raise RuntimeError(msg) from e
        finally:
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                except Exception:
                    self.logger.debug(
                        "MistralParser: failed to remove temp file %s", tmp_path
                    )

    def _parse_large_pdf_in_chunks(self, content: bytes, metadata: Dict[str, Any]) -> ParsedContent:
        """
        Split a large PDF into page-range chunks and OCR each chunk.

        This is a reliability feature: very large PDFs can wedge workers if the OCR
        request blocks for too long. Splitting reduces worst-case latency and
        improves retry behavior.
        """
        if self.split_pages_per_chunk <= 0:
            raise ValueError("split_pages_per_chunk must be > 0")

        try:
            reader = PdfReader(io.BytesIO(content))
            total_pages = len(reader.pages)
        except Exception as e:
            raise RuntimeError(
                f"Failed to read PDF for chunked OCR: {metadata.get('name') or metadata.get('path') or 'unknown'}"
            ) from e

        if total_pages <= 0:
            raise RuntimeError(
                f"PDF has no pages: {metadata.get('name') or metadata.get('path') or 'unknown'}"
            )

        self.logger.warning(
            "MistralParser: large PDF detected (%.2f MB, %d pages) — splitting into %d-page chunks",
            len(content) / (1024 * 1024),
            total_pages,
            self.split_pages_per_chunk,
        )

        combined_text_parts: List[str] = []
        combined_tables: List[Dict[str, Any]] = []
        combined_page_info: List[Dict[str, Any]] = []
        chunk_count = 0
        start_ts_all = time.time()

        for start_page in range(0, total_pages, self.split_pages_per_chunk):
            end_page = min(start_page + self.split_pages_per_chunk, total_pages)
            chunk_count += 1

            writer = PdfWriter()
            for i in range(start_page, end_page):
                writer.add_page(reader.pages[i])

            buf = io.BytesIO()
            writer.write(buf)
            chunk_bytes = buf.getvalue()

            # OCR this chunk and re-number pages in output to global page numbers.
            chunk_meta = {
                **metadata,
                "chunked_ocr": True,
                "chunk_index": chunk_count - 1,
                "chunk_page_start": start_page + 1,  # 1-based for humans
                "chunk_page_end": end_page,
            }

            parsed_chunk = self._parse_pdf_content(chunk_bytes, chunk_meta)  # will not recurse (chunk < threshold)

            # Adjust any "=== Page X ===" markers to global page numbers.
            # This is best-effort string rewrite; keeps downstream chunker stable.
            text = parsed_chunk.text or ""
            if text:
                adjusted_lines: List[str] = []
                for line in text.splitlines():
                    if line.startswith("=== Page ") and line.endswith(" ==="):
                        try:
                            num_str = line[len("=== Page ") : -len(" ===")].strip()
                            local_num = int(num_str)
                            global_num = start_page + local_num
                            adjusted_lines.append(f"=== Page {global_num} ===")
                            continue
                        except Exception:
                            pass
                    adjusted_lines.append(line)
                text = "\n".join(adjusted_lines).strip()

            combined_text_parts.append(text)

            # Merge tables + page info
            if parsed_chunk.tables:
                combined_tables.extend(parsed_chunk.tables)
            if parsed_chunk.page_info:
                for pi in parsed_chunk.page_info:
                    if isinstance(pi, dict) and "page_number" in pi:
                        try:
                            pi = {**pi, "page_number": int(pi["page_number"]) + start_page}
                        except Exception:
                            pass
                    combined_page_info.append(pi)

        full_text = "\n\n".join([t for t in combined_text_parts if t]).strip()
        elapsed_all = time.time() - start_ts_all

        enhanced_metadata = {
            **metadata,
            "parser": "mistral_ocr_chunked",
            "processing_method": "mistral_extraction_chunked",
            "total_pages": total_pages,
            "chunk_count": chunk_count,
            "chunk_pages": self.split_pages_per_chunk,
            "text_length": len(full_text),
            "processing_time_seconds": round(elapsed_all, 3),
        }

        return ParsedContent(
            text=full_text,
            metadata=enhanced_metadata,
            tables=combined_tables,
            page_info=combined_page_info,
        )

    def _classify_api_error(self, error_str: str) -> str:
        """
        Classify API error by status code or message content.
        
        Args:
            error_str: Error message string from exception
            
        Returns:
            Error type classification string
        """
        error_lower = error_str.lower()
        
        # Check for specific HTTP status codes and error types
        if "401" in error_str or "unauthorized" in error_lower:
            return "unauthorized"
        elif "429" in error_str or "rate limit" in error_lower or "too many requests" in error_lower:
            return "rate_limit"
        elif "402" in error_str or "payment" in error_lower or "billing" in error_lower or "quota" in error_lower:
            return "spending_limit"
        elif "403" in error_str or "forbidden" in error_lower:
            return "forbidden"
        elif "503" in error_str or "service unavailable" in error_lower or "temporarily unavailable" in error_lower:
            return "service_unavailable"
        elif "504" in error_str or "timeout" in error_lower or "gateway" in error_lower:
            return "gateway_timeout"
        elif "500" in error_str or "internal server" in error_lower:
            return "server_error"
        else:
            return "unknown"
    
    def _create_error_message(self, error_type: str, error_str: str, metadata: Dict[str, Any]) -> str:
        """
        Create an actionable error message based on the error type.
        
        Args:
            error_type: Classified error type
            error_str: Original error string
            metadata: Document metadata
            
        Returns:
            Formatted error message with suggested actions
        """
        file_name = metadata.get('name', 'unknown')
        
        error_messages = {
            "unauthorized": (
                f"🚨 MISTRAL API ERROR: Unauthorized (401)\n\n"
                f"Your Mistral API key is invalid or has been revoked.\n\n"
                f"ACTION REQUIRED:\n"
                f"1. Check your API key at: https://console.mistral.ai/api-keys/\n"
                f"2. Update MISTRAL_API_KEY in your .env file\n"
                f"3. Restart the pipeline\n\n"
                f"File: {file_name}\n"
                f"Error: {error_str}"
            ),
            "rate_limit": (
                f"🚨 MISTRAL API ERROR: Rate Limit Exceeded (429)\n\n"
                f"You've hit Mistral's rate limit (requests per minute/hour).\n\n"
                f"ACTION REQUIRED:\n"
                f"1. Wait a few minutes for the rate limit to reset\n"
                f"2. Reduce --workers count to slow down request rate\n"
                f"3. Or upgrade your Mistral API plan at: https://console.mistral.ai/billing/\n\n"
                f"File: {file_name}\n"
                f"Error: {error_str}"
            ),
            "spending_limit": (
                f"🚨 MISTRAL API ERROR: Spending Limit Reached (402)\n\n"
                f"You've reached your Mistral API spending limit for this billing period.\n\n"
                f"ACTION REQUIRED:\n"
                f"1. Check usage at: https://console.mistral.ai/billing/\n"
                f"2. Increase your spending limit or add credits\n"
                f"3. Or switch to --parser-backend pdfplumber (free, local)\n\n"
                f"File: {file_name}\n"
                f"Error: {error_str}"
            ),
            "service_unavailable": (
                f"⚠️ MISTRAL API ERROR: Service Temporarily Unavailable (503)\n\n"
                f"Mistral's API is temporarily unavailable or overloaded.\n\n"
                f"SUGGESTED ACTIONS:\n"
                f"1. Wait 5-10 minutes and retry with --resume\n"
                f"2. Check Mistral status at: https://status.mistral.ai/\n"
                f"3. Or switch to --parser-backend pdfplumber for now\n\n"
                f"File: {file_name}\n"
                f"Error: {error_str}"
            ),
            "gateway_timeout": (
                f"⚠️ MISTRAL API ERROR: Gateway Timeout (504)\n\n"
                f"The API request timed out (network or server issue).\n\n"
                f"SUGGESTED ACTIONS:\n"
                f"1. Check your internet connection\n"
                f"2. Retry with --resume (may be a transient issue)\n"
                f"3. If persistent, switch to --parser-backend pdfplumber\n\n"
                f"File: {file_name}\n"
                f"Error: {error_str}"
            ),
            "server_error": (
                f"⚠️ MISTRAL API ERROR: Internal Server Error (500)\n\n"
                f"Mistral's API encountered an internal error.\n\n"
                f"SUGGESTED ACTIONS:\n"
                f"1. Wait a few minutes and retry with --resume\n"
                f"2. Report to Mistral if persistent\n"
                f"3. Or switch to --parser-backend pdfplumber\n\n"
                f"File: {file_name}\n"
                f"Error: {error_str}"
            ),
        }
        
        # Return specific message or generic fallback
        return error_messages.get(
            error_type,
            f"Mistral OCR extraction failed: {error_str}\n\n"
            f"File: {file_name}\n"
            f"This may indicate a corrupted PDF, API issue, or configuration problem.\n\n"
            f"Try switching to --parser-backend pdfplumber or docling."
        )

    def _extract_tables_from_markdown(
        self, text_parts: List[str]
    ) -> Dict[str, List[Any]]:
        """
        Best-effort detection of markdown-style tables in Mistral output.

        This is heuristic; it looks for typical markdown table headers ('|'
        separated lines) and formats them using the unified table formatter so
        the chunker preserves them.
        """

        tables: List[Dict[str, Any]] = []
        formatted_blocks: List[str] = []

        current_table: List[List[str]] = []
        table_page_hint = None
        table_index = 0

        for block in text_parts:
            lines = block.splitlines()
            for line in lines:
                stripped = line.strip()
                # Simple heuristic: a markdown table header has at least two pipes
                if stripped.startswith("|") and stripped.count("|") >= 2:
                    cells = [c.strip() for c in stripped.strip("|").split("|")]
                    current_table.append(cells)
                elif current_table and ("---" in stripped or stripped.startswith("|")):
                    # Continue table if separator or subsequent rows
                    cells = [c.strip() for c in stripped.strip("|").split("|")]
                    if cells:
                        current_table.append(cells)
                else:
                    # Flush current table if we were collecting one
                    if current_table:
                        table_name = (
                            f"TABLE_P{table_page_hint}_{table_index + 1}"
                            if table_page_hint
                            else f"TABLE_{table_index + 1}"
                        )
                        formatted = format_table_for_chunking(
                            current_table, table_name, page_num=table_page_hint
                        )
                        if formatted:
                            tables.append(
                                {
                                    "page": table_page_hint or 0,
                                    "table_index": table_index,
                                    "data": current_table,
                                    "rows": len(current_table),
                                    "cols": len(current_table[0])
                                    if current_table and current_table[0]
                                    else 0,
                                }
                            )
                            formatted_blocks.append("")
                            formatted_blocks.append(formatted)
                            table_index += 1
                        current_table = []
                    formatted_blocks.append(line)

        # Flush trailing table
        if current_table:
            table_name = f"TABLE_{table_index + 1}"
            formatted = format_table_for_chunking(
                current_table, table_name, page_num=table_page_hint
            )
            if formatted:
                tables.append(
                    {
                        "page": table_page_hint or 0,
                        "table_index": table_index,
                        "data": current_table,
                        "rows": len(current_table),
                        "cols": len(current_table[0]) if current_table[0] else 0,
                    }
                )
                formatted_blocks.append("")
                formatted_blocks.append(formatted)

        return {"tables": tables, "formatted_blocks": formatted_blocks}

