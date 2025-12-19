"""
Parallel Document Processor using multiprocessing

Design principles:
1. Each worker is a separate PROCESS (not thread) - required for signal.SIGALRM timeouts
2. Each worker initializes its own parser, chunker, Pinecone client
3. Main process coordinates work distribution and progress tracking
4. Graceful shutdown on Ctrl+C preserves progress
5. Resume capability via discovery JSON processing_status

CRITICAL: Do NOT convert to threading - signal-based PDF timeouts only work in main thread.
"""

from __future__ import annotations

import multiprocessing as mp
from multiprocessing import Process, Queue, Value
import signal
import time
import os
import sys
from typing import List, Dict, Any, Optional

# Processing constants (must be defined before use in function defaults)
DEFAULT_MAX_CHUNK_SIZE: int = 1500
DEFAULT_CHUNK_OVERLAP: int = 200
METADATA_TEXT_MAX_BYTES: int = 37 * 1024  # 37KB for Pinecone 40KB limit
WORKER_QUEUE_TIMEOUT_SECONDS: float = 1.0
PROGRESS_UPDATE_INTERVAL: int = 10
REDACTION_TIMEOUT_SECONDS: int = 300  # Hard timeout for redaction per document (OpenAI + span logic)
CHUNKING_TIMEOUT_SECONDS: int = 300  # Hard timeout for chunking per document (table scanning can be expensive)
CHUNKING_TIMEOUT_SECONDS_SPREADSHEETS: int = 60  # Spreadsheets are number-dense; fail fast + fallback chunking


def _truncate_text_for_metadata(text: str, max_bytes: int = METADATA_TEXT_MAX_BYTES) -> str:
    """
    Truncate text to fit within max_bytes when UTF-8 encoded.
    
    Pinecone metadata has a 40KB limit, so we use 37KB (37,888 bytes) as a safe margin.
    
    Args:
        text: Text to truncate
        max_bytes: Maximum size in bytes (default: 37KB)
    
    Returns:
        Truncated text that fits within max_bytes when UTF-8 encoded
    """
    if not text:
        return ""
    
    encoded = text.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text
    
    # Binary search for the right truncation point
    # Start with a conservative estimate
    truncate_at = int(len(text) * (max_bytes / len(encoded)))
    
    # Refine to find exact truncation point
    while True:
        truncated = text[:truncate_at]
        encoded_truncated = truncated.encode('utf-8')
        if len(encoded_truncated) <= max_bytes:
            # Try to add one more character
            if truncate_at < len(text):
                next_char = text[:truncate_at + 1]
                if len(next_char.encode('utf-8')) <= max_bytes:
                    truncate_at += 1
                    continue
            break
        else:
            # Too long, reduce
            truncate_at = int(truncate_at * 0.9)
    
    return text[:truncate_at]
from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
from pathlib import Path
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# Type hints
DocumentData = Dict[str, Any]
ProcessingResult = Dict[str, Any]

logger = logging.getLogger(__name__)

__all__ = [
    "WorkerStats",
    "worker_initializer",
    "process_single_document",
    "worker_main",
    "run_parallel_processing",
    "build_metadata_dict",
]


@dataclass
class WorkerStats:
    """Statistics collected by each worker process"""
    worker_id: int
    documents_processed: int = 0
    documents_failed: int = 0
    total_chunks: int = 0
    total_time: float = 0.0
    errors: List[str] = field(default_factory=list)


def worker_initializer(
    worker_id: int,
    parser_backend: str,
    pinecone_api_key: str,
    pinecone_index: str,
    openai_api_key: str,
    source_path: str,
    docling_kwargs: Optional[Dict[str, Any]] = None,
    redaction_service: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Initialize worker-local resources.
    
    CRITICAL: This runs ONCE per worker process at startup.
    Each worker gets its own:
    - Parser instance (DoclingParser or PDFPlumberParser)
    - PineconeDocumentClient (thread-safe HTTP client)
    - LocalFilesystemClient
    - SemanticChunker
    
    This ensures:
    - Signal handlers work (each process has own main thread)
    - No shared mutable state between workers
    - GPU memory is isolated per process
    """
    from src.connectors.pinecone_client import PineconeDocumentClient
    from src.connectors.local_filesystem_client import LocalFilesystemClient
    from src.chunking.semantic_chunker import SemanticChunker
    from src.parsers.document_converter import DocumentConverter
    
    # Select parser based on backend
    if parser_backend == "docling":
        try:
            from src.parsers.docling_parser import DoclingParser, is_docling_available
            if is_docling_available():
                # Use custom kwargs if provided, otherwise use defaults
                docling_init_kwargs = docling_kwargs or {}
                # Convert ocr_mode to ocr boolean for old API
                if "ocr_mode" in docling_init_kwargs:
                    ocr_mode_val = docling_init_kwargs.pop("ocr_mode")
                    docling_init_kwargs["ocr"] = ocr_mode_val.lower() in ("on", "auto")
                elif "ocr" not in docling_init_kwargs:
                    docling_init_kwargs["ocr"] = True  # Default to OCR enabled
                if "timeout_seconds" not in docling_init_kwargs:
                    docling_init_kwargs["timeout_seconds"] = 240
                parser = DoclingParser(**docling_init_kwargs)
                ocr_enabled = docling_init_kwargs.get("ocr", True)
                print(f"[Worker {worker_id}] Initialized Docling parser with OCR: {ocr_enabled}")
            else:
                from src.parsers.pdfplumber_parser import PDFPlumberParser
                parser = PDFPlumberParser()
                print(f"[Worker {worker_id}] Docling unavailable, using PDFPlumber")
        except ImportError:
            from src.parsers.pdfplumber_parser import PDFPlumberParser
            parser = PDFPlumberParser()
            print(f"[Worker {worker_id}] Docling import failed, using PDFPlumber")
    elif parser_backend == "mistral":
        try:
            from src.parsers.mistral_parser import MistralParser, is_mistral_available
            if is_mistral_available():
                parser = MistralParser()
                print(f"[Worker {worker_id}] Initialized Mistral OCR parser")
            else:
                from src.parsers.pdfplumber_parser import PDFPlumberParser
                parser = PDFPlumberParser()
                print(f"[Worker {worker_id}] Mistral unavailable, using PDFPlumber")
        except ImportError:
            from src.parsers.pdfplumber_parser import PDFPlumberParser
            parser = PDFPlumberParser()
            print(f"[Worker {worker_id}] Mistral import failed, using PDFPlumber")
    else:
        from src.parsers.pdfplumber_parser import PDFPlumberParser
        parser = PDFPlumberParser()
        print(f"[Worker {worker_id}] Initialized PDFPlumber parser")
    
    return {
        "worker_id": worker_id,
        "parser": parser,
        "parser_backend": parser_backend,
        "chunker": SemanticChunker(max_chunk_size=DEFAULT_MAX_CHUNK_SIZE, overlap_size=DEFAULT_CHUNK_OVERLAP),
        "converter": DocumentConverter(),
        "pinecone": PineconeDocumentClient(
            api_key=pinecone_api_key,
            index_name=pinecone_index
        ),
        "filesystem": LocalFilesystemClient(
            source_path,
            openai_api_key=openai_api_key
        ),
        "redaction_service": redaction_service,  # PII redaction service
        "stats": WorkerStats(worker_id=worker_id)
    }


def build_metadata_dict(doc_data: DocumentData) -> Dict[str, Any]:
    """
    Build metadata dict from discovery document structure.
    
    Maps discovery JSON fields to the 22-field Pinecone schema.
    """
    file_info = doc_data.get("file_info", {})
    deal_meta = doc_data.get("deal_metadata", {})
    business_meta = doc_data.get("business_metadata", {})
    
    return {
        # Core document
        "name": file_info.get("name", "")[:200],  # Truncate to 200 chars to prevent size limit errors
        "file_type": file_info.get("file_type", ""),
        "path": file_info.get("path", ""),
        
        # Identifiers
        "deal_id": deal_meta.get("deal_id", ""),
        "salesforce_deal_id": deal_meta.get("salesforce_deal_id", ""),
        "salesforce_client_id": deal_meta.get("salesforce_client_id", ""),
        "salesforce_vendor_id": deal_meta.get("salesforce_vendor_id", ""),
        
        # Business context
        "client_name": deal_meta.get("client_name", ""),
        "vendor_name": deal_meta.get("vendor_name", ""),
        "deal_creation_date": business_meta.get("deal_creation_date") or deal_meta.get("deal_creation_date", ""),
        "deal_status": deal_meta.get("deal_status", ""),
        
        # Contract
        "contract_start": deal_meta.get("contract_start", ""),
        "contract_end": deal_meta.get("contract_end", ""),
        "contract_term": deal_meta.get("contract_term", ""),
        
        # Financial
        "final_amount": deal_meta.get("final_amount"),
        "savings_1yr": deal_meta.get("savings_1yr"),
        "savings_3yr": deal_meta.get("savings_3yr"),
        "savings_achieved": deal_meta.get("savings_achieved", ""),
        "fixed_savings": deal_meta.get("fixed_savings"),
        "savings_target_full_term": deal_meta.get("savings_target_full_term"),
        
        # Deal classification fields (added December 2025)
        "report_type": deal_meta.get("report_type", ""),
        "project_type": deal_meta.get("project_type", ""),
        "competition": deal_meta.get("competition", ""),
        "npi_analyst": deal_meta.get("npi_analyst", ""),
        "dual_multi_sourcing": deal_meta.get("dual_multi_sourcing", ""),
        "time_pressure": deal_meta.get("time_pressure", ""),
        "advisor_network_used": deal_meta.get("advisor_network_used", ""),
    }


def redact_metadata_fields(
    metadata_dict: Dict[str, Any],
    client_registry: Optional[Any],
    salesforce_client_id: Optional[str]
) -> Dict[str, Any]:
    """
    Redact client-identifying fields from metadata dict.
    
    When redaction is enabled, replaces:
    - client_name → <<CLIENT>>
    - file_name → replace client name occurrences with <<CLIENT>>
    
    Args:
        metadata_dict: Metadata dictionary to redact
        client_registry: ClientRegistry instance (from redaction service)
        salesforce_client_id: Salesforce client ID for registry lookup
        
    Returns:
        Metadata dict with client names redacted
    """
    if not client_registry or not salesforce_client_id:
        return metadata_dict
    
    # Get client info from registry
    client_info = client_registry.get_client_info(salesforce_client_id)
    if not client_info:
        return metadata_dict
    
    result = metadata_dict.copy()
    
    # Redact client_name field
    result["client_name"] = "<<CLIENT>>"
    
    # Redact client name in file_name
    file_name = result.get("name", "")  # Note: metadata_dict uses "name" not "file_name"
    if file_name:
        client_name = client_info.get("client_name", "")
        if client_name:
            # Use registry patterns for consistent replacement
            redacted_file_name, replacement_count = client_registry.replace_client_names(
                file_name, salesforce_client_id
            )
            # Replace the full placeholder with shorter form for filenames
            replacement_token = client_registry.get_replacement_token(salesforce_client_id)
            if replacement_token and replacement_token in redacted_file_name:
                redacted_file_name = redacted_file_name.replace(
                    replacement_token,
                    "<<CLIENT>>"
                )
            # Only update if a replacement was actually made
            if replacement_count > 0:
                result["name"] = redacted_file_name
    
    return result


def _timeout_context(seconds: int):
    """
    Context manager for a Unix-only timeout using SIGALRM.

    NOTE: This only works reliably in the main thread of a given process and is
    not supported on Windows. In this pipeline, each worker is a dedicated process
    so this guard helps prevent a single stage (e.g., redaction) from wedging a worker.
    """
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        import os
        import signal

        if seconds <= 0 or os.name == "nt":
            yield
            return

        def handler(signum, frame):  # pragma: no cover - signal path
            raise TimeoutError(f"Stage timed out after {seconds} seconds")

        old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(int(seconds))
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    return _cm()


def process_single_document(
    doc_data: DocumentData,
    worker_ctx: Dict[str, Any],
    namespace: str
) -> ProcessingResult:
    """
    Process a single document using worker-local resources.
    
    This function mirrors DocumentProcessor.process_document() but uses
    the worker's pre-initialized resources for process safety.
    
    Steps:
    1. Download file content
    2. Convert to processable format (PDF/text)
    3. Parse with Docling/PDFPlumber
    4. Chunk with SemanticChunker
    5. Generate embeddings via Pinecone
    6. Upsert to Pinecone
    
    Returns:
        ProcessingResult with success status, chunks created, timing, errors
    """
    start_time = time.time()
    file_path = doc_data.get("file_info", {}).get("path", "unknown")
    
    # Get file_type for tracking which parser was used
    file_info = doc_data.get("file_info", {})
    file_type_raw = file_info.get("file_type", "")
    
    # Fallback: extract from filename if file_type is missing
    if not file_type_raw:
        file_name = file_info.get("name", "")
        if file_name:
            import os
            _, ext = os.path.splitext(file_name)
            file_type_raw = ext.lower() if ext else ""
    
    # Normalize empty strings to .unknown for statistics tracking
    file_type = file_type_raw.lower() if file_type_raw else ".unknown"
    
    result: ProcessingResult = {
        "success": False,
        "document_path": file_path,
        "file_type": file_type,  # Track file type for parser selection logging
        "chunks_created": 0,
        "processing_time": 0.0,
        "errors": []
    }
    
    try:
        parser = worker_ctx["parser"]
        chunker = worker_ctx["chunker"]
        converter = worker_ctx["converter"]
        pinecone = worker_ctx["pinecone"]
        filesystem = worker_ctx["filesystem"]
        worker_id = worker_ctx.get("worker_id")
        
        # Extract file name (file_info and file_type already extracted above)
        file_name = file_info.get("name", "")
        
        # Step 1: Check if we can process this file type
        # Pass file_name for extension detection when file_path lacks extension (Salesforce exports)
        if not converter.can_process(file_path, file_name):
            result["errors"].append(f"Unsupported file type: {file_type}")
            return result
        
        # Step 2: Download content
        content = filesystem.download_file(file_path)
        if not content:
            result["errors"].append(f"Failed to download: {file_path}")
            return result
        
        # Step 3: Convert to processable format
        # Pass file_name for extension detection when file_path lacks extension (Salesforce exports)
        processed_content, content_type = converter.convert_to_processable_content(
            file_path, content, file_name
        )
        
        # Step 4: Parse
        metadata_dict = build_metadata_dict(doc_data)
        parsed = parser.parse(processed_content, metadata_dict, content_type)
        
        if not parsed.text or len(parsed.text.strip()) == 0:
            result["errors"].append("No text extracted from document")
            return result
        
        # Step 4.5: PII Redaction (before chunking)
        text_to_chunk = parsed.text
        redaction_service = worker_ctx.get("redaction_service")
        if redaction_service:
            try:
                from src.redaction.redaction_context import RedactionContext
                
                # Extract client/vendor info from metadata_dict
                redaction_context = RedactionContext(
                    salesforce_client_id=metadata_dict.get("salesforce_client_id"),
                    client_name=metadata_dict.get("client_name"),
                    industry_label=None,  # Will be looked up from registry
                    vendor_name=metadata_dict.get("vendor_name"),
                    file_type=file_type,
                    document_type=metadata_dict.get("document_type")
                )
                
                redaction_start = time.time()
                if worker_id is not None:
                    print(
                        f"[Worker {worker_id}] 🛡️ Redaction start: name='{file_name}' "
                        f"type={file_type} text_len={len(text_to_chunk)}",
                        flush=True,
                    )

                with _timeout_context(REDACTION_TIMEOUT_SECONDS):
                    redaction_result = redaction_service.redact(text_to_chunk, redaction_context)

                redaction_elapsed = time.time() - redaction_start
                if worker_id is not None:
                    total_repl = None
                    try:
                        total_repl_attr = getattr(redaction_result, "total_replacements", None)
                        total_repl = total_repl_attr() if callable(total_repl_attr) else total_repl_attr
                    except Exception:
                        total_repl = None
                    print(
                        f"[Worker {worker_id}] 🛡️ Redaction done: name='{file_name}' "
                        f"elapsed={redaction_elapsed:.2f}s "
                        f"replacements={total_repl}",
                        flush=True,
                    )
                
                if redaction_result.has_errors() or redaction_result.has_validation_failures():
                    error_msg = f"Redaction failed: {redaction_result.errors + redaction_result.validation_failures}"
                    result["errors"].append(error_msg)
                    return result
                
                text_to_chunk = redaction_result.redacted_text
                
                # Also redact metadata fields (client_name and file_name)
                metadata_dict = redact_metadata_fields(
                    metadata_dict,
                    redaction_service.client_registry,
                    metadata_dict.get("salesforce_client_id")
                )
            except Exception as e:
                error_msg = f"Redaction error: {str(e)}"
                result["errors"].append(error_msg)
                return result
        
        # Step 5: Chunk
        chunk_start = time.time()
        if worker_id is not None:
            print(
                f"[Worker {worker_id}] ✂️ Chunking start: name='{file_name}' type={file_type} text_len={len(text_to_chunk)}",
                flush=True,
            )

        spreadsheet_types = (".xlsx", ".xls", ".csv")
        chunk_timeout = (
            CHUNKING_TIMEOUT_SECONDS_SPREADSHEETS
            if (file_type or "").lower().endswith(spreadsheet_types)
            else CHUNKING_TIMEOUT_SECONDS
        )

        with _timeout_context(chunk_timeout):
            chunks = chunker.chunk_document(
                text_to_chunk,
                {
                    "document_name": file_name,
                    "file_type": file_type,
                    "document_path": file_path,
                },
            )

        chunk_elapsed = time.time() - chunk_start
        if worker_id is not None:
            print(
                f"[Worker {worker_id}] ✂️ Chunking done: name='{file_name}' chunks={len(chunks) if chunks else 0} elapsed={chunk_elapsed:.2f}s",
                flush=True,
            )
        
        # Fallback for short docs
        if not chunks and text_to_chunk:
            from src.chunking.semantic_chunker import Chunk
            bounded_text = text_to_chunk[:4000]
            chunks = [Chunk(
                text=bounded_text,
                metadata={
                    "document_name": file_name,
                    "chunk_index": 0,
                    "section_name": "content",
                    "chunk_type": "general"
                },
                start_index=0,
                end_index=len(bounded_text)
            )]
        
        if not chunks:
            result["errors"].append("No chunks created from document")
            return result
        
        # Step 6: Generate embeddings
        chunk_texts = [c.text for c in chunks]
        embeddings = pinecone._generate_embeddings(chunk_texts)
        
        # Step 7: Prepare for upsert with text field in metadata (truncated to 37KB)
        embedded_chunks = []
        for i, chunk in enumerate(chunks):
            # Truncate text to 37KB for metadata (Pinecone has 40KB limit)
            chunk_text = chunk.text
            truncated_text = _truncate_text_for_metadata(chunk_text, max_bytes=37 * 1024)
            
            chunk_meta = {
                **metadata_dict,
                "chunk_index": i,
                "text": truncated_text,  # Truncated text field (37KB max) for metadata searchability
            }
            embedded_chunks.append({
                "id": f"{file_path}_{i}",
                "text": chunk.text,
                "dense_embedding": embeddings["dense_embeddings"][i],
                "sparse_embedding": embeddings["sparse_embeddings"][i],
                "metadata": chunk_meta
            })
        
        # Step 8: Upsert to Pinecone
        success = pinecone.upsert_chunks(embedded_chunks, namespace)
        
        if success:
            result["success"] = True
            result["chunks_created"] = len(chunks)
            
            # Extract docling OCR decision metadata from parsed content
            if parsed.metadata:
                docling_metadata = {}
                for key in [
                    "docling_ocr_mode",
                    "docling_ocr_used",
                    "docling_text_chars",
                    "docling_word_count",
                    "docling_alnum_ratio",
                    "docling_table_count",
                ]:
                    if key in parsed.metadata:
                        docling_metadata[key] = parsed.metadata[key]
                if docling_metadata:
                    result["docling_metadata"] = docling_metadata
        else:
            result["errors"].append("Pinecone upsert failed")
        
    except Exception as e:
        result["errors"].append(f"Processing error: {str(e)}")
    
    result["processing_time"] = time.time() - start_time
    return result


def worker_main(
    worker_id: int,
    document_queue: Queue,
    result_queue: Queue,
    stop_flag: Value,
    config: Dict[str, Any]
) -> None:
    """
    Worker process main loop.
    
    Lifecycle:
    1. Initialize resources (parser, pinecone, etc.) - ONCE at startup
    2. Loop: get document from queue, process, put result
    3. On stop_flag or poison pill (None): cleanup and exit
    
    CRITICAL: Signal handlers work here because this is a new process
    with its own main thread. This enables PDF timeout functionality.
    """
    worker_prefix = f"[Worker {worker_id}]"
    
    try:
        # Initialize redaction service if enabled
        redaction_service = None
        if config.get("enable_redaction") and config.get("client_redaction_csv"):
            try:
                from src.redaction.client_registry import ClientRegistry
                from src.redaction.llm_span_detector import LLMSpanDetector
                from src.redaction.redaction_service import RedactionService
                
                client_registry = ClientRegistry(config["client_redaction_csv"])
                llm_detector = LLMSpanDetector(
                    api_key=config["openai_api_key"],
                    model=config.get("redaction_model", "gpt-5-mini-2025-08-07")
                )
                redaction_service = RedactionService(
                    client_registry=client_registry,
                    llm_span_detector=llm_detector,
                    strict_mode=True
                )
                print(f"{worker_prefix} Initialized redaction service")
            except Exception as e:
                print(f"{worker_prefix} Failed to initialize redaction service: {e}")
        
        # Initialize worker-local resources
        ctx = worker_initializer(
            worker_id=worker_id,
            parser_backend=config["parser_backend"],
            pinecone_api_key=config["pinecone_api_key"],
            pinecone_index=config["pinecone_index"],
            openai_api_key=config["openai_api_key"],
            source_path=config["source_path"],
            docling_kwargs=config.get("docling_kwargs"),
            redaction_service=redaction_service
        )
        
        print(f"{worker_prefix} Ready for processing")
        
        namespace = config["namespace"]
        
        while not stop_flag.value:
            try:
                # Non-blocking get with timeout allows checking stop_flag
                doc_data = document_queue.get(timeout=WORKER_QUEUE_TIMEOUT_SECONDS)
                
                if doc_data is None:  # Poison pill - graceful shutdown
                    break
                
                # Process the document
                doc_name = doc_data.get("file_info", {}).get("name", "unknown")
                result = process_single_document(doc_data, ctx, namespace)
                
                # Update local stats
                if result["success"]:
                    ctx["stats"].documents_processed += 1
                    ctx["stats"].total_chunks += result.get("chunks_created", 0)
                else:
                    ctx["stats"].documents_failed += 1
                    # Keep limited errors for debugging
                    if len(ctx["stats"].errors) < 20:
                        ctx["stats"].errors.extend(result.get("errors", [])[:2])
                
                ctx["stats"].total_time += result.get("processing_time", 0)
                
                # Send result back to main process
                result_queue.put({
                    "worker_id": worker_id,
                    "document_path": result["document_path"],
                    "document_name": doc_name,
                    "file_type": result.get("file_type", ""),
                    "success": result["success"],
                    "chunks_created": result["chunks_created"],
                    "processing_time": result["processing_time"],
                    "errors": result["errors"]
                })
                
            except mp.queues.Empty:
                # No documents available, loop and check stop_flag
                continue
            except Exception as e:
                print(f"{worker_prefix} Error processing document: {e}")
                # Continue processing other documents
                continue
        
        # Send final stats before exiting
        result_queue.put({
            "worker_id": worker_id,
            "type": "final_stats",
            "stats": asdict(ctx["stats"])
        })
        
        print(f"{worker_prefix} Shutdown complete. "
              f"Processed: {ctx['stats'].documents_processed}, "
              f"Failed: {ctx['stats'].documents_failed}")
        
    except Exception as e:
        print(f"{worker_prefix} Fatal error: {e}")
        result_queue.put({
            "worker_id": worker_id,
            "type": "worker_error",
            "error": str(e)
        })


class ParallelDocumentProcessor:
    """
    Coordinates parallel document processing across multiple worker processes.
    
    Features:
    - Process-based parallelism (safe for signal handlers and GPU)
    - Progress tracking with ETA
    - Resume capability via discovery JSON
    - Graceful shutdown on Ctrl+C
    - Automatic progress saving
    
    Usage:
        processor = ParallelDocumentProcessor(
            discovery_file="discovery.json",
            workers=4,
            namespace="production",
            parser_backend="docling"
        )
        processor.run()
    """
    
    def __init__(
        self,
        discovery_file: str,
        workers: int = 4,
        namespace: str = "documents",
        parser_backend: str = "docling",
        resume: bool = True,
        limit: Optional[int] = None,
        filter_file_type: Optional[str] = None,
        exclude_file_type: Optional[str] = ".png",
        modified_after: Optional[str] = None,
        modified_before: Optional[str] = None,
        deal_created_after: Optional[str] = None,
        deal_created_before: Optional[str] = None,
        min_size_kb: Optional[float] = None,
        max_size_mb: Optional[float] = None,
        docling_kwargs: Optional[Dict[str, Any]] = None,
        client_redaction_csv: Optional[str] = None,
        redaction_model: Optional[str] = None,
        enable_redaction: bool = False,
    ):
        self.discovery_file = Path(discovery_file)
        self.workers = min(workers, mp.cpu_count())  # Don't exceed CPU count
        self.namespace = namespace
        self.parser_backend = parser_backend
        self.resume = resume
        self.limit = limit

        # Selection filters
        self.filter_file_type = filter_file_type
        self.exclude_file_type = exclude_file_type
        self.modified_after = modified_after
        self.modified_before = modified_before
        self.deal_created_after = deal_created_after
        self.deal_created_before = deal_created_before
        self.min_size_kb = min_size_kb
        self.max_size_mb = max_size_mb
        
        # Load environment
        from dotenv import load_dotenv
        load_dotenv()
        
        self.config = {
            "parser_backend": parser_backend,
            "pinecone_api_key": os.getenv("PINECONE_API_KEY"),
            "pinecone_index": os.getenv("PINECONE_INDEX_NAME", "business-documents"),
            "openai_api_key": os.getenv("OPENAI_API_KEY"),
            "namespace": namespace,
            "source_path": None,  # Set after loading discovery
            "docling_kwargs": docling_kwargs,
            "client_redaction_csv": client_redaction_csv,
            "redaction_model": redaction_model or "gpt-5-mini-2025-08-07",
            "enable_redaction": enable_redaction
        }
        
        # Validate configuration
        if not self.config["pinecone_api_key"]:
            raise ValueError("PINECONE_API_KEY environment variable not set")
        if not self.config["openai_api_key"]:
            raise ValueError("OPENAI_API_KEY environment variable not set")
        
        # Multiprocessing primitives
        self.document_queue: Optional[Queue] = None
        self.result_queue: Optional[Queue] = None
        self.stop_flag: Optional[Value] = None
        self.worker_processes: List[Process] = []
        
        # Progress tracking
        self.total_documents = 0
        self.processed_count = 0
        self.failed_count = 0
        self.total_chunks = 0
        self.start_time: Optional[float] = None
        
        # Enhanced statistics for performance analysis
        self.stats_by_file_type: Dict[str, Dict[str, Any]] = {}  # {".pdf": {"count": 0, "success": 0, "failed": 0, "total_time": 0.0, "chunks": 0}}
        self.error_categories: Dict[str, int] = {}  # {"timeout": 0, "download_failed": 0, "no_text": 0, ...}
        self.processing_times: List[float] = []  # All processing times for percentile analysis
        
        # Graceful shutdown
        self._original_sigint = None
        self._shutdown_requested = False
        self._persistence = None  # Will be set in _process_documents
    
    def run(self):
        """Main entry point - orchestrates parallel processing"""
        try:
            # Load and prepare documents
            documents = self._load_documents()
            if not documents:
                print("❌ No documents to process")
                return
            
            self.total_documents = len(documents)
            
            print(f"\n{'='*60}")
            print(f"📋 PARALLEL DOCUMENT PROCESSING")
            print(f"{'='*60}")
            print(f"📄 Documents to process: {self.total_documents}")
            print(f"👷 Workers: {self.workers}")
            print(f"🔧 Parser: {self.parser_backend}")
            print(f"📦 Namespace: {self.namespace}")
            print(f"🔄 Resume mode: {self.resume}")
            
            # Estimate time
            avg_time = 3.6  # seconds per doc (from benchmarks)
            parallel_time = (self.total_documents * avg_time) / self.workers
            print(f"⏱️  Estimated time: {parallel_time/3600:.1f} hours ({parallel_time/60:.0f} min)")
            print(f"{'='*60}\n")
            
            # Initialize multiprocessing primitives
            # Queue size prevents memory bloat from too many pending docs
            self.document_queue = mp.Queue(maxsize=self.workers * 20)
            self.result_queue = mp.Queue()
            self.stop_flag = mp.Value('b', False)
            
            # Setup signal handler for graceful shutdown
            self._setup_signal_handler()
            
            # Start workers
            self._start_workers()
            
            # Feed documents and collect results
            self.start_time = time.time()
            self._process_documents(documents)
            
            # Wait for completion and print summary
            self._finalize()
            
        except KeyboardInterrupt:
            print("\n\n⚠️ Interrupted by user (Ctrl+C)")
            self._graceful_shutdown()
        except Exception as e:
            print(f"\n❌ Fatal error: {e}")
            self._graceful_shutdown()
            raise
        finally:
            # Restore original signal handler
            if self._original_sigint:
                signal.signal(signal.SIGINT, self._original_sigint)
    
    def _load_documents(self) -> List[DocumentData]:
        """Load documents from discovery JSON, filtering already processed if resuming"""
        if not self.discovery_file.exists():
            raise FileNotFoundError(f"Discovery file not found: {self.discovery_file}")
        
        with open(self.discovery_file, 'r') as f:
            data = json.load(f)
        
        documents = data.get("documents", [])
        source_path = data.get("metadata", {}).get("source_path") or data.get("discovery_metadata", {}).get("source_path")
        
        # Fallback: extract source_path from first document if not in metadata
        if not source_path and documents:
            first_doc = documents[0]
            doc_path = first_doc.get("source_metadata", {}).get("source_path", "") or \
                       first_doc.get("file_info", {}).get("path", "")
            full_path = first_doc.get("source_metadata", {}).get("full_source_path", "")
            
            # For Salesforce exports, find the ContentVersions root
            if full_path:
                # Try to infer export root from full_source_path
                path_parts = Path(full_path).parts
                for i, part in enumerate(path_parts):
                    if part in ("ContentVersion", "ContentVersions"):
                        source_path = str(Path(*path_parts[:i]))
                        break
            elif "ContentVersions/" in doc_path or "ContentVersion/" in doc_path:
                # Can't infer reliably from a relative path without an explicit export root
                source_path = os.getenv("SALESFORCE_EXPORT_ROOT") or os.getenv("EXPORT_DIR")
            elif doc_path.startswith("/"):
                # Absolute path - use parent directory
                source_path = str(Path(doc_path).parent)
        
        self.config["source_path"] = source_path
        
        if not source_path:
            raise ValueError(
                "Discovery file missing source_path in metadata and could not be inferred. "
                "For Salesforce raw exports, ensure discovery used --export-root-dir so discovery_metadata.source_path is set, "
                "or set SALESFORCE_EXPORT_ROOT/EXPORT_DIR in the environment."
            )
        
        total_in_file = len(documents)
        
        if self.resume:
            # Filter out already processed documents
            documents = [
                doc for doc in documents
                if not doc.get("processing_status", {}).get("processed", False)
            ]
            already_done = total_in_file - len(documents)
            if already_done > 0:
                print(f"📂 Resume mode: {already_done} already processed, {len(documents)} remaining")

        # Apply selection filters (file type/date/size)
        from src.utils.discovery_persistence import DiscoveryPersistence
        include_types = None
        if self.filter_file_type:
            include_types = {x.strip() for x in str(self.filter_file_type).split(",") if x.strip()}
        exclude_types = {".png"}
        if self.exclude_file_type:
            exclude_types = {x.strip() for x in str(self.exclude_file_type).split(",") if x.strip()}

        filtered = DiscoveryPersistence.filter_documents(
            documents,
            include_processed=True,  # already handled above for resume; always include the docs we decided to process
            include_file_types=include_types,
            exclude_file_types=exclude_types,
            modified_after=self.modified_after,
            modified_before=self.modified_before,
            deal_created_after=self.deal_created_after,
            deal_created_before=self.deal_created_before,
            min_size_kb=self.min_size_kb,
            max_size_mb=self.max_size_mb,
        )
        documents = filtered["documents"]
        fstats = filtered["stats"]
        print(
            "🔎 Selection summary | "
            f"in={fstats.get('input_total')} out={fstats.get('output_total')} "
            f"excluded_type={fstats.get('excluded_file_type')} "
            f"excluded_date_invalid={fstats.get('excluded_modified_time_missing_or_invalid')} "
            f"excluded_after={fstats.get('excluded_modified_after')} "
            f"excluded_before={fstats.get('excluded_modified_before')} "
            f"excluded_deal_date_missing={fstats.get('excluded_deal_date_missing')} "
            f"excluded_deal_after={fstats.get('excluded_deal_created_after')} "
            f"excluded_deal_before={fstats.get('excluded_deal_created_before')} "
            f"excluded_min_size={fstats.get('excluded_min_size')} "
            f"excluded_max_size={fstats.get('excluded_max_size')}"
        )
        
        # Apply limit if specified
        if self.limit and len(documents) > self.limit:
            documents = documents[:self.limit]
            print(f"📂 Limited to {self.limit} documents")
        
        return documents
    
    def _start_workers(self):
        """Start worker processes"""
        print(f"🚀 Starting {self.workers} worker processes...")
        
        for i in range(self.workers):
            p = Process(
                target=worker_main,
                args=(
                    i,  # worker_id
                    self.document_queue,
                    self.result_queue,
                    self.stop_flag,
                    self.config
                ),
                daemon=False  # Don't auto-terminate on main exit
            )
            p.start()
            self.worker_processes.append(p)
        
        # Give workers time to initialize
        time.sleep(2)
        print(f"✅ All {self.workers} workers initialized\n")
    
    def _process_documents(self, documents: List[DocumentData]):
        """Feed documents to workers and collect results"""
        from src.utils.discovery_persistence import DiscoveryPersistence
        persistence = DiscoveryPersistence(str(self.discovery_file))
        self._persistence = persistence  # Store for graceful shutdown access
        
        total_docs = len(documents)
        feed_idx = 0
        results_received = 0
        
        print(f"📤 Feeding {total_docs} documents to workers...\n")
        
        while results_received < total_docs and not self._shutdown_requested:
            # Feed more documents if queue has space and we have more to send
            while feed_idx < total_docs and not self.document_queue.full():
                try:
                    self.document_queue.put_nowait(documents[feed_idx])
                    feed_idx += 1
                except:
                    break  # Queue full, will try again next loop
            
            # Collect results (with timeout to allow checking shutdown flag)
            try:
                result = self.result_queue.get(timeout=0.5)
                
                # Skip worker shutdown messages for now
                if result.get("type") in ("final_stats", "worker_error"):
                    continue
                
                results_received += 1
                
                # Update counters
                if result["success"]:
                    self.processed_count += 1
                    self.total_chunks += result["chunks_created"]
                else:
                    self.failed_count += 1
                
                # Track enhanced statistics
                file_type_raw = result.get("file_type", "")
                # Normalize empty strings to .unknown for statistics tracking
                file_type = file_type_raw.lower() if file_type_raw else ".unknown"
                proc_time = result.get("processing_time", 0.0)
                
                # Initialize file type stats if needed
                if file_type not in self.stats_by_file_type:
                    self.stats_by_file_type[file_type] = {
                        "count": 0, "success": 0, "failed": 0, 
                        "total_time": 0.0, "chunks": 0
                    }
                
                # Update file type stats
                self.stats_by_file_type[file_type]["count"] += 1
                self.stats_by_file_type[file_type]["total_time"] += proc_time
                if result["success"]:
                    self.stats_by_file_type[file_type]["success"] += 1
                    self.stats_by_file_type[file_type]["chunks"] += result.get("chunks_created", 0)
                else:
                    self.stats_by_file_type[file_type]["failed"] += 1
                    # Categorize errors
                    for err in result.get("errors", []):
                        err_lower = err.lower()
                        if "timeout" in err_lower:
                            self.error_categories["timeout"] = self.error_categories.get("timeout", 0) + 1
                        elif "download" in err_lower or "file not found" in err_lower:
                            self.error_categories["download_failed"] = self.error_categories.get("download_failed", 0) + 1
                        elif "no text" in err_lower or "empty" in err_lower:
                            self.error_categories["no_text_extracted"] = self.error_categories.get("no_text_extracted", 0) + 1
                        elif "unsupported" in err_lower:
                            self.error_categories["unsupported_type"] = self.error_categories.get("unsupported_type", 0) + 1
                        elif "pinecone" in err_lower or "upsert" in err_lower:
                            self.error_categories["pinecone_error"] = self.error_categories.get("pinecone_error", 0) + 1
                        else:
                            self.error_categories["other"] = self.error_categories.get("other", 0) + 1
                
                # Track processing times for percentile analysis
                self.processing_times.append(proc_time)
                
                # Update discovery JSON with processing status
                doc_path = result["document_path"]
                try:
                    # Determine content_parser based on file type
                    file_type = result.get("file_type", "").lower()
                    if file_type == ".pdf":
                        content_parser = self.parser_backend
                    elif file_type in [".xlsx", ".xls", ".csv"]:
                        content_parser = "pandas_openpyxl"
                    elif file_type == ".docx":
                        content_parser = "python_docx"
                    elif file_type == ".doc":
                        content_parser = "docx2txt"
                    elif file_type == ".msg":
                        content_parser = "extract_msg"
                    elif file_type == ".pptx":
                        content_parser = "python_pptx"
                    elif file_type in [".png", ".jpg", ".jpeg"]:
                        content_parser = f"image_to_pdf_{self.parser_backend}"
                    elif file_type == ".txt":
                        content_parser = "direct_text"
                    else:
                        content_parser = "unknown"
                    
                    processing_status = {
                        "processed": result["success"],
                        "processing_date": datetime.now().isoformat(),
                        "processor_version": "parallel_2.0",
                        "parser_backend": self.parser_backend,  # PDF parser selection
                        "content_parser": content_parser,  # Actual parser used for this file type
                        "chunks_created": result["chunks_created"],
                        "pinecone_namespace": self.namespace,
                        "processing_errors": result.get("errors", []),
                        "processing_time_seconds": result["processing_time"]
                    }
                    
                    # Add docling OCR decision metadata if available
                    if "docling_metadata" in result:
                        processing_status.update(result["docling_metadata"])
                    
                    persistence.update_document_metadata(doc_path, {
                        "processing_status": processing_status
                    })
                except Exception as e:
                    # Non-fatal: log but continue
                    pass
                
                # Progress display
                self._display_progress(results_received, total_docs, result)
                
            except mp.queues.Empty:
                # No results ready, continue loop
                continue
            except Exception as e:
                print(f"\n⚠️ Error collecting result: {e}")
                continue
        
        # Flush any pending updates to disk before shutdown
        try:
            persistence.flush_updates()
            print("💾 Flushed pending updates to disk")
        except Exception as e:
            print(f"⚠️ Warning: Could not flush updates: {e}")
        
        # Send poison pills to signal workers to stop
        print("\n📥 Signaling workers to finish...")
        for _ in range(self.workers):
            try:
                self.document_queue.put(None, timeout=WORKER_QUEUE_TIMEOUT_SECONDS)
            except:
                pass
    
    def _display_progress(self, completed: int, total: int, last_result: Dict):
        """Display progress bar with ETA"""
        elapsed = time.time() - self.start_time
        rate = completed / elapsed if elapsed > 0 else 0
        eta = (total - completed) / rate if rate > 0 else 0
        pct = 100 * completed / total
        
        # Status indicator
        status = "✅" if last_result["success"] else "❌"
        doc_name = last_result.get("document_name", "")[:30]
        
        # Build progress line
        bar_width = 20
        filled = int(bar_width * completed / total)
        bar = "█" * filled + "░" * (bar_width - filled)
        
        print(f"\r[{bar}] {pct:5.1f}% | "
              f"{completed}/{total} | "
              f"✅{self.processed_count} ❌{self.failed_count} | "
              f"🧩{self.total_chunks} | "
              f"ETA: {eta/60:.0f}m | "
              f"{status} {doc_name}", end="", flush=True)
    
    def _finalize(self):
        """Wait for workers and print summary"""
        print("\n\n🏁 Waiting for workers to finish...")
        
        # Collect final stats from workers
        worker_stats = []
        timeout_time = time.time() + 30  # 30 second timeout
        
        while len(worker_stats) < self.workers and time.time() < timeout_time:
            try:
                result = self.result_queue.get(timeout=WORKER_QUEUE_TIMEOUT_SECONDS)
                if result.get("type") == "final_stats":
                    worker_stats.append(result["stats"])
                    print(f"   Worker {result['stats']['worker_id']} reported final stats")
            except mp.queues.Empty:
                # Check if all workers have exited
                alive = sum(1 for p in self.worker_processes if p.is_alive())
                if alive == 0:
                    break
                continue
        
        # Join worker processes with timeout
        for p in self.worker_processes:
            p.join(timeout=5)
            if p.is_alive():
                print(f"⚠️ Force terminating stuck worker")
                p.terminate()
        
        # Calculate final metrics
        elapsed = time.time() - self.start_time
        
        print(f"\n{'='*60}")
        print(f"🎉 PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"📊 Documents processed: {self.processed_count}")
        print(f"❌ Documents failed: {self.failed_count}")
        print(f"🧩 Total chunks created: {self.total_chunks}")
        print(f"⏱️  Total time: {elapsed/60:.1f} minutes ({elapsed/3600:.2f} hours)")
        
        if self.processed_count > 0:
            print(f"📈 Throughput: {self.processed_count/elapsed*60:.1f} docs/minute")
            print(f"📈 Average: {elapsed/self.processed_count:.2f}s per document")
            print(f"📈 Average chunks: {self.total_chunks/self.processed_count:.1f} per document")
        
        # ===== FILE TYPE BREAKDOWN =====
        if self.stats_by_file_type:
            print(f"\n📁 FILE TYPE BREAKDOWN:")
            print(f"   {'Type':<10} {'Count':>6} {'Success':>8} {'Failed':>7} {'Avg Time':>10} {'Chunks':>8}")
            print(f"   {'-'*10} {'-'*6} {'-'*8} {'-'*7} {'-'*10} {'-'*8}")
            
            for ftype in sorted(self.stats_by_file_type.keys(), key=lambda x: self.stats_by_file_type[x]["count"], reverse=True):
                stats = self.stats_by_file_type[ftype]
                avg_time = stats["total_time"] / stats["count"] if stats["count"] > 0 else 0
                success_rate = 100 * stats["success"] / stats["count"] if stats["count"] > 0 else 0
                print(f"   {ftype:<10} {stats['count']:>6} {stats['success']:>8} {stats['failed']:>7} {avg_time:>9.2f}s {stats['chunks']:>8}")
        
        # ===== ERROR BREAKDOWN =====
        if self.error_categories:
            print(f"\n⚠️ ERROR BREAKDOWN:")
            for err_type, count in sorted(self.error_categories.items(), key=lambda x: x[1], reverse=True):
                print(f"   {err_type}: {count}")
        
        # ===== PROCESSING TIME PERCENTILES =====
        if self.processing_times:
            sorted_times = sorted(self.processing_times)
            n = len(sorted_times)
            p50 = sorted_times[int(n * 0.5)] if n > 0 else 0
            p90 = sorted_times[int(n * 0.9)] if n > 0 else 0
            p99 = sorted_times[int(n * 0.99)] if n > 0 else 0
            slowest = sorted_times[-1] if n > 0 else 0
            fastest = sorted_times[0] if n > 0 else 0
            
            print(f"\n⏱️  PROCESSING TIME DISTRIBUTION:")
            print(f"   Fastest:   {fastest:.2f}s")
            print(f"   P50 (med): {p50:.2f}s")
            print(f"   P90:       {p90:.2f}s")
            print(f"   P99:       {p99:.2f}s")
            print(f"   Slowest:   {slowest:.2f}s")
        
        # Worker breakdown
        if worker_stats:
            print(f"\n👷 Worker Statistics:")
            for ws in sorted(worker_stats, key=lambda x: x["worker_id"]):
                print(f"   Worker {ws['worker_id']}: "
                      f"✅{ws['documents_processed']} "
                      f"❌{ws['documents_failed']} "
                      f"🧩{ws['total_chunks']}")
        
        print(f"\n{'='*60}")
        
        # Report any errors
        if self.failed_count > 0:
            print(f"\n⚠️ {self.failed_count} documents failed. "
                  f"Check discovery JSON for error details.")
    
    def _setup_signal_handler(self):
        """Setup Ctrl+C handler for graceful shutdown"""
        self._original_sigint = signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C by requesting graceful shutdown"""
        if self._shutdown_requested:
            # Second Ctrl+C - force exit
            print("\n\n🛑 Force shutdown requested...")
            self._force_shutdown()
            sys.exit(1)
        
        print("\n\n⚠️ Shutdown requested (Ctrl+C again to force)...")
        print("   Finishing current documents...")
        self._shutdown_requested = True
        if self.stop_flag:
            self.stop_flag.value = True
    
    def _graceful_shutdown(self):
        """Cleanup on graceful shutdown"""
        self._shutdown_requested = True
        
        if self.stop_flag:
            self.stop_flag.value = True
        
        # Give workers time to finish current document
        print("💾 Saving progress...")
        time.sleep(3)
        
        # Flush any pending discovery updates BEFORE terminating
        if hasattr(self, '_persistence') and self._persistence:
            try:
                self._persistence.flush_updates()
                print("✅ Flushed pending updates to discovery JSON")
            except Exception as e:
                print(f"⚠️ Warning: Could not flush updates: {e}")
        
        # Send poison pills
        if self.document_queue:
            for _ in range(self.workers):
                try:
                    self.document_queue.put(None, timeout=0.5)
                except:
                    pass
        
        # Wait briefly for workers
        time.sleep(2)
        
        # Terminate any stuck workers
        for p in self.worker_processes:
            if p.is_alive():
                p.terminate()
        
        elapsed = time.time() - (self.start_time or time.time())
        print(f"\n📊 Partial results: {self.processed_count} processed, "
              f"{self.failed_count} failed, {self.total_chunks} chunks")
        print(f"💾 Progress saved to {self.discovery_file}")
        print(f"🔄 Run with --resume to continue from where you left off")
    
    def _force_shutdown(self):
        """Force terminate all workers"""
        for p in self.worker_processes:
            if p.is_alive():
                p.terminate()


def run_parallel_processing(
    discovery_file: str,
    workers: int = 4,
    namespace: str = "documents",
    parser_backend: str = "docling",
    resume: bool = True,
    limit: Optional[int] = None,
    # Selection filters (mirrors process_discovered_documents.py)
    filter_file_type: Optional[str] = None,
    exclude_file_type: Optional[str] = ".png",
    modified_after: Optional[str] = None,
    modified_before: Optional[str] = None,
    deal_created_after: Optional[str] = None,
    deal_created_before: Optional[str] = None,
    min_size_kb: Optional[float] = None,
    max_size_mb: Optional[float] = None,
    docling_kwargs: Optional[Dict[str, Any]] = None,
    client_redaction_csv: Optional[str] = None,
    redaction_model: Optional[str] = None,
    enable_redaction: bool = False,
) -> None:
    """
    Convenience function to run parallel processing.
    
    Args:
        discovery_file: Path to discovery JSON file
        workers: Number of parallel worker processes
        namespace: Pinecone namespace for storage
        parser_backend: Parser to use ("docling" or "pdfplumber")
        resume: Skip already-processed documents
        limit: Maximum documents to process (for testing)
        deal_created_after: Only process documents with deal_creation_date on/after this date (YYYY-MM-DD)
        deal_created_before: Only process documents with deal_creation_date on/before this date
        docling_kwargs: Optional dict of DoclingParser initialization kwargs
    """
    processor = ParallelDocumentProcessor(
        discovery_file=discovery_file,
        workers=workers,
        namespace=namespace,
        client_redaction_csv=client_redaction_csv,
        redaction_model=redaction_model,
        enable_redaction=enable_redaction,
        parser_backend=parser_backend,
        resume=resume,
        limit=limit,
        filter_file_type=filter_file_type,
        exclude_file_type=exclude_file_type,
        modified_after=modified_after,
        modified_before=modified_before,
        deal_created_after=deal_created_after,
        deal_created_before=deal_created_before,
        min_size_kb=min_size_kb,
        max_size_mb=max_size_mb,
        docling_kwargs=docling_kwargs,
    )
    processor.run()


# Allow direct execution for testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Parallel document processor")
    parser.add_argument("--input", required=True, help="Discovery JSON file")
    parser.add_argument("--workers", "-w", type=int, default=4, help="Number of workers")
    parser.add_argument("--namespace", default="documents", help="Pinecone namespace")
    parser.add_argument("--parser-backend", default="docling", choices=["docling", "pdfplumber"])
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.add_argument("--limit", type=int, help="Limit documents for testing")
    
    args = parser.parse_args()
    
    run_parallel_processing(
        discovery_file=args.input,
        workers=args.workers,
        namespace=args.namespace,
        parser_backend=args.parser_backend,
        resume=args.resume,
        limit=args.limit
    )

