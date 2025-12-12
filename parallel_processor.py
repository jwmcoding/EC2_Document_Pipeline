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
    source_path: str
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
                parser = DoclingParser(ocr=True, timeout_seconds=240)
                print(f"[Worker {worker_id}] Initialized Docling parser with OCR")
            else:
                from src.parsers.pdfplumber_parser import PDFPlumberParser
                parser = PDFPlumberParser()
                print(f"[Worker {worker_id}] Docling unavailable, using PDFPlumber")
        except ImportError:
            from src.parsers.pdfplumber_parser import PDFPlumberParser
            parser = PDFPlumberParser()
            print(f"[Worker {worker_id}] Docling import failed, using PDFPlumber")
    else:
        from src.parsers.pdfplumber_parser import PDFPlumberParser
        parser = PDFPlumberParser()
        print(f"[Worker {worker_id}] Initialized PDFPlumber parser")
    
    return {
        "worker_id": worker_id,
        "parser": parser,
        "parser_backend": parser_backend,
        "chunker": SemanticChunker(max_chunk_size=1500, overlap_size=200),
        "converter": DocumentConverter(),
        "pinecone": PineconeDocumentClient(
            api_key=pinecone_api_key,
            index_name=pinecone_index
        ),
        "filesystem": LocalFilesystemClient(
            source_path,
            openai_api_key=openai_api_key
        ),
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
        "description": deal_meta.get("description", ""),
        "project_type": deal_meta.get("project_type", ""),
        "competition": deal_meta.get("competition", ""),
        "npi_analyst": deal_meta.get("npi_analyst", ""),
        "dual_multi_sourcing": deal_meta.get("dual_multi_sourcing", ""),
        "time_pressure": deal_meta.get("time_pressure", ""),
        "advisor_network_used": deal_meta.get("advisor_network_used", ""),
    }


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
    
    result: ProcessingResult = {
        "success": False,
        "document_path": file_path,
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
        
        # Extract file info
        file_info = doc_data.get("file_info", {})
        file_name = file_info.get("name", "")
        file_type = file_info.get("file_type", "").lower()
        
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
        
        # Step 5: Chunk
        chunks = chunker.chunk_document(parsed.text, {
            "document_name": file_name,
            "file_type": file_type,
            "document_path": file_path
        })
        
        # Fallback for short docs
        if not chunks and parsed.text:
            from src.chunking.semantic_chunker import Chunk
            bounded_text = parsed.text[:4000]
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
        
        # Step 7: Prepare for upsert with 22-field schema
        embedded_chunks = []
        for i, chunk in enumerate(chunks):
            chunk_meta = {
                **metadata_dict,
                "chunk_index": i,
                # REMOVED: "text" field - stored at top level of Pinecone record, not in metadata
                # This prevents metadata size limit errors (40KB limit)
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
):
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
        # Initialize worker-local resources
        ctx = worker_initializer(
            worker_id=worker_id,
            parser_backend=config["parser_backend"],
            pinecone_api_key=config["pinecone_api_key"],
            pinecone_index=config["pinecone_index"],
            openai_api_key=config["openai_api_key"],
            source_path=config["source_path"]
        )
        
        print(f"{worker_prefix} Ready for processing")
        
        namespace = config["namespace"]
        
        while not stop_flag.value:
            try:
                # Non-blocking get with timeout allows checking stop_flag
                doc_data = document_queue.get(timeout=1.0)
                
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
        limit: Optional[int] = None
    ):
        self.discovery_file = Path(discovery_file)
        self.workers = min(workers, mp.cpu_count())  # Don't exceed CPU count
        self.namespace = namespace
        self.parser_backend = parser_backend
        self.resume = resume
        self.limit = limit
        
        # Load environment
        from dotenv import load_dotenv
        load_dotenv()
        
        self.config = {
            "parser_backend": parser_backend,
            "pinecone_api_key": os.getenv("PINECONE_API_KEY"),
            "pinecone_index": os.getenv("PINECONE_INDEX_NAME", "business-documents"),
            "openai_api_key": os.getenv("OPENAI_API_KEY"),
            "namespace": namespace,
            "source_path": None  # Set after loading discovery
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
        
        # Graceful shutdown
        self._original_sigint = None
        self._shutdown_requested = False
    
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
            
            # For Salesforce exports, find the ContentVersions root
            if "ContentVersions/" in doc_path:
                # Path like: ContentVersions/0690f00000xxxxx/filename.pdf
                # We need the parent of ContentVersions
                parts = doc_path.split("ContentVersions/")
                if len(parts) > 1:
                    # Default to known export location
                    source_path = "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771"
            elif doc_path.startswith("/"):
                # Absolute path - use parent directory
                from pathlib import Path
                source_path = str(Path(doc_path).parent)
        
        self.config["source_path"] = source_path
        
        if not source_path:
            raise ValueError(
                "Discovery file missing source_path in metadata and could not be inferred. "
                "Ensure the discovery was run with --source salesforce_raw or provide source_path."
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
                
                # Update discovery JSON with processing status
                doc_path = result["document_path"]
                try:
                    persistence.update_document_metadata(doc_path, {
                        "processing_status": {
                            "processed": result["success"],
                            "processing_date": datetime.now().isoformat(),
                            "processor_version": "parallel_2.0",
                            "chunks_created": result["chunks_created"],
                            "pinecone_namespace": self.namespace,
                            "processing_errors": result.get("errors", []),
                            "processing_time_seconds": result["processing_time"]
                        }
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
        
        # Send poison pills to signal workers to stop
        print("\n\n📥 Signaling workers to finish...")
        for _ in range(self.workers):
            try:
                self.document_queue.put(None, timeout=1.0)
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
                result = self.result_queue.get(timeout=1.0)
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
        
        # Worker breakdown
        if worker_stats:
            print(f"\n👷 Worker Statistics:")
            for ws in sorted(worker_stats, key=lambda x: x["worker_id"]):
                print(f"   Worker {ws['worker_id']}: "
                      f"✅{ws['documents_processed']} "
                      f"❌{ws['documents_failed']} "
                      f"🧩{ws['total_chunks']}")
        
        print(f"{'='*60}")
        
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
    limit: Optional[int] = None
):
    """
    Convenience function to run parallel processing.
    
    Args:
        discovery_file: Path to discovery JSON file
        workers: Number of parallel worker processes
        namespace: Pinecone namespace for storage
        parser_backend: Parser to use ("docling" or "pdfplumber")
        resume: Skip already-processed documents
        limit: Maximum documents to process (for testing)
    """
    processor = ParallelDocumentProcessor(
        discovery_file=discovery_file,
        workers=workers,
        namespace=namespace,
        parser_backend=parser_backend,
        resume=resume,
        limit=limit
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

