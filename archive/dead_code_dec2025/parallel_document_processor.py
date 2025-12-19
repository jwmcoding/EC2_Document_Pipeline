"""
Parallel Document Processor for Enhanced Performance
Multi-threaded document processing with configurable workers and comprehensive monitoring
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging
from datetime import datetime

from ..models.document_models import DocumentMetadata
from .document_processor import DocumentProcessor


@dataclass
class ParallelProcessingStats:
    """Statistics for parallel processing operations"""
    total_documents: int = 0
    completed_documents: int = 0
    failed_documents: int = 0
    total_chunks_created: int = 0
    total_processing_time: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    worker_count: int = 0
    
    def get_rate(self) -> float:
        """Calculate processing rate in documents per second"""
        if self.total_processing_time > 0:
            return self.completed_documents / self.total_processing_time
        return 0.0
    
    def get_success_rate(self) -> float:
        """Calculate success rate as percentage"""
        if self.total_documents > 0:
            return (self.completed_documents / self.total_documents) * 100
        return 0.0


class ThreadSafeLogger:
    """Thread-safe wrapper for logging with worker identification"""
    
    def __init__(self, base_logger: logging.Logger):
        self.base_logger = base_logger
        self._lock = threading.Lock()
    
    def _log_with_thread_id(self, level: str, message: str):
        """Log message with thread identification"""
        thread_name = threading.current_thread().name
        thread_id = thread_name.split('-')[-1] if '-' in thread_name else thread_name[-2:]
        
        with self._lock:
            getattr(self.base_logger, level)(f"[W{thread_id}] {message}")
    
    def info(self, message: str):
        self._log_with_thread_id('info', message)
    
    def error(self, message: str):
        self._log_with_thread_id('error', message)
    
    def warning(self, message: str):
        self._log_with_thread_id('warning', message)
    
    def debug(self, message: str):
        self._log_with_thread_id('debug', message)


class ParallelDocumentProcessor:
    """
    Multi-threaded document processing with configurable workers
    
    Features:
    - Configurable worker pool (default: 4 workers)
    - Individual error isolation (failed documents don't stop batch)
    - Real-time progress tracking with statistics
    - Thread-safe logging with worker identification
    - Graceful shutdown and resource cleanup
    """
    
    def __init__(self, 
                 max_workers: int = 4,
                 base_processor: Optional[DocumentProcessor] = None,
                 worker_memory_limit_mb: int = 512):
        """
        Initialize parallel document processor
        
        Args:
            max_workers: Maximum number of worker threads
            base_processor: Base DocumentProcessor instance (creates new if None)
            worker_memory_limit_mb: Memory limit per worker thread
        """
        self.max_workers = max_workers
        self.worker_memory_limit_mb = worker_memory_limit_mb
        self.base_processor = base_processor or DocumentProcessor()
        
        # Thread-safe logging
        base_logger = logging.getLogger(__name__)
        self.logger = ThreadSafeLogger(base_logger)
        
        # Processing statistics
        self.stats = ParallelProcessingStats()
        self.stats.worker_count = max_workers
        
        # Thread safety
        self._stats_lock = threading.Lock()
        self._results_lock = threading.Lock()
        
        self.logger.info(f"🚀 Parallel processor initialized with {max_workers} workers")
    
    def process_documents_parallel(self, 
                                 documents: List[DocumentMetadata], 
                                 namespace: str = "documents") -> Dict[str, Any]:
        """
        Process multiple documents in parallel with comprehensive error handling
        
        Args:
            documents: List of document metadata to process
            namespace: Pinecone namespace for uploads
            
        Returns:
            Dictionary with processing results and statistics
        """
        if not documents:
            self.logger.warning("No documents provided for parallel processing")
            return {"success": False, "error": "No documents to process"}
        
        # Initialize statistics
        self.stats = ParallelProcessingStats(
            total_documents=len(documents),
            start_time=datetime.now(),
            worker_count=self.max_workers
        )
        
        self.logger.info(f"📊 Starting parallel processing of {len(documents)} documents")
        self.logger.info(f"⚙️ Configuration: {self.max_workers} workers, namespace='{namespace}'")
        
        start_time = time.time()
        results = []
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers, 
                                  thread_name_prefix="DocWorker") as executor:
                
                # Submit all documents for processing
                future_to_doc = {
                    executor.submit(self._process_single_document_safe, doc, namespace): doc 
                    for doc in documents
                }
                
                # Process completed tasks as they finish
                for future in as_completed(future_to_doc):
                    doc = future_to_doc[future]
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # Update statistics
                        with self._stats_lock:
                            if result.get("success", False):
                                self.stats.completed_documents += 1
                                self.stats.total_chunks_created += result.get("chunks_created", 0)
                            else:
                                self.stats.failed_documents += 1
                        
                        # Log progress
                        progress = len(results)
                        if progress % 5 == 0 or result.get("success", False):  # Every 5 docs or on success
                            rate = progress / (time.time() - start_time) if time.time() > start_time else 0
                            success_count = self.stats.completed_documents
                            self.logger.info(f"📈 Progress: {progress}/{len(documents)} "
                                           f"({success_count} successful) | Rate: {rate:.2f} docs/sec")
                    
                    except Exception as e:
                        self.logger.error(f"❌ Failed to get result for {doc.name}: {e}")
                        results.append({
                            "success": False,
                            "document": doc.name,
                            "error": f"Future result error: {str(e)}"
                        })
                        
                        with self._stats_lock:
                            self.stats.failed_documents += 1
        
        except Exception as e:
            self.logger.error(f"💥 Critical error in parallel processing: {e}")
            return {
                "success": False,
                "error": f"Parallel processing failed: {str(e)}",
                "stats": asdict(self.stats)
            }
        
        # Finalize statistics
        end_time = time.time()
        self.stats.total_processing_time = end_time - start_time
        self.stats.end_time = datetime.now()
        
        # Calculate final metrics
        success_rate = self.stats.get_success_rate()
        processing_rate = self.stats.get_rate()
        
        # Log final results
        self.logger.info(f"🎉 Parallel processing complete!")
        self.logger.info(f"📊 Results: {self.stats.completed_documents}/{self.stats.total_documents} "
                        f"successful ({success_rate:.1f}%)")
        self.logger.info(f"⚡ Performance: {processing_rate:.2f} docs/sec, "
                        f"{self.stats.total_chunks_created} chunks created")
        self.logger.info(f"⏱️ Time: {self.stats.total_processing_time:.1f}s total")
        
        return {
            "success": True,
            "results": results,
            "stats": asdict(self.stats),
            "summary": {
                "total_documents": self.stats.total_documents,
                "successful_documents": self.stats.completed_documents,
                "failed_documents": self.stats.failed_documents,
                "success_rate_percent": success_rate,
                "processing_rate_docs_per_sec": processing_rate,
                "total_time_seconds": self.stats.total_processing_time,
                "total_chunks_created": self.stats.total_chunks_created,
                "worker_count": self.stats.worker_count
            }
        }
    
    def _process_single_document_safe(self, 
                                    doc_metadata: DocumentMetadata, 
                                    namespace: str) -> Dict[str, Any]:
        """
        Thread-safe wrapper for processing a single document
        
        Args:
            doc_metadata: Document metadata to process
            namespace: Pinecone namespace
            
        Returns:
            Processing result dictionary
        """
        thread_name = threading.current_thread().name
        worker_id = thread_name.split('-')[-1] if '-' in thread_name else thread_name[-2:]
        
        try:
            self.logger.debug(f"🔄 Starting document: {doc_metadata.name}")
            
            # Process document using base processor
            result = self.base_processor.process_document(doc_metadata, namespace)
            
            # Enhance result with worker information
            result.update({
                "worker_id": worker_id,
                "document": doc_metadata.name,
                "thread_name": thread_name
            })
            
            if result.get("success", False):
                self.logger.info(f"✅ Completed: {doc_metadata.name} "
                               f"({result.get('chunks_created', 0)} chunks)")
            else:
                self.logger.warning(f"⚠️ Failed: {doc_metadata.name} - "
                                  f"{result.get('errors', ['Unknown error'])[0]}")
            
            return result
            
        except Exception as e:
            error_msg = f"Worker {worker_id} error processing {doc_metadata.name}: {str(e)}"
            self.logger.error(f"💥 {error_msg}")
            
            return {
                "success": False,
                "document": doc_metadata.name,
                "worker_id": worker_id,
                "error": error_msg,
                "errors": [str(e)]
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        with self._stats_lock:
            return asdict(self.stats)
    
    def estimate_completion_time(self, documents_remaining: int) -> float:
        """
        Estimate completion time based on current processing rate
        
        Args:
            documents_remaining: Number of documents left to process
            
        Returns:
            Estimated seconds to completion
        """
        current_rate = self.stats.get_rate()
        if current_rate > 0:
            return documents_remaining / current_rate
        return 0.0 