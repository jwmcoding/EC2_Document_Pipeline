"""
Batch Document Processor

Extends the standard DocumentProcessor to support batch-based processing.
Processes documents in configurable batch sizes for better resource management
and fault tolerance while maintaining all existing functionality.
"""

import time
from typing import List, Dict, Any, Optional, Iterator
from dataclasses import asdict

# Import parent class and required components - using absolute imports
from pipeline.document_processor import DocumentProcessor
from connectors.dropbox_client import DropboxClient, DocumentMetadata
from connectors.pinecone_client import PineconeDocumentClient
from utils.batch_state import BatchState, BatchInfo
from utils.document_batch_queue import DocumentBatchQueue
from utils.token_validator import TokenValidatedDropboxClient
from config.colored_logging import ColoredLogger


class BatchDocumentProcessor(DocumentProcessor):
    """Enhanced document processor with batch processing capabilities"""
    
    def __init__(self, dropbox_client: DropboxClient, pinecone_client: PineconeDocumentClient,
                 max_chunk_size: int = 1500, chunk_overlap: int = 200, 
                 enable_discovery_cache: bool = True, cache_save_interval: int = 50,
                 batch_size: int = 50, enable_batch_processing: bool = True,
                 enable_token_validation: bool = True, token_validation_interval: int = 300):
        """Initialize batch processor extending the base DocumentProcessor
        
        Args:
            batch_size: Number of documents to process in each batch
            enable_batch_processing: Enable batch processing mode
            enable_token_validation: Enable automatic token validation during processing
            token_validation_interval: Seconds between token validations (default 5 minutes)
        """
        # Initialize parent class
        super().__init__(
            dropbox_client=dropbox_client,
            pinecone_client=pinecone_client,
            max_chunk_size=max_chunk_size,
            chunk_overlap=chunk_overlap,
            enable_discovery_cache=enable_discovery_cache,
            cache_save_interval=cache_save_interval
        )
        
        # Batch processing configuration
        self.batch_size = batch_size
        self.enable_batch_processing = enable_batch_processing
        
        # Token validation setup
        self.enable_token_validation = enable_token_validation
        if enable_token_validation:
            self.validated_dropbox = TokenValidatedDropboxClient(
                dropbox_client, 
                validation_interval=token_validation_interval
            )
        else:
            self.validated_dropbox = dropbox_client
        
        # Batch management components
        self.batch_state: Optional[BatchState] = None
        self.batch_queue: Optional[DocumentBatchQueue] = None
        
        # Update logger name
        self.logger = ColoredLogger("batch_processor")
        
        validation_status = "enabled" if enable_token_validation else "disabled"
        self.logger.info(f"🔄 Batch Document Processor initialized: "
                        f"batch_size={batch_size}, batch_mode={enable_batch_processing}, "
                        f"token_validation={validation_status}")
    
    def _init_batch_components(self, folder_path: str) -> None:
        """Initialize batch state and queue for the given folder"""
        self.batch_state = BatchState(folder_path, self.batch_size)
        self.batch_queue = DocumentBatchQueue(folder_path)
        self.logger.info(f"🔄 Batch components ready for: {folder_path}")
    
    def process_folder_in_batches(self, folder_path: str, namespace: str = "documents", 
                                 max_documents: int = None, file_types: List[str] = None,
                                 force_fresh_discovery: bool = False) -> Dict[str, Any]:
        """Process documents in batches with comprehensive state management
        
        This is the main batch processing method that:
        1. Discovers documents in batches
        2. Processes each batch individually  
        3. Maintains state for resume capability
        4. Provides detailed progress tracking
        
        Args:
            force_fresh_discovery: If True, ignore existing batch state and start fresh
        """
        
        if not self.enable_batch_processing:
            self.logger.info("🔄 Batch processing disabled, using standard processing")
            return super().process_folder(folder_path, namespace, max_documents, file_types)
        
        start_time = time.time()
        self._init_batch_components(folder_path)
        
        results = {
            "folder_path": folder_path,
            "namespace": namespace,
            "batch_processing": True,
            "batch_size": self.batch_size,
            "summary": {
                "total_batches": 0,
                "processed_batches": 0,
                "failed_batches": 0,
                "total_documents": 0,
                "processed_documents": 0,
                "failed_documents": 0,
                "total_chunks": 0,
                "processing_time": 0.0,
                "discovery_time": 0.0,
                "batch_results": []
            },
            "errors": []
        }
        
        try:
            # Check if we should resume from existing state
            if force_fresh_discovery or not self.batch_state.discovery_complete:
                self.logger.info("🔍 Starting fresh discovery in batches...")
                discovery_time = self._run_batch_discovery(folder_path, max_documents, file_types)
                results["summary"]["discovery_time"] = discovery_time
            else:
                self.logger.success(f"📂 Resuming from existing batch state: "
                                  f"{self.batch_state.total_batches} batches")
            
            # Update summary with discovery results
            results["summary"]["total_batches"] = self.batch_state.total_batches
            results["summary"]["total_documents"] = self.batch_state.total_documents
            
            # Process all batches
            batch_processing_time = self._process_all_batches(namespace, results)
            
            # Calculate final results
            processing_time = time.time() - start_time
            results["summary"]["processing_time"] = processing_time
            
            # Get final progress summary
            progress = self.batch_state.get_progress_summary()
            results["summary"].update({
                "processed_batches": progress["completed_batches"],
                "failed_batches": progress["failed_batches"],
                "processed_documents": progress["total_processed_docs"],
                "failed_documents": progress["total_failed_docs"]
            })
            
            # Mark processing as complete if all batches are done
            if (results["summary"]["processed_batches"] + results["summary"]["failed_batches"] >= 
                results["summary"]["total_batches"]):
                self.batch_state.mark_processing_complete()
                self.logger.success(f"🎉 All batch processing complete!")
            
            self.logger.info(f"📊 Batch processing summary: "
                           f"{results['summary']['processed_batches']}/{results['summary']['total_batches']} batches, "
                           f"{results['summary']['processed_documents']} documents processed, "
                           f"{results['summary']['total_chunks']} chunks created")
            
        except Exception as e:
            error_msg = f"Error in batch processing: {str(e)}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
        
        return results
    
    def _run_batch_discovery(self, folder_path: str, max_documents: int = None, 
                           file_types: List[str] = None) -> float:
        """Run discovery phase in batches, saving each batch as discovered"""
        discovery_start = time.time()
        
        try:
            current_batch = []
            batch_id = 1
            total_discovered = 0
            
            self.logger.info(f"🔍 Starting batch discovery: {self.batch_size} docs per batch")
            
            # Use validated discovery method with token checks
            if self.enable_token_validation:
                document_iterator = self.validated_dropbox.list_documents_with_validation(
                    folder_path, validation_interval_docs=25
                )
            else:
                document_iterator = self.dropbox.list_documents(folder_path)
            
            for doc_metadata in document_iterator:
                # Apply filters
                if file_types and doc_metadata.file_type not in file_types:
                    continue
                
                current_batch.append(doc_metadata)
                total_discovered += 1
                
                # Save batch when it reaches target size
                if len(current_batch) >= self.batch_size:
                    self._save_batch(batch_id, current_batch)
                    self.logger.info(f"🔷 Discovered batch {batch_id}: {len(current_batch)} documents")
                    current_batch = []
                    batch_id += 1
                
                # Check max documents limit
                if max_documents and total_discovered >= max_documents:
                    break
            
            # Save final partial batch if any documents remain
            if current_batch:
                self._save_batch(batch_id, current_batch)
                self.logger.info(f"🔷 Discovered final batch {batch_id}: {len(current_batch)} documents")
                batch_id += 1
            
            # Update batch state
            total_batches = batch_id - 1
            self.batch_state.update_discovery_progress(total_discovered, total_batches)
            self.batch_state.mark_discovery_complete()
            
            discovery_time = time.time() - discovery_start
            self.logger.success(f"✅ Discovery complete: {total_discovered} documents in "
                              f"{total_batches} batches ({discovery_time:.1f}s)")
            
            return discovery_time
            
        except Exception as e:
            error_msg = f"Error during batch discovery: {str(e)}"
            self.logger.error(error_msg)
            raise e
    
    def _save_batch(self, batch_id: int, documents: List[DocumentMetadata]) -> None:
        """Save a batch of documents to both batch state and batch queue"""
        # Save to batch queue for processing
        batch_metadata = {
            "discovery_method": "batch_discovery",
            "batch_size": len(documents),
            "file_types": list(set(doc.file_type for doc in documents))
        }
        
        self.batch_queue.save_batch(batch_id, documents, batch_metadata)
        
        # Create batch record in state
        self.batch_state.create_batch(batch_id, len(documents))
    
    def _process_all_batches(self, namespace: str, results: Dict[str, Any]) -> float:
        """Process all unprocessed batches"""
        processing_start = time.time()
        
        # Get unprocessed batches
        unprocessed_batches = self.batch_queue.get_unprocessed_batches()
        
        if not unprocessed_batches:
            self.logger.info("📂 No unprocessed batches found")
            return 0.0
        
        self.logger.info(f"🔄 Processing {len(unprocessed_batches)} unprocessed batches")
        
        for batch_id in unprocessed_batches:
            try:
                batch_result = self._process_single_batch(batch_id, namespace)
                results["summary"]["batch_results"].append(batch_result)
                
                # Update total chunks count
                results["summary"]["total_chunks"] += batch_result.get("chunks_created", 0)
                
                self.logger.info(f"✅ Batch {batch_id} complete: {batch_result['success_count']} success, "
                               f"{batch_result['failure_count']} failed")
                
            except Exception as e:
                error_msg = f"Failed to process batch {batch_id}: {str(e)}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                
                # Mark batch as failed
                self.batch_queue.mark_batch_failed(batch_id, error_msg)
                self.batch_state.mark_batch_failed(batch_id, error_msg)
        
        processing_time = time.time() - processing_start
        return processing_time
    
    def _process_single_batch(self, batch_id: int, namespace: str) -> Dict[str, Any]:
        """Process a single batch of documents"""
        batch_start = time.time()
        
        # Validate token before processing this batch
        if self.enable_token_validation:
            self.logger.info(f"🔍 Validating token before batch {batch_id}...")
            if not self.validated_dropbox.validate_before_batch(batch_id, self.batch_size):
                raise ValueError(f"Token validation failed for batch {batch_id}. Processing aborted.")
        
        # Load batch documents
        batch_documents_data = self.batch_queue.get_batch_documents(batch_id)
        if not batch_documents_data:
            raise ValueError(f"Batch {batch_id} not found or empty")
        
        # Convert back to DocumentMetadata objects
        documents = []
        for doc_data in batch_documents_data:
            doc = DocumentMetadata(**doc_data)
            documents.append(doc)
        
        self.logger.info(f"🔷 Processing batch {batch_id}: {len(documents)} documents")
        
        # Process each document in the batch
        batch_result = {
            "batch_id": batch_id,
            "document_count": len(documents),
            "success_count": 0,
            "failure_count": 0,
            "chunks_created": 0,
            "processing_time": 0.0,
            "document_results": [],
            "errors": []
        }
        
        for doc in documents:
            try:
                doc_result = self.process_document(doc, namespace)
                batch_result["document_results"].append(doc_result)
                
                if doc_result["success"]:
                    batch_result["success_count"] += 1
                    batch_result["chunks_created"] += doc_result["chunks_created"]
                else:
                    batch_result["failure_count"] += 1
                    batch_result["errors"].extend(doc_result["errors"])
                
            except Exception as e:
                error_msg = f"Error processing {doc.path}: {str(e)}"
                batch_result["failure_count"] += 1
                batch_result["errors"].append(error_msg)
                self.logger.error(error_msg)
        
        # Complete batch processing
        batch_result["processing_time"] = time.time() - batch_start
        
        # Update batch state and queue
        self.batch_queue.mark_batch_processed(batch_id, batch_result)
        self.batch_state.mark_batch_processed(
            batch_id, 
            batch_result["success_count"], 
            batch_result["failure_count"],
            batch_result["errors"]
        )
        
        return batch_result
    
    def get_batch_progress(self, folder_path: str) -> Optional[Dict[str, Any]]:
        """Get current batch processing progress for a folder"""
        if not self.enable_batch_processing:
            return None
        
        try:
            self._init_batch_components(folder_path)
            
            # Get progress from batch state
            progress = self.batch_state.get_progress_summary()
            
            # Get queue status
            queue_status = self.batch_queue.get_queue_status()
            
            # Combine information
            return {
                "batch_state": progress,
                "queue_status": queue_status,
                "can_resume": not progress["processing_complete"]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting batch progress: {e}")
            return None
    
    def resume_batch_processing(self, folder_path: str, namespace: str = "documents") -> Dict[str, Any]:
        """Resume batch processing from where it left off"""
        if not self.enable_batch_processing:
            return {"error": "Batch processing is disabled"}
        
        self.logger.info(f"🔄 Resuming batch processing for: {folder_path}")
        
        # Just call the main batch processing method - it will automatically resume
        return self.process_folder_in_batches(
            folder_path=folder_path, 
            namespace=namespace,
            force_fresh_discovery=False
        )
    
    def clear_batch_data(self, folder_path: str) -> bool:
        """Clear all batch data for a folder (useful for starting fresh)"""
        try:
            self._init_batch_components(folder_path)
            
            # Clear batch state
            state_cleared = self.batch_state.clear_state()
            
            # Clear batch queue
            queue_cleared = self.batch_queue.clear_all_batches()
            
            success = state_cleared and queue_cleared
            if success:
                self.logger.info(f"🗑️ Cleared all batch data for: {folder_path}")
            else:
                self.logger.warning(f"⚠️ Partial cleanup of batch data for: {folder_path}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Failed to clear batch data: {e}")
            return False
    
    # Override the original process_folder to optionally use batch processing
    def process_folder(self, folder_path: str, namespace: str = "documents", 
                      max_documents: int = None, file_types: List[str] = None,
                      use_discovery_cache: bool = True, use_batch_processing: bool = None) -> Dict[str, Any]:
        """Enhanced process_folder that can use either standard or batch processing
        
        Args:
            use_batch_processing: If True, use batch processing. If None, use class default.
        """
        
        # Determine which processing mode to use
        if use_batch_processing is None:
            use_batch_processing = self.enable_batch_processing
        
        if use_batch_processing:
            self.logger.info(f"🔄 Using batch processing mode (batch_size={self.batch_size})")
            return self.process_folder_in_batches(
                folder_path=folder_path,
                namespace=namespace, 
                max_documents=max_documents,
                file_types=file_types
            )
        else:
            self.logger.info("📄 Using standard processing mode")
            return super().process_folder(
                folder_path=folder_path,
                namespace=namespace,
                max_documents=max_documents, 
                file_types=file_types,
                use_discovery_cache=use_discovery_cache
            ) 