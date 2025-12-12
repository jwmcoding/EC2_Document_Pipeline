"""
Enhanced Batch Document Processor with Progressive Discovery

This processor can handle very large Dropbox collections (2000+ files) by:
1. Resuming discovery from where it left off after interruptions
2. Processing new batches as they're discovered
3. Tracking progress against the full collection
4. Handling auth token refresh and error recovery
"""

import time
from typing import Dict, List, Optional, Any
from dataclasses import asdict

from pipeline.batch_document_processor import BatchDocumentProcessor
from utils.progressive_discovery import ProgressiveDiscovery
from connectors.dropbox_client import DropboxClient
from connectors.pinecone_client import PineconeDocumentClient
from config.colored_logging import ColoredLogger


class EnhancedBatchProcessor(BatchDocumentProcessor):
    """Enhanced batch processor with progressive discovery capability"""
    
    def __init__(self, dropbox_client: DropboxClient, pinecone_client: PineconeDocumentClient,
                 max_chunk_size: int = 1500, chunk_overlap: int = 200, 
                 batch_size: int = 50, enable_token_validation: bool = True, 
                 token_validation_interval: int = 300):
        super().__init__(
            dropbox_client, pinecone_client, 
            max_chunk_size, chunk_overlap, 
            enable_batch_processing=True, batch_size=batch_size,
            enable_token_validation=enable_token_validation,
            token_validation_interval=token_validation_interval
        )
        self.logger = ColoredLogger("enhanced_batch_processor")
        
    def discover_and_process_with_resume(self, folder_path: str, namespace: str = "documents",
                                       max_documents: Optional[int] = None,
                                       discovery_only: bool = False) -> Dict[str, Any]:
        """Discover and process documents with full resume capability
        
        This method can:
        1. Resume discovery from the last cursor position
        2. Continue processing new batches as they're discovered
        3. Handle interruptions gracefully
        4. Track progress against the full collection
        """
        
        start_time = time.time()
        
        # Initialize progressive discovery
        progressive_discovery = ProgressiveDiscovery(
            self.dropbox, folder_path, self.batch_size
        )
        
        # Get current progress
        progress_summary = progressive_discovery.get_progress_summary()
        
        self.logger.info(f"🚀 Starting enhanced discovery and processing...")
        self.logger.info(f"📊 Current progress: {progress_summary['total_discovered']} documents discovered")
        
        if progress_summary['can_resume']:
            self.logger.success(f"🔄 Can resume discovery from previous session")
        
        if progress_summary['estimated_total']:
            self.logger.info(f"📈 Estimated total: {progress_summary['estimated_total']} documents")
        
        results = {
            "folder_path": folder_path,
            "namespace": namespace,
            "enhanced_mode": True,
            "discovery_results": {
                "total_discovered": 0,
                "total_batches_created": 0,
                "discovery_time": 0.0,
                "resumed_from_cursor": progress_summary['can_resume']
            },
            "processing_results": {
                "total_processed": 0,
                "total_failed": 0,
                "processing_time": 0.0
            },
            "summary": {
                "total_time": 0.0,
                "discovery_complete": False,
                "processing_complete": False
            },
            "errors": []
        }
        
        try:
            # Phase 1: Discovery (with resume capability)
            discovery_start = time.time()
            
            self.logger.info("🔍 Phase 1: Progressive Discovery with Resume")
            
            discovery_count = 0
            for document in progressive_discovery.discover_with_resume(max_documents):
                discovery_count += 1
                
                # Log progress periodically
                if discovery_count % 100 == 0:
                    current_progress = progressive_discovery.get_progress_summary()
                    if current_progress['estimated_total']:
                        percentage = (current_progress['total_discovered'] / current_progress['estimated_total']) * 100
                        self.logger.info(f"📊 Discovery: {current_progress['total_discovered']}/{current_progress['estimated_total']} ({percentage:.1f}%)")
                    else:
                        self.logger.info(f"📊 Discovered: {current_progress['total_discovered']} documents")
            
            discovery_time = time.time() - discovery_start
            final_progress = progressive_discovery.get_progress_summary()
            
            results["discovery_results"].update({
                "total_discovered": final_progress.get('total_discovered', 0),
                "total_batches_created": final_progress.get('total_batches', 0),
                "discovery_time": discovery_time,
                "discovery_complete": final_progress.get('discovery_complete', False)
            })
            
            self.logger.info(f"✅ Discovery complete: {final_progress.get('total_discovered', 0)} documents in {final_progress.get('total_batches', 0)} batches")
            
            # Stop here if discovery-only mode
            if discovery_only:
                results["summary"]["discovery_complete"] = True
                results["summary"]["total_time"] = time.time() - start_time
                return results
            
            # Phase 2: Process all discovered batches
            self.logger.info("⚡ Phase 2: Batch Processing")
            processing_start = time.time()
            
            # Initialize batch processing components
            self._init_batch_components(folder_path)
            
            # Create proper results structure for batch processing
            batch_results_structure = {
                "summary": {
                    "batch_results": [],
                    "total_chunks": 0
                },
                "errors": []
            }
            
            # Process all batches (including newly discovered ones)
            processing_time = self._process_all_batches(namespace, batch_results_structure)
            
            # Merge any batch-level errors into main results
            if batch_results_structure["errors"]:
                results["errors"].extend(batch_results_structure["errors"])
            
            # Get final batch state
            batch_progress = self.batch_state.get_progress_summary()
            
            results["processing_results"].update({
                "total_processed": batch_progress.get('total_processed_docs', 0),
                "total_failed": batch_progress.get('total_failed_docs', 0),
                "processing_time": processing_time
            })
            
            # Mark processing complete if all batches are done
            completed_batches = batch_progress.get('completed_batches', 0)
            total_batches = batch_progress.get('total_batches', 0)
            
            # Reset processing complete flag if new batches were discovered
            if self.batch_state.processing_complete and completed_batches < total_batches:
                self.logger.info(f"🔄 New batches discovered: {completed_batches}/{total_batches} - resetting processing complete flag")
                self.batch_state.reset_processing_complete()
            
            if completed_batches >= total_batches:
                self.batch_state.mark_processing_complete()
                results["summary"]["processing_complete"] = True
                self.logger.success(f"🎉 All processing complete: {completed_batches}/{total_batches} batches")
            else:
                self.logger.info(f"⏳ Processing progress: {completed_batches}/{total_batches} batches")
            
        except Exception as e:
            error_msg = f"Enhanced processing error: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            results["errors"].append(error_msg)
            raise
        
        finally:
            # Calculate final results
            total_time = time.time() - start_time
            results["summary"]["total_time"] = total_time
            results["summary"]["discovery_complete"] = results["discovery_results"].get("discovery_complete", False)
            
            # Log final summary
            self.logger.info(f"📊 Final Summary:")
            self.logger.info(f"   🔍 Discovery: {results['discovery_results'].get('total_discovered', 0)} documents, "
                           f"{results['discovery_results'].get('total_batches_created', 0)} batches")
            self.logger.info(f"   ⚡ Processing: {results['processing_results'].get('total_processed', 0)} processed, "
                           f"{results['processing_results'].get('total_failed', 0)} failed")
            self.logger.info(f"   ⏱️ Total time: {total_time:.1f}s")
        
        return results
    
    def get_comprehensive_status(self, folder_path: str) -> Dict[str, Any]:
        """Get comprehensive status of both discovery and processing"""
        
        try:
            # Get discovery progress with error handling
            progressive_discovery = ProgressiveDiscovery(
                self.dropbox, folder_path, self.batch_size
            )
            discovery_progress = progressive_discovery.get_progress_summary()
            
            # Get batch processing progress with error handling
            batch_progress = self.get_batch_progress(folder_path)
            
            # Safely extract values with defaults
            discovery_complete = discovery_progress.get('discovery_complete', False) if discovery_progress else False
            processing_complete = False
            total_processed = 0
            can_resume_processing = False
            
            if batch_progress and 'batch_state' in batch_progress:
                batch_state = batch_progress['batch_state']
                processing_complete = batch_state.get('processing_complete', False)
                total_processed = batch_state.get('total_processed_docs', 0)
                can_resume_processing = batch_progress.get('can_resume', False)
                
                # Check for incorrect processing_complete flag
                completed_batches = batch_state.get('completed_batches', 0)
                total_batches = batch_state.get('total_batches', 0)
                if processing_complete and completed_batches < total_batches:
                    self.logger.info(f"🔄 Detected incorrect processing_complete flag: {completed_batches}/{total_batches} batches - resetting")
                    self.batch_state.reset_processing_complete()
                    processing_complete = False
            
            return {
                "discovery": discovery_progress or {},
                "processing": batch_progress,
                "overall_status": {
                    "discovery_complete": discovery_complete,
                    "processing_complete": processing_complete,
                    "can_resume_discovery": discovery_progress.get('can_resume', False) if discovery_progress else False,
                    "can_resume_processing": can_resume_processing,
                    "total_discovered": discovery_progress.get('total_discovered', 0) if discovery_progress else 0,
                    "total_processed": total_processed,
                    "estimated_total": discovery_progress.get('estimated_total') if discovery_progress else None,
                }
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error getting comprehensive status: {e}")
            # Return safe default structure
            return {
                "discovery": {},
                "processing": None,
                "overall_status": {
                    "discovery_complete": False,
                    "processing_complete": False,
                    "can_resume_discovery": False,
                    "can_resume_processing": False,
                    "total_discovered": 0,
                    "total_processed": 0,
                    "estimated_total": None,
                }
            }
    
    def resume_discovery_and_processing(self, folder_path: str, namespace: str = "documents") -> Dict[str, Any]:
        """Resume both discovery and processing from where they left off"""
        
        try:
            status = self.get_comprehensive_status(folder_path)
            
            self.logger.info("🔄 Resuming discovery and processing...")
            self.logger.info(f"📊 Current status:")
            
            # Safely access overall status with defaults
            overall_status = status.get('overall_status', {})
            total_discovered = overall_status.get('total_discovered', 0)
            total_processed = overall_status.get('total_processed', 0)
            discovery_complete = overall_status.get('discovery_complete', False)
            processing_complete = overall_status.get('processing_complete', False)
            
            self.logger.info(f"   🔍 Discovery: {total_discovered} documents")
            self.logger.info(f"   ⚡ Processing: {total_processed} documents")
            
            # Continue discovery if not complete
            if not discovery_complete:
                self.logger.info("🔍 Continuing discovery...")
                return self.discover_and_process_with_resume(folder_path, namespace)
            
            # Continue processing if discovery complete but processing not
            elif not processing_complete:
                self.logger.info("⚡ Discovery complete, continuing processing...")
                return self.discover_and_process_with_resume(folder_path, namespace, discovery_only=False)
            
            else:
                self.logger.success("✅ Both discovery and processing are complete!")
                return {
                    "status": "complete",
                    "message": "Both discovery and processing are already complete",
                    "summary": status
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error during resume: {e}")
            self.logger.info("🔄 Starting fresh discovery and processing...")
            # Fallback to fresh start if resume fails
            return self.discover_and_process_with_resume(folder_path, namespace)
    
    def clear_all_state(self, folder_path: str) -> None:
        """Clear all discovery and processing state (start completely fresh)"""
        
        # Clear progressive discovery state
        progressive_discovery = ProgressiveDiscovery(
            self.dropbox, folder_path, self.batch_size
        )
        progressive_discovery.clear_progress()
        
        # Clear batch processing state
        self.clear_batch_data(folder_path)
        
        self.logger.info("🗑️ Cleared all discovery and processing state") 