"""
Batch State Management for Document Processing Pipeline

Tracks the progress of batch-based document processing, allowing for
resume capability and fine-grained progress monitoring.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

from config.colored_logging import ColoredLogger


@dataclass
class BatchInfo:
    """Information about a single batch"""
    batch_id: int
    batch_size: int
    discovered_at: datetime
    processed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    document_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class BatchState:
    """Manages the state of batch processing for a folder"""
    
    def __init__(self, folder_path: str, batch_size: int = 50, cache_dir: str = "cache/batches"):
        self.folder_path = folder_path
        self.batch_size = batch_size
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Create safe filename from folder path
        safe_name = folder_path.replace('/', '_').replace(' ', '_').replace('\\', '_').lower()
        self.state_file = self.cache_dir / f"batch_state_{safe_name}.json"
        
        self.logger = ColoredLogger("batch_state")
        
        # State tracking
        self.total_documents: int = 0
        self.total_batches: int = 0
        self.current_batch: int = 0
        self.batches: Dict[int, BatchInfo] = {}
        self.discovery_complete: bool = False
        self.processing_complete: bool = False
        self.created_at: datetime = datetime.now()
        self.updated_at: datetime = datetime.now()
        
        # Load existing state if available
        self._load_state()
    
    def _load_state(self) -> bool:
        """Load batch state from disk if it exists"""
        try:
            if not self.state_file.exists():
                self.logger.info(f"📁 No existing batch state found for: {self.folder_path}")
                return False
            
            with open(self.state_file, 'r') as f:
                state_data = json.load(f)
            
            # Restore state
            self.total_documents = state_data.get('total_documents', 0)
            self.total_batches = state_data.get('total_batches', 0)
            self.current_batch = state_data.get('current_batch', 0)
            self.discovery_complete = state_data.get('discovery_complete', False)
            self.processing_complete = state_data.get('processing_complete', False)
            self.created_at = datetime.fromisoformat(state_data.get('created_at', datetime.now().isoformat()))
            self.updated_at = datetime.fromisoformat(state_data.get('updated_at', datetime.now().isoformat()))
            
            # Restore batch info
            batch_data = state_data.get('batches', {})
            for batch_id_str, batch_info in batch_data.items():
                batch_id = int(batch_id_str)
                
                # Convert datetime strings back to objects
                discovered_at = datetime.fromisoformat(batch_info['discovered_at'])
                processed_at = None
                if batch_info.get('processed_at'):
                    processed_at = datetime.fromisoformat(batch_info['processed_at'])
                failed_at = None
                if batch_info.get('failed_at'):
                    failed_at = datetime.fromisoformat(batch_info['failed_at'])
                
                self.batches[batch_id] = BatchInfo(
                    batch_id=batch_id,
                    batch_size=batch_info.get('batch_size', self.batch_size),
                    discovered_at=discovered_at,
                    processed_at=processed_at,
                    failed_at=failed_at,
                    document_count=batch_info.get('document_count', 0),
                    success_count=batch_info.get('success_count', 0),
                    failure_count=batch_info.get('failure_count', 0),
                    errors=batch_info.get('errors', [])
                )
            
            # Sync with actual batch files on disk to ensure accuracy
            self._sync_with_disk_files()
            
            self.logger.info(f"📊 Loaded batch state: {len(self.batches)} batches, "
                           f"batch {self.current_batch}/{self.total_batches}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load batch state: {e}")
            return False
    
    def _save_state(self) -> bool:
        """Save current batch state to disk"""
        try:
            # Convert to serializable format
            batches_data = {}
            for batch_id, batch_info in self.batches.items():
                batch_dict = asdict(batch_info)
                # Convert datetime objects to strings
                batch_dict['discovered_at'] = batch_info.discovered_at.isoformat()
                if batch_info.processed_at:
                    batch_dict['processed_at'] = batch_info.processed_at.isoformat()
                if batch_info.failed_at:
                    batch_dict['failed_at'] = batch_info.failed_at.isoformat()
                batches_data[str(batch_id)] = batch_dict
            
            state_data = {
                "folder_path": self.folder_path,
                "batch_size": self.batch_size,
                "total_documents": self.total_documents,
                "total_batches": self.total_batches,
                "current_batch": self.current_batch,
                "discovery_complete": self.discovery_complete,
                "processing_complete": self.processing_complete,
                "created_at": self.created_at.isoformat(),
                "updated_at": self.updated_at.isoformat(),
                "batches": batches_data,
                "version": "1.0"
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
            
            self.logger.debug(f"💾 Saved batch state: {len(self.batches)} batches")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save batch state: {e}")
            return False
    
    def create_batch(self, batch_id: int, document_count: int) -> BatchInfo:
        """Create a new batch record"""
        batch_info = BatchInfo(
            batch_id=batch_id,
            batch_size=self.batch_size,
            discovered_at=datetime.now(),
            document_count=document_count
        )
        
        self.batches[batch_id] = batch_info
        self.updated_at = datetime.now()
        self._save_state()
        
        self.logger.info(f"🔷 Created batch {batch_id}: {document_count} documents")
        return batch_info
    
    def mark_batch_processed(self, batch_id: int, success_count: int, failure_count: int, 
                           errors: List[str] = None) -> bool:
        """Mark a batch as completed"""
        if batch_id not in self.batches:
            self.logger.error(f"❌ Batch {batch_id} not found")
            return False
        
        batch_info = self.batches[batch_id]
        batch_info.processed_at = datetime.now()
        batch_info.success_count = success_count
        batch_info.failure_count = failure_count
        if errors:
            batch_info.errors.extend(errors)
        
        self.updated_at = datetime.now()
        self._save_state()
        
        self.logger.info(f"✅ Marked batch {batch_id} processed: {success_count} success, {failure_count} failed")
        return True
    
    def mark_batch_failed(self, batch_id: int, error: str) -> bool:
        """Mark a batch as failed"""
        if batch_id not in self.batches:
            self.logger.error(f"❌ Batch {batch_id} not found")
            return False
        
        batch_info = self.batches[batch_id]
        batch_info.failed_at = datetime.now()
        batch_info.errors.append(error)
        
        self.updated_at = datetime.now()
        self._save_state()
        
        self.logger.warning(f"⚠️ Marked batch {batch_id} as failed: {error}")
        return True
    
    def get_next_unprocessed_batch(self) -> Optional[int]:
        """Get the next batch that needs processing"""
        for batch_id in sorted(self.batches.keys()):
            batch_info = self.batches[batch_id]
            if batch_info.processed_at is None and batch_info.failed_at is None:
                return batch_id
        return None
    
    def get_completed_batches(self) -> List[int]:
        """Get list of successfully completed batch IDs"""
        completed = []
        for batch_id, batch_info in self.batches.items():
            if batch_info.processed_at is not None:
                completed.append(batch_id)
        return sorted(completed)
    
    def get_failed_batches(self) -> List[int]:
        """Get list of failed batch IDs"""
        failed = []
        for batch_id, batch_info in self.batches.items():
            if batch_info.failed_at is not None:
                failed.append(batch_id)
        return sorted(failed)
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """Get comprehensive progress summary"""
        completed_batches = self.get_completed_batches()
        failed_batches = self.get_failed_batches()
        
        total_processed_docs = sum(self.batches[bid].success_count for bid in completed_batches)
        total_failed_docs = sum(self.batches[bid].failure_count for bid in completed_batches)
        total_failed_docs += sum(self.batches[bid].document_count for bid in failed_batches)
        
        progress_pct = 0
        if self.total_documents > 0:
            progress_pct = ((total_processed_docs + total_failed_docs) / self.total_documents) * 100
        
        return {
            "folder_path": self.folder_path,
            "total_documents": self.total_documents,
            "total_batches": self.total_batches,
            "completed_batches": len(completed_batches),
            "failed_batches": len(failed_batches),
            "pending_batches": self.total_batches - len(completed_batches) - len(failed_batches),
            "total_processed_docs": total_processed_docs,
            "total_failed_docs": total_failed_docs,
            "progress_percentage": round(progress_pct, 1),
            "discovery_complete": self.discovery_complete,
            "processing_complete": self.processing_complete,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
    
    def update_discovery_progress(self, total_documents: int, total_batches: int) -> None:
        """Update discovery progress information"""
        self.total_documents = total_documents
        self.total_batches = total_batches
        self.updated_at = datetime.now()
        self._save_state()
    
    def mark_discovery_complete(self) -> None:
        """Mark discovery phase as complete"""
        self.discovery_complete = True
        self.updated_at = datetime.now()
        self._save_state()
        self.logger.info(f"🎯 Discovery complete: {self.total_documents} documents in {self.total_batches} batches")
    
    def mark_processing_complete(self) -> None:
        """Mark entire processing as complete"""
        self.processing_complete = True
        self.updated_at = datetime.now()
        self._save_state()
        self.logger.success(f"🎉 Processing complete for: {self.folder_path}")
    
    def reset_processing_complete(self) -> None:
        """Reset processing complete flag (useful when new batches are discovered)"""
        self.processing_complete = False
        self.updated_at = datetime.now()
        self._save_state()
        self.logger.info(f"🔄 Reset processing complete flag for: {self.folder_path}")
    
    def _sync_with_disk_files(self) -> None:
        """Sync batch state with actual batch files on disk"""
        try:
            # Create safe filename from folder path (same logic as __init__)
            safe_name = self.folder_path.replace('/', '_').replace(' ', '_').replace('\\', '_').lower()
            
            # Count actual batch files on disk
            actual_batch_files = list(self.cache_dir.glob(f"batch_{safe_name}_*.json"))
            # Filter out the state file itself
            actual_batch_files = [f for f in actual_batch_files if not f.name.startswith("batch_state_")]
            actual_batch_count = len(actual_batch_files)
            
            if actual_batch_count > self.total_batches:
                self.logger.info(f"🔄 Found {actual_batch_count} batch files on disk vs {self.total_batches} in state - syncing")
                
                # Update total_batches to match reality
                old_total_batches = self.total_batches
                self.total_batches = actual_batch_count
                
                # Count total documents from all batch files
                total_docs = 0
                for batch_file in actual_batch_files:
                    try:
                        with open(batch_file, 'r') as f:
                            batch_data = json.load(f)
                            total_docs += len(batch_data.get('documents', []))
                    except Exception as e:
                        self.logger.error(f"❌ Error reading batch file {batch_file}: {e}")
                
                # Update total_documents to match reality
                old_total_docs = self.total_documents
                self.total_documents = total_docs
                
                # Reset processing_complete since we have new unprocessed batches
                if self.processing_complete and actual_batch_count > old_total_batches:
                    self.logger.info(f"🔄 Resetting processing_complete: new batches found ({old_total_batches} → {actual_batch_count})")
                    self.processing_complete = False
                
                # Save the updated state
                self.updated_at = datetime.now()
                self._save_state()
                
                self.logger.info(f"✅ Synced batch state: {old_total_batches}→{self.total_batches} batches, {old_total_docs}→{self.total_documents} docs")
                
        except Exception as e:
            self.logger.error(f"❌ Error syncing with disk files: {e}")
    
    def clear_state(self) -> bool:
        """Clear all batch state (useful for starting fresh)"""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
            
            # Reset in-memory state
            self.total_documents = 0
            self.total_batches = 0
            self.current_batch = 0
            self.batches = {}
            self.discovery_complete = False
            self.processing_complete = False
            self.created_at = datetime.now()
            self.updated_at = datetime.now()
            
            self.logger.info(f"🗑️ Cleared batch state for: {self.folder_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to clear batch state: {e}")
            return False 