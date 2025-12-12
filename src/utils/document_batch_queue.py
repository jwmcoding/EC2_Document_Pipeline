"""
Document Batch Queue System

Manages batches of documents for coordinated discovery and processing.
Provides file-based storage for batches with resume capabilities.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Iterator
from dataclasses import asdict, is_dataclass
import logging


class DocumentBatchQueue:
    """Manages batches of documents for processing coordination"""
    
    def __init__(self, folder_path: str, cache_dir: str = "cache/batches"):
        self.folder_path = folder_path
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Create safe folder name for batch files
        safe_name = folder_path.replace('/', '_').replace(' ', '_').replace('\\', '_').lower()
        self.batch_prefix = f"batch_{safe_name}"
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📁 Document batch queue initialized for: {folder_path}")
    
    def _serialize_document(self, document: Any) -> Dict[str, Any]:
        """Serialize a document object to dictionary"""
        if is_dataclass(document):
            return asdict(document)
        elif hasattr(document, '__dict__'):
            return document.__dict__
        else:
            return document
    
    def save_batch(self, batch_id: int, documents: List[Any], 
                   metadata: Dict[str, Any] = None) -> bool:
        """Save a batch of documents to disk
        
        Args:
            batch_id: Unique identifier for this batch
            documents: List of DocumentMetadata objects
            metadata: Additional metadata about the batch
        """
        try:
            batch_file = self.cache_dir / f"{self.batch_prefix}_{batch_id:04d}.json"
            
            # Serialize documents
            serialized_docs = [self._serialize_document(doc) for doc in documents]
            
            batch_data = {
                "batch_id": batch_id,
                "folder_path": self.folder_path,
                "document_count": len(documents),
                "created_at": datetime.now().isoformat(),
                "processed": False,
                "failed": False,
                "metadata": metadata or {},
                "documents": serialized_docs,
                "version": "1.0"
            }
            
            with open(batch_file, 'w') as f:
                json.dump(batch_data, f, indent=2, default=str)
            
            self.logger.info(f"💾 Saved batch {batch_id}: {len(documents)} documents")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save batch {batch_id}: {e}")
            return False
    
    def load_batch(self, batch_id: int) -> Optional[Dict[str, Any]]:
        """Load a specific batch from disk"""
        try:
            batch_file = self.cache_dir / f"{self.batch_prefix}_{batch_id:04d}.json"
            
            if not batch_file.exists():
                self.logger.warning(f"⚠️ Batch file not found: {batch_file}")
                return None
            
            with open(batch_file, 'r') as f:
                batch_data = json.load(f)
            
            self.logger.debug(f"📂 Loaded batch {batch_id}: {batch_data.get('document_count', 0)} documents")
            return batch_data
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load batch {batch_id}: {e}")
            return None
    
    def get_available_batches(self) -> List[int]:
        """Get list of all available batch IDs"""
        batch_ids = []
        
        try:
            pattern = f"{self.batch_prefix}_*.json"
            for batch_file in self.cache_dir.glob(pattern):
                # Extract batch ID from filename
                filename = batch_file.stem
                batch_id_str = filename.split('_')[-1]
                try:
                    batch_id = int(batch_id_str)
                    batch_ids.append(batch_id)
                except ValueError:
                    self.logger.warning(f"⚠️ Invalid batch filename: {batch_file}")
            
            batch_ids.sort()
            self.logger.debug(f"📊 Found {len(batch_ids)} available batches")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to list batches: {e}")
        
        return batch_ids
    
    def get_unprocessed_batches(self) -> List[int]:
        """Get list of batches that haven't been processed yet"""
        unprocessed = []
        
        for batch_id in self.get_available_batches():
            batch_data = self.load_batch(batch_id)
            if batch_data and not batch_data.get('processed', False) and not batch_data.get('failed', False):
                unprocessed.append(batch_id)
        
        self.logger.debug(f"🔍 Found {len(unprocessed)} unprocessed batches")
        return unprocessed
    
    def get_next_unprocessed_batch(self) -> Optional[int]:
        """Get the next batch ID that needs processing"""
        unprocessed = self.get_unprocessed_batches()
        return unprocessed[0] if unprocessed else None
    
    def mark_batch_processed(self, batch_id: int, results: Dict[str, Any] = None) -> bool:
        """Mark a batch as successfully processed"""
        try:
            batch_data = self.load_batch(batch_id)
            if not batch_data:
                return False
            
            batch_data['processed'] = True
            batch_data['processed_at'] = datetime.now().isoformat()
            batch_data['processing_results'] = results or {}
            
            batch_file = self.cache_dir / f"{self.batch_prefix}_{batch_id:04d}.json"
            with open(batch_file, 'w') as f:
                json.dump(batch_data, f, indent=2, default=str)
            
            self.logger.info(f"✅ Marked batch {batch_id} as processed")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to mark batch {batch_id} as processed: {e}")
            return False
    
    def mark_batch_failed(self, batch_id: int, error: str) -> bool:
        """Mark a batch as failed"""
        try:
            batch_data = self.load_batch(batch_id)
            if not batch_data:
                return False
            
            batch_data['failed'] = True
            batch_data['failed_at'] = datetime.now().isoformat()
            batch_data['error'] = error
            
            batch_file = self.cache_dir / f"{self.batch_prefix}_{batch_id:04d}.json"
            with open(batch_file, 'w') as f:
                json.dump(batch_data, f, indent=2, default=str)
            
            self.logger.warning(f"⚠️ Marked batch {batch_id} as failed: {error}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to mark batch {batch_id} as failed: {e}")
            return False
    
    def get_batch_documents(self, batch_id: int) -> Optional[List[Dict[str, Any]]]:
        """Get the documents from a specific batch"""
        batch_data = self.load_batch(batch_id)
        if batch_data:
            return batch_data.get('documents', [])
        return None
    
    def iterate_batches(self, only_unprocessed: bool = True) -> Iterator[tuple[int, List[Dict[str, Any]]]]:
        """Iterate through batches, yielding (batch_id, documents) pairs"""
        if only_unprocessed:
            batch_ids = self.get_unprocessed_batches()
        else:
            batch_ids = self.get_available_batches()
        
        for batch_id in batch_ids:
            documents = self.get_batch_documents(batch_id)
            if documents is not None:
                yield batch_id, documents
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get comprehensive status of the batch queue"""
        all_batches = self.get_available_batches()
        unprocessed_batches = self.get_unprocessed_batches()
        
        processed_count = 0
        failed_count = 0
        total_documents = 0
        processed_documents = 0
        failed_documents = 0
        
        for batch_id in all_batches:
            batch_data = self.load_batch(batch_id)
            if batch_data:
                doc_count = batch_data.get('document_count', 0)
                total_documents += doc_count
                
                if batch_data.get('processed', False):
                    processed_count += 1
                    processed_documents += doc_count
                elif batch_data.get('failed', False):
                    failed_count += 1
                    failed_documents += doc_count
        
        progress_pct = 0
        if total_documents > 0:
            progress_pct = ((processed_documents + failed_documents) / total_documents) * 100
        
        return {
            "folder_path": self.folder_path,
            "total_batches": len(all_batches),
            "processed_batches": processed_count,
            "failed_batches": failed_count,
            "unprocessed_batches": len(unprocessed_batches),
            "total_documents": total_documents,
            "processed_documents": processed_documents,
            "failed_documents": failed_documents,
            "progress_percentage": round(progress_pct, 1),
            "next_batch_id": self.get_next_unprocessed_batch()
        }
    
    def clear_all_batches(self) -> bool:
        """Clear all batch files (useful for starting fresh)"""
        try:
            pattern = f"{self.batch_prefix}_*.json"
            deleted_count = 0
            
            for batch_file in self.cache_dir.glob(pattern):
                batch_file.unlink()
                deleted_count += 1
            
            self.logger.info(f"🗑️ Cleared {deleted_count} batch files for: {self.folder_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to clear batch files: {e}")
            return False
    
    def delete_batch(self, batch_id: int) -> bool:
        """Delete a specific batch file"""
        try:
            batch_file = self.cache_dir / f"{self.batch_prefix}_{batch_id:04d}.json"
            
            if batch_file.exists():
                batch_file.unlink()
                self.logger.info(f"🗑️ Deleted batch {batch_id}")
                return True
            else:
                self.logger.warning(f"⚠️ Batch file not found: {batch_file}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Failed to delete batch {batch_id}: {e}")
            return False
    
    def get_batch_summary(self, batch_id: int) -> Optional[Dict[str, Any]]:
        """Get summary information about a specific batch"""
        batch_data = self.load_batch(batch_id)
        if not batch_data:
            return None
        
        return {
            "batch_id": batch_id,
            "document_count": batch_data.get('document_count', 0),
            "created_at": batch_data.get('created_at'),
            "processed": batch_data.get('processed', False),
            "failed": batch_data.get('failed', False),
            "processed_at": batch_data.get('processed_at'),
            "failed_at": batch_data.get('failed_at'),
            "error": batch_data.get('error'),
            "metadata": batch_data.get('metadata', {})
        } 