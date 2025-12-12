"""
Progressive Discovery System for Large Dropbox Collections

This module provides resumable discovery capability for large Dropbox folders,
allowing the system to resume discovery from where it left off after interruptions.

Key Features:
- Cursor-based discovery with state persistence
- Resume capability after interruptions
- Progress tracking against expected total
- Incremental batch creation during discovery
- Error handling and retry logic
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Generator, Tuple
from dataclasses import dataclass, asdict
import logging
import dropbox

from connectors.dropbox_client import DropboxClient, DocumentMetadata
from utils.batch_state import BatchState
from utils.document_batch_queue import DocumentBatchQueue
from config.colored_logging import ColoredLogger


@dataclass
class DiscoveryProgress:
    """Tracks progress of discovery across large folder structures"""
    folder_path: str
    current_cursor: Optional[str] = None
    total_discovered: int = 0
    total_batches_created: int = 0
    last_discovered_path: str = ""
    discovery_complete: bool = False
    estimated_total: Optional[int] = None
    started_at: str = ""
    last_updated: str = ""
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.started_at == "":
            self.started_at = datetime.now().isoformat()


class ProgressiveDiscovery:
    """Manages resumable discovery for large Dropbox collections"""
    
    def __init__(self, dropbox_client: DropboxClient, folder_path: str, 
                 batch_size: int = 50, cache_dir: str = "cache/discovery"):
        self.dropbox = dropbox_client
        self.folder_path = folder_path
        self.batch_size = batch_size
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Create safe filename
        safe_name = folder_path.replace('/', '_').replace(' ', '_').replace('\\', '_').lower()
        self.progress_file = self.cache_dir / f"progressive_{safe_name}.json"
        
        self.logger = ColoredLogger("progressive_discovery")
        
        # Initialize components
        self.batch_state = BatchState(folder_path, batch_size)
        self.batch_queue = DocumentBatchQueue(folder_path)
        
        # Load existing progress
        self.progress = self._load_progress()
        
    def _load_progress(self) -> DiscoveryProgress:
        """Load existing discovery progress from disk"""
        try:
            if not self.progress_file.exists():
                self.logger.info(f"📂 No existing discovery progress found")
                return DiscoveryProgress(folder_path=self.folder_path)
            
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
            
            progress = DiscoveryProgress(**data)
            self.logger.info(f"📂 Loaded discovery progress: {progress.total_discovered} documents")
            if progress.current_cursor:
                self.logger.info(f"🔄 Can resume from cursor: {progress.current_cursor[:20]}...")
            
            return progress
            
        except Exception as e:
            self.logger.error(f"❌ Error loading discovery progress: {e}")
            return DiscoveryProgress(folder_path=self.folder_path)
    
    def _save_progress(self) -> None:
        """Save current discovery progress to disk"""
        try:
            self.progress.last_updated = datetime.now().isoformat()
            
            with open(self.progress_file, 'w') as f:
                json.dump(asdict(self.progress), f, indent=2)
                
        except Exception as e:
            self.logger.error(f"❌ Error saving discovery progress: {e}")
    
    def _estimate_total_files(self) -> Optional[int]:
        """Estimate total files by doing a quick count (optional)"""
        try:
            # Quick estimation by counting first few pages
            self.logger.info("🔍 Estimating total file count...")
            
            count = 0
            result = self.dropbox.client.files_list_folder(self.folder_path, recursive=True)
            
            # Count first few pages to get estimate
            pages_to_sample = 3
            page_count = 0
            
            while page_count < pages_to_sample and result.has_more:
                count += len([e for e in result.entries if isinstance(e, dropbox.files.FileMetadata)])
                result = self.dropbox.client.files_list_folder_continue(result.cursor)
                page_count += 1
            
            # Rough estimation (this is just for progress tracking)
            if result.has_more:
                estimated = count * 10  # Rough multiplier
                self.logger.info(f"📊 Estimated ~{estimated} total files (based on {count} in first {page_count} pages)")
                return estimated
            else:
                self.logger.info(f"📊 Exact count: {count} total files")
                return count
                
        except Exception as e:
            self.logger.warning(f"⚠️ Could not estimate file count: {e}")
            return None
    
    def discover_with_resume(self, max_documents: Optional[int] = None, 
                           save_interval: int = 25) -> Generator[DocumentMetadata, None, None]:
        """Discover documents with resume capability
        
        Args:
            max_documents: Maximum documents to discover (for testing)
            save_interval: Save progress every N documents
        """
        
        # Estimate total if not already done
        if self.progress.estimated_total is None:
            self.progress.estimated_total = self._estimate_total_files()
        
        discovered_count = 0
        current_batch = []
        
        try:
            # Resume from cursor or start fresh
            if self.progress.current_cursor:
                self.logger.info(f"🔄 Resuming discovery from cursor...")
                result = self.dropbox.client.files_list_folder_continue(self.progress.current_cursor)
            else:
                self.logger.info(f"🔍 Starting fresh discovery...")
                result = self.dropbox.client.files_list_folder(self.folder_path, recursive=True)
            
            while True:
                for entry in result.entries:
                    if isinstance(entry, dropbox.files.FileMetadata):
                        # Parse metadata
                        metadata = self.dropbox.parse_document_path(
                            entry.path_display,
                            entry.size,
                            entry.server_modified.isoformat(),
                            entry.id,
                            getattr(entry, 'content_hash', None)
                        )
                        
                        current_batch.append(metadata)
                        discovered_count += 1
                        self.progress.total_discovered += 1
                        self.progress.last_discovered_path = entry.path_display
                        
                        # Save batch when full
                        if len(current_batch) >= self.batch_size:
                            self._save_current_batch(current_batch)
                            current_batch = []
                        
                        # Periodic progress save
                        if discovered_count % save_interval == 0:
                            self.progress.current_cursor = result.cursor
                            self._save_progress()
                            self._log_progress()
                        
                        yield metadata
                        
                        # Check max documents limit
                        if max_documents and self.progress.total_discovered >= max_documents:
                            self.logger.info(f"🛑 Reached max documents limit: {max_documents}")
                            self.progress.current_cursor = result.cursor
                            self._save_progress()
                            return
                
                # Check if more pages exist
                if not result.has_more:
                    self.logger.success(f"✅ Discovery complete: {self.progress.total_discovered} total documents")
                    self.progress.discovery_complete = True
                    self.progress.current_cursor = None
                    break
                
                # Continue to next page
                self.progress.current_cursor = result.cursor
                result = self.dropbox.client.files_list_folder_continue(result.cursor)
                
                # Save progress after each page
                self._save_progress()
            
            # Save final batch if any
            if current_batch:
                self._save_current_batch(current_batch)
            
            # Update batch state
            if not self.batch_state.discovery_complete:
                self.batch_state.update_discovery_progress(
                    self.progress.total_discovered, 
                    self.progress.total_batches_created
                )
                self.batch_state.mark_discovery_complete()
            
            self._save_progress()
            
        except Exception as e:
            error_msg = f"Discovery error: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            self.progress.errors.append(error_msg)
            self.progress.current_cursor = getattr(result, 'cursor', None) if 'result' in locals() else None
            self._save_progress()
            raise
    
    def _save_current_batch(self, documents: List[DocumentMetadata]) -> None:
        """Save current batch of documents"""
        if not documents:
            return
        
        batch_id = self.progress.total_batches_created + 1
        success = self.batch_queue.save_batch(batch_id, documents)
        
        if success:
            self.progress.total_batches_created += 1
            self.logger.info(f"💾 Saved batch {batch_id}: {len(documents)} documents")
        else:
            self.logger.error(f"❌ Failed to save batch {batch_id}")
    
    def _log_progress(self) -> None:
        """Log current discovery progress"""
        if self.progress.estimated_total:
            percentage = (self.progress.total_discovered / self.progress.estimated_total) * 100
            self.logger.info(f"📊 Discovery progress: {self.progress.total_discovered}/{self.progress.estimated_total} "
                           f"({percentage:.1f}%) - {self.progress.total_batches_created} batches")
        else:
            self.logger.info(f"📊 Discovered: {self.progress.total_discovered} documents, "
                           f"{self.progress.total_batches_created} batches")
    
    def get_progress_summary(self) -> Dict[str, any]:
        """Get comprehensive progress summary"""
        return {
            "total_discovered": self.progress.total_discovered,
            "total_batches": self.progress.total_batches_created,
            "estimated_total": self.progress.estimated_total,
            "completion_percentage": (
                (self.progress.total_discovered / self.progress.estimated_total * 100) 
                if self.progress.estimated_total else None
            ),
            "discovery_complete": self.progress.discovery_complete,
            "can_resume": self.progress.current_cursor is not None,
            "last_discovered": self.progress.last_discovered_path,
            "errors": self.progress.errors
        }
    
    def clear_progress(self) -> None:
        """Clear discovery progress (start fresh)"""
        if self.progress_file.exists():
            self.progress_file.unlink()
        
        self.progress = DiscoveryProgress(folder_path=self.folder_path)
        self.logger.info("🗑️ Cleared discovery progress") 