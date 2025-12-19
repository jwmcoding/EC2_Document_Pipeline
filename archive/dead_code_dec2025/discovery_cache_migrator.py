"""
Discovery Cache to Batch Migrator

Converts existing discovery cache files to batch processing format,
preserving all LLM classifications and metadata to avoid re-processing.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from utils.document_batch_queue import DocumentBatchQueue
from utils.batch_state import BatchState
from connectors.dropbox_client import DocumentMetadata
from config.colored_logging import ColoredLogger


class DiscoveryCacheMigrator:
    """Migrates existing discovery cache to batch processing format"""
    
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.logger = ColoredLogger("cache_migrator")
    
    def find_discovery_caches(self) -> List[Dict[str, Any]]:
        """Find all existing discovery cache files"""
        discovery_dir = self.cache_dir / "discovery"
        if not discovery_dir.exists():
            return []
        
        caches = []
        for cache_file in discovery_dir.glob("*_discovery.json"):
            try:
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                
                # Get metadata if available
                metadata_file = cache_file.with_name(cache_file.stem.replace('_discovery', '_metadata') + '.json')
                metadata = {}
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                
                cache_info = {
                    "cache_file": str(cache_file),
                    "folder_path": cache_data.get("folder_path", "unknown"),
                    "document_count": cache_data.get("document_count", 0),
                    "timestamp": cache_data.get("timestamp", "unknown"),
                    "partial": cache_data.get("partial", False),
                    "llm_classified_count": metadata.get("llm_classified_count", 0),
                    "avg_confidence": metadata.get("avg_confidence", 0.0),
                    "size_mb": metadata.get("size_mb", 0.0)
                }
                caches.append(cache_info)
                
            except Exception as e:
                self.logger.warning(f"⚠️ Error reading cache {cache_file}: {e}")
        
        return sorted(caches, key=lambda x: x["timestamp"], reverse=True)
    
    def migrate_discovery_to_batches(self, cache_file: str, folder_path: str, 
                                   batch_size: int = 50, 
                                   force_migrate: bool = False) -> bool:
        """Convert discovery cache to batch processing format
        
        Args:
            cache_file: Path to existing discovery cache file
            folder_path: Target folder path for batch processing
            batch_size: Size of batches to create
            force_migrate: If True, overwrite existing batch data
        """
        try:
            # Check if batch data already exists
            batch_state = BatchState(folder_path, batch_size)
            batch_queue = DocumentBatchQueue(folder_path)
            
            if batch_state.discovery_complete and not force_migrate:
                self.logger.info(f"📂 Batch data already exists for {folder_path}")
                self.logger.info("Use force_migrate=True to overwrite existing batch data")
                return False
            
            # Load discovery cache
            self.logger.info(f"📂 Loading discovery cache: {cache_file}")
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            documents_data = cache_data.get("documents", [])
            if not documents_data:
                self.logger.error("❌ No documents found in cache file")
                return False
            
            # Convert to DocumentMetadata objects
            self.logger.info(f"🔄 Converting {len(documents_data)} documents to batch format...")
            documents = []
            for doc_data in documents_data:
                try:
                    doc = DocumentMetadata(**doc_data)
                    documents.append(doc)
                except Exception as e:
                    self.logger.warning(f"⚠️ Error converting document {doc_data.get('path', 'unknown')}: {e}")
            
            if not documents:
                self.logger.error("❌ No valid documents found after conversion")
                return False
            
            # Clear existing batch data if force migrate
            if force_migrate:
                self.logger.info("🗑️ Clearing existing batch data...")
                batch_state.clear_state()
                batch_queue.clear_all_batches()
            
            # Create batches
            total_documents = len(documents)
            total_batches = (total_documents + batch_size - 1) // batch_size  # Ceiling division
            
            self.logger.info(f"📦 Creating {total_batches} batches of {batch_size} documents each...")
            
            batch_id = 1
            for i in range(0, total_documents, batch_size):
                batch_docs = documents[i:i + batch_size]
                
                # Save batch
                batch_metadata = {
                    "migration_source": "discovery_cache",
                    "original_cache_file": cache_file,
                    "migration_timestamp": datetime.now().isoformat(),
                    "batch_size": len(batch_docs),
                    "file_types": list(set(doc.file_type for doc in batch_docs))
                }
                
                # Save to batch queue
                batch_queue.save_batch(batch_id, batch_docs, batch_metadata)
                
                # Create batch record in state
                batch_state.create_batch(batch_id, len(batch_docs))
                
                self.logger.info(f"💾 Created batch {batch_id}: {len(batch_docs)} documents")
                batch_id += 1
            
            # Update batch state
            batch_state.update_discovery_progress(total_documents, total_batches)
            batch_state.mark_discovery_complete()
            
            self.logger.success(f"✅ Migration complete!")
            self.logger.info(f"📊 Results:")
            self.logger.info(f"   📄 Documents migrated: {total_documents}")
            self.logger.info(f"   📦 Batches created: {total_batches}")
            self.logger.info(f"   🔍 LLM classifications preserved: {len([d for d in documents if d.document_type and d.document_type != 'None'])}")
            self.logger.info(f"   💾 Ready for batch processing!")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Migration failed: {e}")
            return False
    
    def show_migration_preview(self, cache_file: str, batch_size: int = 50) -> Dict[str, Any]:
        """Show what migration would look like without actually doing it"""
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            
            documents_data = cache_data.get("documents", [])
            total_documents = len(documents_data)
            total_batches = (total_documents + batch_size - 1) // batch_size
            
            # Count LLM classified documents
            llm_classified = sum(1 for doc in documents_data 
                               if doc.get('document_type') and doc.get('document_type') != 'None')
            
            # Get file type distribution
            file_types = {}
            for doc in documents_data:
                ft = doc.get('file_type', 'unknown')
                file_types[ft] = file_types.get(ft, 0) + 1
            
            preview = {
                "source_cache": cache_file,
                "folder_path": cache_data.get("folder_path"),
                "total_documents": total_documents,
                "batches_to_create": total_batches,
                "batch_size": batch_size,
                "llm_classified_count": llm_classified,
                "file_type_distribution": file_types,
                "cache_timestamp": cache_data.get("timestamp"),
                "cache_partial": cache_data.get("partial", False)
            }
            
            return preview
            
        except Exception as e:
            self.logger.error(f"❌ Preview failed: {e}")
            return {}
    
    def list_available_migrations(self) -> None:
        """List all available discovery caches that can be migrated"""
        caches = self.find_discovery_caches()
        
        if not caches:
            self.logger.info("📂 No discovery cache files found")
            return
        
        self.logger.info(f"📂 Found {len(caches)} discovery cache(s) available for migration:")
        
        for i, cache in enumerate(caches, 1):
            self.logger.info(f"\n{i}. 📁 {cache['folder_path']}")
            self.logger.info(f"   📄 Documents: {cache['document_count']}")
            self.logger.info(f"   🧠 LLM classified: {cache['llm_classified_count']}")
            self.logger.info(f"   📊 Avg confidence: {cache['avg_confidence']:.3f}")
            self.logger.info(f"   📅 Cached: {cache['timestamp']}")
            self.logger.info(f"   💾 Size: {cache['size_mb']:.1f}MB")
            if cache['partial']:
                self.logger.warning(f"   ⚠️  Partial cache (interrupted)")
            self.logger.info(f"   📁 File: {cache['cache_file']}")


def migrate_existing_cache_to_batches(folder_path: str, batch_size: int = 50, 
                                    force: bool = False) -> bool:
    """Convenience function to migrate existing cache for a folder"""
    migrator = DiscoveryCacheMigrator()
    
    # Find cache for this folder
    caches = migrator.find_discovery_caches()
    matching_cache = None
    
    for cache in caches:
        if cache["folder_path"] == folder_path:
            matching_cache = cache
            break
    
    if not matching_cache:
        print(f"❌ No discovery cache found for folder: {folder_path}")
        return False
    
    print(f"🔄 Migrating cache for: {folder_path}")
    print(f"📄 Documents: {matching_cache['document_count']}")
    print(f"🧠 LLM classified: {matching_cache['llm_classified_count']}")
    
    return migrator.migrate_discovery_to_batches(
        matching_cache["cache_file"], 
        folder_path, 
        batch_size, 
        force
    ) 