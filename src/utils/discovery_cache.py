"""
Discovery Cache System for Document Processing Pipeline

Saves expensive LLM classification results and metadata extraction to disk
to prevent loss from token expiration, network failures, or other interruptions.

Key Features:
- Saves DocumentMetadata with LLM classifications to JSON files
- Supports incremental updates and resume capability
- Includes timestamp tracking and cache validation
- Prevents re-running expensive OpenAI API calls
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import asdict, is_dataclass
import logging
import hashlib

class DiscoveryCache:
    """Persistent cache for discovery results to prevent data loss"""
    
    def __init__(self, cache_name: str = "default", cache_dir: str = "cache/discovery"):
        self.cache_name = cache_name
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.cache_file = self.cache_dir / f"{cache_name}_discovery.json"
        self.metadata_file = self.cache_dir / f"{cache_name}_metadata.json"
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📁 Discovery cache initialized: {self.cache_file}")
    
    def _serialize_dataclass(self, obj: Any) -> Any:
        """Recursively serialize dataclasses to dict for JSON storage"""
        if is_dataclass(obj):
            return asdict(obj)
        elif isinstance(obj, list):
            return [self._serialize_dataclass(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._serialize_dataclass(value) for key, value in obj.items()}
        else:
            return obj
    
    def save_discovery_results(self, folder_path: str, documents: List[Any], 
                             partial: bool = False) -> bool:
        """Save discovery results to cache file
        
        Args:
            folder_path: Target folder that was processed
            documents: List of DocumentMetadata objects
            partial: If True, indicates this is a partial save during processing
        """
        try:
            # Serialize documents (handle dataclasses)
            serialized_docs = [self._serialize_dataclass(doc) for doc in documents]
            
            cache_data = {
                "folder_path": folder_path,
                "timestamp": datetime.now().isoformat(),
                "document_count": len(documents),
                "partial": partial,
                "documents": serialized_docs,
                "version": "1.0"
            }
            
            # Save main cache file
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2, default=str)
            
            # Save metadata summary
            metadata_summary = {
                "folder_path": folder_path,
                "timestamp": datetime.now().isoformat(),
                "document_count": len(documents),
                "partial": partial,
                "cache_file": str(self.cache_file),
                "size_mb": round(os.path.getsize(self.cache_file) / (1024 * 1024), 2),
                "llm_classified_count": sum(1 for doc in documents 
                                          if getattr(doc, 'document_type', None) and 
                                             getattr(doc, 'document_type', None) != 'None'),
                "avg_confidence": sum(getattr(doc, 'document_type_confidence', 0) for doc in documents) / len(documents) if documents else 0
            }
            
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata_summary, f, indent=2, default=str)
            
            status = "partial" if partial else "complete"
            self.logger.info(f"💾 Saved {status} discovery cache: {len(documents)} documents")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save discovery cache: {e}")
            return False
    
    def load_discovery_results(self, max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
        """Load discovery results from cache if they exist and are recent enough
        
        Args:
            max_age_hours: Maximum age of cache to accept (default 24 hours)
            
        Returns:
            Dict with cache data or None if no valid cache found
        """
        try:
            if not self.cache_file.exists():
                self.logger.info("📂 No discovery cache file found")
                return None
            
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            # Check cache age
            cache_time = datetime.fromisoformat(cache_data['timestamp'])
            age = datetime.now() - cache_time
            
            if age > timedelta(hours=max_age_hours):
                self.logger.warning(f"⏰ Discovery cache too old: {age} > {max_age_hours}h")
                return None
            
            self.logger.info(f"📂 Loaded discovery cache: {cache_data['document_count']} documents "
                           f"from {cache_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            return cache_data
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load discovery cache: {e}")
            return None
    
    def cache_exists(self, folder_path: str) -> bool:
        """Check if a valid cache exists for the given folder"""
        if not self.cache_file.exists():
            return False
        
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            # Check if folder matches and cache is recent
            return (cache_data.get('folder_path') == folder_path and 
                   self._is_cache_recent(cache_data.get('timestamp')))
        except:
            return False
    
    def _is_cache_recent(self, timestamp_str: str, max_age_hours: int = 24) -> bool:
        """Check if cache timestamp is within acceptable age"""
        try:
            cache_time = datetime.fromisoformat(timestamp_str)
            age = datetime.now() - cache_time
            return age <= timedelta(hours=max_age_hours)
        except:
            return False
    
    def get_cache_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the current cache"""
        try:
            if not self.metadata_file.exists():
                return None
            
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def list_caches(self) -> List[Dict[str, Any]]:
        """List all available discovery caches"""
        caches = []
        
        try:
            for cache_file in self.cache_dir.glob("*_discovery.json"):
                try:
                    with open(cache_file, 'r') as f:
                        cache_data = json.load(f)
                    
                    metadata_file = cache_file.parent / f"{cache_file.stem.replace('_discovery', '_metadata')}.json"
                    metadata = {}
                    if metadata_file.exists():
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                    
                    cache_info = {
                        "file": str(cache_file),
                        "cache_name": cache_file.stem.replace('_discovery', ''),
                        "folder_path": cache_data.get('folder_path', 'unknown'),
                        "timestamp": cache_data.get('timestamp', 'unknown'),
                        "document_count": cache_data.get('document_count', 0),
                        "partial": cache_data.get('partial', False),
                        "size_mb": metadata.get('size_mb', 0),
                        "llm_classified_count": metadata.get('llm_classified_count', 0),
                        "age_hours": self._get_cache_age_hours(cache_data.get('timestamp'))
                    }
                    
                    caches.append(cache_info)
                except Exception as e:
                    self.logger.warning(f"⚠️ Error reading cache {cache_file}: {e}")
            
            # Sort by timestamp (newest first)
            caches.sort(key=lambda x: x['timestamp'], reverse=True)
            
        except Exception as e:
            self.logger.error(f"❌ Error listing caches: {e}")
        
        return caches
    
    def _get_cache_age_hours(self, timestamp_str: str) -> float:
        """Calculate cache age in hours"""
        try:
            cache_time = datetime.fromisoformat(timestamp_str)
            age = datetime.now() - cache_time
            return round(age.total_seconds() / 3600, 1)
        except:
            return -1
    
    def clear_cache(self) -> bool:
        """Clear the current cache files"""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
                self.logger.info(f"🗑️ Cleared cache file: {self.cache_file}")
            
            if self.metadata_file.exists():
                self.metadata_file.unlink()
                self.logger.info(f"🗑️ Cleared metadata file: {self.metadata_file}")
            
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to clear cache: {e}")
            return False
    
    def get_folder_hash(self, folder_path: str) -> str:
        """Generate a hash for a folder path to create unique cache names"""
        return hashlib.md5(folder_path.encode()).hexdigest()[:8] 