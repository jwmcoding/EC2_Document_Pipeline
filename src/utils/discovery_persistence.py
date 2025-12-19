"""
Discovery Persistence Module for Document Processing Pipeline

This module handles saving and loading discovery results in JSON format,
supporting batch saves, progress tracking, and incremental updates.
"""

import json
import math
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Union
from dataclasses import asdict, is_dataclass
import logging
import hashlib
from filelock import FileLock
from datetime import timezone


def _sanitize_for_json(obj: Any) -> Any:
    """
    Recursively sanitize data for JSON serialization.
    
    Converts NaN and Infinity to None (null in JSON).
    This prevents invalid JSON with literal 'NaN' or 'Infinity' values.
    
    Args:
        obj: Any Python object (dict, list, or scalar)
        
    Returns:
        Sanitized object safe for json.dump()
    """
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj

try:
    from src.config.colored_logging import ColoredLogger
except ImportError:
    # Fallback for direct execution
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from src.config.colored_logging import ColoredLogger


class DiscoveryPersistence:
    """Manages persistent storage of discovery results with Batch API support"""
    
    def __init__(self, output_file: str = "discovery_results.json"):
        """
        Initialize discovery persistence.
        
        Args:
            output_file: Path to the main discovery JSON file
        """
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Related files
        self.progress_file = self.output_file.with_suffix('.progress.json')
        self.temp_file = self.output_file.with_suffix('.tmp')
        self.lock_file = self.output_file.with_suffix('.lock')
        self.batch_jobs_file = self.output_file.with_suffix('.batch_jobs.json')
        
        # File lock for concurrent access protection
        self.lock = FileLock(str(self.lock_file))
        
        self.logger = ColoredLogger("discovery_persistence")
        
        # Track current session
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.documents_buffer = []
        self.buffer_size = 100  # Flush every 100 documents
        
        # Batched update tracking (to reduce disk writes)
        self._pending_updates = 0
        self._batch_save_threshold = 50  # Save to disk every 50 document updates
        
        # Initialize or load existing data
        self._initialize_storage()
    
    def _initialize_storage(self):
        """Initialize storage structure or load existing data"""
        if self.output_file.exists():
            self.logger.info(f"📂 Loading existing discovery data from {self.output_file}")
            try:
                with self.lock:
                    with open(self.output_file, 'r') as f:
                        self.data = json.load(f)
                        
                # Validate and upgrade schema if needed
                schema_version = self.data.get('discovery_metadata', {}).get('schema_version', '1.0')
                if schema_version != '2.1':
                    self.logger.warning(f"⚠️ Schema version mismatch: {schema_version} vs 2.1, upgrading...")
                    self._upgrade_schema()
                    
                # Ensure batch fields exist for backward compatibility
                if 'batch_processing' not in self.data.get('discovery_metadata', {}):
                    self.data['discovery_metadata']['batch_processing'] = {
                        "enabled": False,
                        "jobs_submitted": 0,
                        "jobs_completed": 0,
                        "estimated_cost": 0.0,
                        "actual_cost": 0.0
                    }
                
                if 'batch_jobs' not in self.data.get('discovery_metadata', {}):
                    self.data['discovery_metadata']['batch_jobs'] = []
                    
            except Exception as e:
                self.logger.error(f"❌ Error loading existing data: {e}")
                self._create_new_storage()
        else:
            self._create_new_storage()
    
    def _create_new_storage(self):
        """Create new storage structure"""
        self.data = {
            "discovery_metadata": {
                "source_type": None,
                "source_path": None,
                "discovery_started": datetime.now().isoformat(),
                "discovery_completed": None,
                "discovery_interrupted": False,
                "total_documents": 0,
                "total_batches": 0,
                "llm_classification_enabled": False,
                "llm_model": "gpt-4.1-mini",
                "schema_version": "2.1",
                "batch_processing": {
                    "enabled": False,
                    "jobs_submitted": 0,
                    "jobs_completed": 0,
                    "estimated_cost": 0.0,
                    "actual_cost": 0.0
                },
                "batch_jobs": []
            },
            "discovery_progress": {
                "last_processed_path": None,
                "current_batch": 0,
                "documents_discovered": 0,
                "resume_cursor": None
            },
            "documents": []
        }
        self.logger.info("📝 Created new discovery storage")
    
    def _upgrade_schema(self):
        """Upgrade schema to current version"""
        if 'discovery_metadata' in self.data:
            # Update schema version
            self.data['discovery_metadata']['schema_version'] = '2.1'
            
            # Add batch processing fields if missing
            if 'batch_processing' not in self.data['discovery_metadata']:
                self.data['discovery_metadata']['batch_processing'] = {
                    "enabled": False,
                    "jobs_submitted": 0,
                    "jobs_completed": 0,
                    "estimated_cost": 0.0,
                    "actual_cost": 0.0
                }
            
            if 'batch_jobs' not in self.data['discovery_metadata']:
                self.data['discovery_metadata']['batch_jobs'] = []
            
            self.logger.success("✅ Schema upgraded to v2.1 with batch processing support")
    
    def set_discovery_metadata(self, source_type: str, source_path: str, 
                             llm_enabled: bool = False, batch_mode: bool = False):
        """Set initial discovery metadata"""
        self.data["discovery_metadata"].update({
            "source_type": source_type,
            "source_path": source_path,
            "llm_classification_enabled": llm_enabled,
            "discovery_started": datetime.now().isoformat()
        })
        
        # Update batch processing settings
        self.data["discovery_metadata"]["batch_processing"]["enabled"] = batch_mode
        
        self._save_progress()
    
    def add_document(self, document: Union[Dict, Any]):
        """
        Add a single document to the buffer.
        
        Args:
            document: Document data (dict or dataclass)
        """
        # Convert dataclass to dict if needed
        if is_dataclass(document):
            document = self._serialize_dataclass(document)
        
        # Ensure document has required structure
        doc_data = self._ensure_document_structure(document)
        
        self.documents_buffer.append(doc_data)
        
        # Flush if buffer is full
        if len(self.documents_buffer) >= self.buffer_size:
            self.flush_buffer()
    
    def add_batch(self, documents: List[Union[Dict, Any]], batch_num: int):
        """
        Add a batch of documents.
        
        Args:
            documents: List of documents to add
            batch_num: Batch number for tracking
        """
        self.logger.info(f"💾 Saving batch {batch_num} with {len(documents)} documents")
        
        for doc in documents:
            self.add_document(doc)
        
        # Update progress
        self.data["discovery_progress"]["current_batch"] = batch_num
        
        # Always flush after batch
        self.flush_buffer()
    
    def flush_buffer(self):
        """Flush document buffer to disk"""
        if not self.documents_buffer:
            return
        
        try:
            with self.lock:
                # Add buffered documents to data
                self.data["documents"].extend(self.documents_buffer)
                
                # Update counts
                self.data["discovery_metadata"]["total_documents"] = len(self.data["documents"])
                self.data["discovery_progress"]["documents_discovered"] = len(self.data["documents"])
                
                # Save to temp file first (sanitize NaN/Inf to prevent invalid JSON)
                with open(self.temp_file, 'w') as f:
                    json.dump(_sanitize_for_json(self.data), f, indent=2, default=str)
                
                # Atomic rename
                shutil.move(str(self.temp_file), str(self.output_file))
                
                self.logger.success(f"✅ Flushed {len(self.documents_buffer)} documents to disk")
                self.documents_buffer.clear()
                
                # Update progress file
                self._save_progress()
                
        except Exception as e:
            self.logger.error(f"❌ Error flushing buffer: {e}")
            raise
    
    def update_document_metadata(self, file_path: str, updates: Dict[str, Any], save_immediately: bool = False):
        """
        Update metadata for a specific document.
        
        Args:
            file_path: Path to identify the document
            updates: Dictionary of fields to update
            save_immediately: If True, save to disk immediately (default: batched)
        """
        updated = False
        
        with self.lock:
            for i, doc in enumerate(self.data["documents"]):
                doc_path = doc.get("file_info", {}).get("path") or doc.get("path")
                if doc_path == file_path:
                    # Update fields
                    for key, value in updates.items():
                        if "." in key:  # Handle nested keys like "processing_status.processed"
                            parts = key.split(".")
                            target = doc
                            for part in parts[:-1]:
                                if part not in target:
                                    target[part] = {}
                                target = target[part]
                            target[parts[-1]] = value
                        else:
                            doc[key] = value
                    
                    updated = True
                    self._pending_updates += 1
                    self.logger.info(f"📝 Updated metadata for {file_path}")
                    break
            
            if updated:
                # Batch saves: only write to disk every N updates or if explicitly requested
                if save_immediately or self._pending_updates >= self._batch_save_threshold:
                    self._atomic_save()
                    self._pending_updates = 0
            else:
                self.logger.warning(f"⚠️ Document not found: {file_path}")
        
        return updated
    
    def _atomic_save(self):
        """Save data atomically using temp file + rename pattern"""
        try:
            # Write to temp file first (sanitize NaN/Inf to prevent invalid JSON)
            with open(self.temp_file, 'w') as f:
                json.dump(_sanitize_for_json(self.data), f, indent=2, default=str)
                f.flush()          # Flush Python buffers
                os.fsync(f.fileno())  # Force write to disk
            
            # Atomic rename (works on POSIX systems)
            shutil.move(str(self.temp_file), str(self.output_file))
            
        except Exception as e:
            self.logger.error(f"❌ Error during atomic save: {e}")
            # Try to clean up temp file
            if self.temp_file.exists():
                try:
                    self.temp_file.unlink()
                except:
                    pass
            raise
    
    def flush_updates(self):
        """Force save any pending updates to disk"""
        with self.lock:
            if self._pending_updates > 0:
                self._atomic_save()
                self._pending_updates = 0
                self.logger.info(f"💾 Flushed pending updates to disk")
    
    def get_discovery_summary(self) -> Dict[str, Any]:
        """Get summary of discovery results"""
        summary = {
            "source_type": self.data["discovery_metadata"]["source_type"],
            "source_path": self.data["discovery_metadata"]["source_path"],
            "total_documents": self.data["discovery_metadata"]["total_documents"],
            "discovery_started": self.data["discovery_metadata"]["discovery_started"],
            "discovery_completed": self.data["discovery_metadata"]["discovery_completed"],
            "is_complete": self.data["discovery_metadata"]["discovery_completed"] is not None,
            "llm_classification_enabled": self.data["discovery_metadata"]["llm_classification_enabled"]
        }
        
        # Add statistics
        if self.data["documents"]:
            classified_count = sum(1 for doc in self.data["documents"] 
                                 if doc.get("llm_classification", {}).get("document_type"))
            
            summary["statistics"] = {
                "classified_documents": classified_count,
                "classification_rate": classified_count / len(self.data["documents"]) if self.data["documents"] else 0,
                "document_types": self._get_document_type_distribution(),
                "file_types": self._get_file_type_distribution()
            }
        
        return summary
    
    def get_documents(self, start_index: int = 0, limit: Optional[int] = None) -> List[Dict]:
        """
        Get documents from the discovery results.
        
        Args:
            start_index: Starting index for pagination
            limit: Maximum number of documents to return
            
        Returns:
            List of document dictionaries
        """
        documents = self.data["documents"][start_index:]
        if limit:
            documents = documents[:limit]
        return documents
    
    def get_unprocessed_documents(self, limit: Optional[int] = None) -> List[Dict]:
        """Get documents that haven't been processed yet"""
        unprocessed = [
            doc for doc in self.data["documents"]
            if not doc.get("processing_status", {}).get("processed", False)
        ]
        
        if limit:
            unprocessed = unprocessed[:limit]
            
        return unprocessed
    
    def mark_discovery_complete(self):
        """Mark discovery as complete"""
        self.flush_buffer()  # Ensure all documents are saved
        
        self.data["discovery_metadata"]["discovery_completed"] = datetime.now().isoformat()
        self.data["discovery_metadata"]["discovery_interrupted"] = False
        self.data["discovery_metadata"]["total_batches"] = self.data["discovery_progress"]["current_batch"]
        
        with self.lock:
            # Sanitize NaN/Inf to prevent invalid JSON
            with open(self.output_file, 'w') as f:
                json.dump(_sanitize_for_json(self.data), f, indent=2, default=str)
        
        self.logger.success("🎉 Discovery marked as complete")
    
    def save_progress(self, last_path: str, cursor: Optional[str] = None):
        """Save discovery progress for resume capability"""
        self.data["discovery_progress"]["last_processed_path"] = last_path
        self.data["discovery_progress"]["resume_cursor"] = cursor
        self._save_progress()
    
    def load_progress(self) -> Dict[str, Any]:
        """Load discovery progress"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading progress: {e}")
        
        return self.data["discovery_progress"]
    
    def _save_progress(self):
        """Save progress to separate file"""
        progress_data = {
            "session_id": self.session_id,
            "last_update": datetime.now().isoformat(),
            "discovery_progress": self.data["discovery_progress"],
            "documents_discovered": len(self.data["documents"]),
            "buffer_size": len(self.documents_buffer)
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
    
    def save_batch_job(self, job_id: str, document_count: int, estimated_cost: float = 0.0):
        """Save batch job information"""
        job_info = {
            "job_id": job_id,
            "submitted_at": datetime.now().isoformat(),
            "document_count": document_count,
            "estimated_cost": estimated_cost,
            "status": "submitted",
            "completed_at": None,
            "actual_cost": None,
            "results_applied": False
        }
        
        self.data["discovery_metadata"]["batch_jobs"].append(job_info)
        self.data["discovery_metadata"]["batch_processing"]["jobs_submitted"] += 1
        self.data["discovery_metadata"]["batch_processing"]["estimated_cost"] += estimated_cost
        
        # Save immediately for batch job tracking
        self.flush_buffer()
        self.logger.success(f"✅ Saved batch job: {job_id} ({document_count} documents)")
    
    def update_batch_job_status(self, job_id: str, status: str, actual_cost: float = None,
                               completed_at: str = None):
        """Update batch job status"""
        for job in self.data["discovery_metadata"]["batch_jobs"]:
            if job["job_id"] == job_id:
                job["status"] = status
                if completed_at:
                    job["completed_at"] = completed_at
                if actual_cost is not None:
                    job["actual_cost"] = actual_cost
                    self.data["discovery_metadata"]["batch_processing"]["actual_cost"] += actual_cost
                
                if status == "completed":
                    self.data["discovery_metadata"]["batch_processing"]["jobs_completed"] += 1
                
                self.flush_buffer()
                self.logger.info(f"📊 Updated batch job {job_id}: {status}")
                break
    
    def mark_batch_results_applied(self, job_id: str):
        """Mark that batch job results have been applied to documents"""
        for job in self.data["discovery_metadata"]["batch_jobs"]:
            if job["job_id"] == job_id:
                job["results_applied"] = True
                self.flush_buffer()
                self.logger.success(f"✅ Marked batch job results applied: {job_id}")
                break
    
    def get_pending_batch_jobs(self) -> List[Dict[str, Any]]:
        """Get list of pending batch jobs"""
        return [
            job for job in self.data["discovery_metadata"]["batch_jobs"]
            if job["status"] not in ["completed", "failed", "cancelled"]
        ]
    
    def get_completed_batch_jobs(self) -> List[Dict[str, Any]]:
        """Get list of completed batch jobs that haven't had results applied"""
        return [
            job for job in self.data["discovery_metadata"]["batch_jobs"]
            if job["status"] == "completed" and not job.get("results_applied", False)
        ]
    
    def get_batch_processing_summary(self) -> Dict[str, Any]:
        """Get comprehensive batch processing summary"""
        batch_info = self.data["discovery_metadata"]["batch_processing"]
        return {
            "enabled": batch_info["enabled"],
            "jobs_submitted": batch_info["jobs_submitted"],
            "jobs_completed": batch_info["jobs_completed"],
            "estimated_cost": batch_info["estimated_cost"],
            "actual_cost": batch_info["actual_cost"],
            "pending_jobs": len(self.get_pending_batch_jobs()),
            "completed_jobs_pending_application": len(self.get_completed_batch_jobs()),
            "cost_savings": batch_info["estimated_cost"] * 0.5 if batch_info["estimated_cost"] > 0 else 0.0  # 50% savings vs immediate
        }
    
    def _ensure_document_structure(self, document: Dict) -> Dict:
        """Ensure document has the required structure"""
        # Initialize required sections if missing
        if "source_metadata" not in document:
            document["source_metadata"] = {}
        
        if "file_info" not in document:
            document["file_info"] = {}
        
        if "business_metadata" not in document:
            document["business_metadata"] = {}
        
        # NEW: Handle Salesforce Deal metadata
        if "deal_metadata" not in document:
            document["deal_metadata"] = {}
            
        if "llm_classification" not in document:
            document["llm_classification"] = {
                "document_type": None,
                "confidence": 0.0,
                "reasoning": None,
                "classification_method": "pending_phase_2_processing",
                "alternative_types": [],
                "tokens_used": 0
            }
        
        if "processing_status" not in document:
            document["processing_status"] = {
                "processed": False,
                "processing_date": None,
                "processor_version": None,
                "parser_backend": None,  # PDF parser selection (docling, mistral, pdfplumber)
                "content_parser": None,  # Actual parser for file type (extract_msg, python_docx, etc.)
                "chunks_created": 0,
                "vectors_created": 0,
                "pinecone_namespace": None,
                "processing_errors": [],
                "processing_time_seconds": None
            }
        
        return document
    
    def _serialize_dataclass(self, obj: Any) -> Any:
        """Recursively serialize dataclasses to dict with proper structure"""
        if is_dataclass(obj):
            # Special handling for DocumentMetadata to organize fields properly
            if obj.__class__.__name__ == 'DocumentMetadata':
                return self._serialize_document_metadata(obj)
            else:
                return {k: self._serialize_dataclass(v) for k, v in asdict(obj).items()}
        elif isinstance(obj, list):
            return [self._serialize_dataclass(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self._serialize_dataclass(v) for k, v in obj.items()}
        else:
            return obj
    
    def _serialize_document_metadata(self, doc_metadata) -> Dict:
        """Serialize DocumentMetadata with proper field organization"""
        doc_dict = asdict(doc_metadata)
        
        # Organize into structured sections
        structured_doc = {
            "source_metadata": {
                "source_type": "salesforce" if hasattr(doc_metadata, 'deal_id') and doc_metadata.deal_id else "dropbox",
                "source_id": doc_dict.get("dropbox_id", "") or doc_dict.get("salesforce_content_version_id", ""),
                "source_path": doc_dict.get("path", "")
            },
            "file_info": {
                "path": doc_dict.get("path", ""),
                "name": doc_dict.get("name", ""),
                "size": doc_dict.get("size", 0),
                "size_mb": doc_dict.get("size_mb", 0.0),
                "file_type": doc_dict.get("file_type", ""),
                "modified_time": doc_dict.get("modified_time", ""),
                "content_hash": doc_dict.get("content_hash")
            },
            "business_metadata": {
                "year": doc_dict.get("year"),
                "week_number": doc_dict.get("week_number"),
                "week_date": doc_dict.get("week_date"),
                "vendor": doc_dict.get("vendor"),
                "client": doc_dict.get("client"),
                "deal_number": doc_dict.get("deal_number"),
                "deal_name": doc_dict.get("deal_name"),
                "extraction_confidence": doc_dict.get("extraction_confidence", 0.0),
                "path_components": doc_dict.get("path_components", [])
            },
            "deal_metadata": {
                # Core Deal Info
                "deal_id": doc_dict.get("deal_id"),
                "deal_subject": doc_dict.get("deal_subject"),
                "deal_status": doc_dict.get("deal_status"),
                "deal_reason": doc_dict.get("deal_reason"),
                "deal_start_date": doc_dict.get("deal_start_date"),
                "negotiated_by": doc_dict.get("negotiated_by"),
                
                # Financial Metrics
                "proposed_amount": doc_dict.get("proposed_amount"),
                "final_amount": doc_dict.get("final_amount"),
                "savings_1yr": doc_dict.get("savings_1yr"),
                "savings_3yr": doc_dict.get("savings_3yr"),
                "savings_target": doc_dict.get("savings_target"),
                "savings_percentage": doc_dict.get("savings_percentage"),
                
                # Client/Vendor Info
                "client_id": doc_dict.get("client_id"),
                "client_name": doc_dict.get("client_name"),
                "vendor_id": doc_dict.get("vendor_id"),
                "vendor_name": doc_dict.get("vendor_name"),
                
                # Contract Info
                "contract_term": doc_dict.get("contract_term"),
                "contract_start": doc_dict.get("contract_start"),
                "contract_end": doc_dict.get("contract_end"),
                "effort_level": doc_dict.get("effort_level"),
                "has_fmv_report": doc_dict.get("has_fmv_report"),
                "deal_origin": doc_dict.get("deal_origin"),
                
                # Salesforce specific
                "salesforce_content_version_id": doc_dict.get("salesforce_content_version_id")
            },
            "llm_classification": {
                "document_type": doc_dict.get("document_type"),
                "confidence": doc_dict.get("document_type_confidence", 0.0),
                "reasoning": doc_dict.get("classification_reasoning"),
                "classification_method": doc_dict.get("classification_method", "pending_phase_2_processing"),
                "alternative_types": doc_dict.get("alternative_document_types", []),
                "tokens_used": doc_dict.get("classification_tokens_used", 0)
            },
            "processing_status": {
                "processed": doc_dict.get("processed", False),
                "processing_date": None
            }
        }
        
        return structured_doc

    # -----------------------------
    # Filtering helpers (Phase 2)
    # -----------------------------
    @staticmethod
    def _normalize_file_ext(value: Optional[str]) -> str:
        if not value:
            return ""
        v = str(value).strip().lower()
        if not v:
            return ""
        return v if v.startswith(".") else f".{v}"

    @staticmethod
    def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse a timestamp from discovery JSON into a datetime.

        Accepts:
        - YYYY-MM-DD
        - ISO 8601 datetime (with or without timezone)
        - Trailing 'Z'
        """
        if not value:
            return None
        raw = str(value).strip()
        if not raw:
            return None

        # Handle trailing Z (UTC)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"

        # Handle date-only
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            try:
                return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
            except Exception:
                return None

        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None

        # Assume UTC if timezone missing (consistent comparisons)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    @staticmethod
    def _get_file_info(doc: Dict[str, Any]) -> Dict[str, Any]:
        return doc.get("file_info", {}) if isinstance(doc, dict) else {}

    @classmethod
    def _get_effective_file_type(cls, doc: Dict[str, Any]) -> str:
        file_info = cls._get_file_info(doc)
        ext = cls._normalize_file_ext(file_info.get("file_type"))
        if ext:
            return ext
        name = str(file_info.get("name", "")).strip()
        if "." in name:
            return cls._normalize_file_ext(name.split(".")[-1])
        path = str(file_info.get("path", "")).strip()
        if "." in path:
            return cls._normalize_file_ext(path.split(".")[-1])
        return ""

    @classmethod
    def _parse_deal_date(cls, date_str: Optional[str]) -> Optional[datetime]:
        """
        Parse deal creation date from Salesforce format.
        
        Handles formats:
        - "M/D/YY HH:MM" (e.g., "10/23/17 15:09") - primary Salesforce format
        - "MM/DD/YY HH:MM" (e.g., "01/05/18 09:30")
        - ISO 8601 fallback
        
        Returns naive datetime (no timezone) for consistent comparison.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        
        date_str = date_str.strip()
        if not date_str:
            return None
        
        # Try Salesforce format first: M/D/YY HH:MM
        for fmt in ["%m/%d/%y %H:%M", "%m/%d/%Y %H:%M", "%m/%d/%y", "%m/%d/%Y"]:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Fallback to ISO datetime parser (strip timezone for consistency)
        dt = cls._parse_iso_datetime(date_str)
        if dt and dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt
    
    @classmethod
    def _get_deal_creation_date(cls, doc: Dict[str, Any]) -> Optional[str]:
        """Get deal_creation_date from document, checking both metadata locations."""
        # Check business_metadata first (primary location)
        bm = doc.get("business_metadata", {})
        if bm.get("deal_creation_date"):
            return bm["deal_creation_date"]
        
        # Fallback to deal_metadata
        dm = doc.get("deal_metadata", {})
        if dm.get("deal_creation_date"):
            return dm["deal_creation_date"]
        
        return None

    @classmethod
    def filter_documents(
        cls,
        documents: List[Dict[str, Any]],
        *,
        include_processed: bool = False,
        include_file_types: Optional[Set[str]] = None,
        exclude_file_types: Optional[Set[str]] = None,
        modified_after: Optional[str] = None,
        modified_before: Optional[str] = None,
        deal_created_after: Optional[str] = None,
        deal_created_before: Optional[str] = None,
        min_size_kb: Optional[float] = None,
        max_size_mb: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Filter discovery documents using a small set of production-safe filters.
        
        Date filters:
        - modified_after/before: Filter by file modified_time (disk date - less reliable)
        - deal_created_after/before: Filter by deal_creation_date from Salesforce (authoritative)

        Returns dict:
          {
            "documents": [...],
            "stats": {...}
          }
        """
        include_set = {cls._normalize_file_ext(x) for x in (include_file_types or set()) if x}
        exclude_set = {cls._normalize_file_ext(x) for x in (exclude_file_types or set()) if x}

        after_dt = cls._parse_iso_datetime(modified_after) if modified_after else None
        before_dt = cls._parse_iso_datetime(modified_before) if modified_before else None
        
        # Parse deal date filters (use deal date parser - returns naive datetime for consistency)
        deal_after_dt = cls._parse_deal_date(deal_created_after) if deal_created_after else None
        deal_before_dt = cls._parse_deal_date(deal_created_before) if deal_created_before else None

        stats = {
            "input_total": len(documents),
            "excluded_processed": 0,
            "excluded_file_type": 0,
            "excluded_modified_time_missing_or_invalid": 0,
            "excluded_modified_after": 0,
            "excluded_modified_before": 0,
            "excluded_deal_date_missing": 0,
            "excluded_deal_created_after": 0,
            "excluded_deal_created_before": 0,
            "excluded_min_size": 0,
            "excluded_max_size": 0,
            "output_total": 0,
        }

        filtered: List[Dict[str, Any]] = []

        for doc in documents:
            if not include_processed:
                if doc.get("processing_status", {}).get("processed", False):
                    stats["excluded_processed"] += 1
                    continue

            ext = cls._get_effective_file_type(doc)
            if ext and ext in exclude_set:
                stats["excluded_file_type"] += 1
                continue
            if include_set:
                # If we can't determine the type, treat as not included when include-set is provided
                if not ext or ext not in include_set:
                    stats["excluded_file_type"] += 1
                    continue

            file_info = cls._get_file_info(doc)
            size_mb = file_info.get("size_mb")
            size_bytes = file_info.get("size")
            try:
                if size_mb is None and size_bytes is not None:
                    size_mb = float(size_bytes) / (1024 * 1024)
                elif size_mb is not None:
                    size_mb = float(size_mb)
            except Exception:
                size_mb = None

            if min_size_kb is not None:
                if size_mb is None or (size_mb * 1024) < float(min_size_kb):
                    stats["excluded_min_size"] += 1
                    continue

            if max_size_mb is not None:
                if size_mb is None or size_mb > float(max_size_mb):
                    stats["excluded_max_size"] += 1
                    continue

            if after_dt or before_dt:
                doc_dt = cls._parse_iso_datetime(file_info.get("modified_time"))
                if not doc_dt:
                    stats["excluded_modified_time_missing_or_invalid"] += 1
                    continue

                if after_dt and doc_dt < after_dt:
                    stats["excluded_modified_after"] += 1
                    continue

                if before_dt and doc_dt > before_dt:
                    stats["excluded_modified_before"] += 1
                    continue

            # Deal creation date filtering (authoritative Salesforce date)
            if deal_after_dt or deal_before_dt:
                deal_date_str = cls._get_deal_creation_date(doc)
                deal_dt = cls._parse_deal_date(deal_date_str) if deal_date_str else None
                
                if not deal_dt:
                    stats["excluded_deal_date_missing"] += 1
                    continue
                
                if deal_after_dt and deal_dt < deal_after_dt:
                    stats["excluded_deal_created_after"] += 1
                    continue
                
                if deal_before_dt and deal_dt > deal_before_dt:
                    stats["excluded_deal_created_before"] += 1
                    continue

            filtered.append(doc)

        stats["output_total"] = len(filtered)
        return {"documents": filtered, "stats": stats}
    
    def _get_document_type_distribution(self) -> Dict[str, int]:
        """Get distribution of document types"""
        distribution = {}
        for doc in self.data["documents"]:
            doc_type = doc.get("llm_classification", {}).get("document_type", "Unknown")
            distribution[doc_type] = distribution.get(doc_type, 0) + 1
        return distribution
    
    def _get_file_type_distribution(self) -> Dict[str, int]:
        """Get distribution of file types"""
        distribution = {}
        for doc in self.data["documents"]:
            file_type = doc.get("file_info", {}).get("file_type") or doc.get("file_type", "Unknown")
            distribution[file_type] = distribution.get(file_type, 0) + 1
        return distribution
    
    def _get_year_distribution(self) -> Dict[str, int]:
        """Get distribution of documents by year (from modified_time or deal_creation_date)"""
        from datetime import datetime
        distribution = {}
        no_date_count = 0
        
        for doc in self.data["documents"]:
            year = None
            
            # Try modified_time first
            modified_time = doc.get("file_info", {}).get("modified_time")
            if modified_time:
                try:
                    if isinstance(modified_time, str):
                        # Handle various date formats
                        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                            try:
                                year = datetime.strptime(modified_time[:19], fmt).year
                                break
                            except ValueError:
                                continue
                except Exception:
                    pass
            
            # Try deal_creation_date as fallback
            if not year:
                deal_date = doc.get("deal_metadata", {}).get("deal_creation_date") or \
                           doc.get("business_metadata", {}).get("deal_creation_date")
                if deal_date:
                    try:
                        if isinstance(deal_date, str):
                            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                                try:
                                    year = datetime.strptime(deal_date[:19], fmt).year
                                    break
                                except ValueError:
                                    continue
                    except Exception:
                        pass
            
            if year:
                year_str = str(year)
                distribution[year_str] = distribution.get(year_str, 0) + 1
            else:
                no_date_count += 1
        
        if no_date_count > 0:
            distribution["no_date"] = no_date_count
            
        return distribution
    
    def _get_size_statistics(self) -> Dict[str, Any]:
        """Get file size statistics"""
        sizes = []
        for doc in self.data["documents"]:
            size_mb = doc.get("file_info", {}).get("size_mb", 0)
            if size_mb:
                sizes.append(size_mb)
        
        if not sizes:
            return {"total_size_mb": 0, "avg_size_mb": 0, "max_size_mb": 0, "min_size_mb": 0}
        
        return {
            "total_size_mb": round(sum(sizes), 2),
            "avg_size_mb": round(sum(sizes) / len(sizes), 2),
            "max_size_mb": round(max(sizes), 2),
            "min_size_mb": round(min(sizes), 2),
            "files_over_10mb": sum(1 for s in sizes if s > 10),
            "files_over_50mb": sum(1 for s in sizes if s > 50)
        }
    
    def get_detailed_summary(self) -> Dict[str, Any]:
        """Get comprehensive discovery summary with date ranges, file types, and sizes"""
        basic_summary = self.get_discovery_summary()
        
        # Add detailed statistics
        year_dist = self._get_year_distribution()
        file_dist = self._get_file_type_distribution()
        size_stats = self._get_size_statistics()
        
        # Calculate date range
        years = [int(y) for y in year_dist.keys() if y != "no_date"]
        date_range = {
            "earliest_year": min(years) if years else None,
            "latest_year": max(years) if years else None,
            "pre_2000_count": sum(v for k, v in year_dist.items() if k != "no_date" and int(k) < 2000),
            "2000_and_later_count": sum(v for k, v in year_dist.items() if k != "no_date" and int(k) >= 2000)
        }
        
        basic_summary["detailed_statistics"] = {
            "year_distribution": dict(sorted(year_dist.items())),
            "file_type_distribution": dict(sorted(file_dist.items(), key=lambda x: x[1], reverse=True)),
            "date_range": date_range,
            "size_statistics": size_stats
        }
        
        return basic_summary
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure buffer is flushed"""
        self.flush_buffer() 