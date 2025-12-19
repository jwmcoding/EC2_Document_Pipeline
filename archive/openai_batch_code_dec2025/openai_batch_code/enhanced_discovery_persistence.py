#!/usr/bin/env python3
"""
Enhanced Discovery Persistence Module with Batch API Support

This module handles saving and loading discovery results with additional
support for tracking batch classification jobs and their status.

Features:
- Batch job tracking and status updates
- Cost estimation and actual cost tracking
- Enhanced metadata for batch processing
- Resume capability for batch operations
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


class DiscoveryPersistence:
    """Enhanced persistence layer with batch API support"""
    
    def __init__(self, output_file: str = "discovery_results.json"):
        self.output_file = output_file
        self.progress_file = f"{output_file}.progress"
        self.batch_jobs_file = f"{output_file}.batch_jobs"
        self.data = self._initialize_data()
        self._buffer = []
        self._batch_buffer = []
    
    def _initialize_data(self) -> Dict[str, Any]:
        """Initialize or load existing discovery data"""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r') as f:
                    data = json.load(f)
                    # Ensure new batch-related fields exist
                    if 'batch_jobs' not in data.get('discovery_metadata', {}):
                        data['discovery_metadata']['batch_jobs'] = []
                    if 'batch_processing' not in data.get('discovery_metadata', {}):
                        data['discovery_metadata']['batch_processing'] = {
                            'enabled': False,
                            'jobs_submitted': 0,
                            'jobs_completed': 0,
                            'estimated_cost': 0.0,
                            'actual_cost': 0.0
                        }
                    return data
            except Exception as e:
                print(f"Warning: Could not load existing file: {e}")
        
        return {
            "discovery_metadata": {
                "source_type": "",
                "source_path": "",
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
    
    def set_discovery_metadata(self, source_type: str, source_path: str, 
                             llm_enabled: bool = False, batch_mode: bool = False):
        """Set discovery metadata with batch processing information"""
        self.data["discovery_metadata"].update({
            "source_type": source_type,
            "source_path": source_path,
            "llm_classification_enabled": llm_enabled,
            "batch_processing": {
                "enabled": batch_mode,
                "jobs_submitted": 0,
                "jobs_completed": 0,
                "estimated_cost": 0.0,
                "actual_cost": 0.0
            }
        })
    
    def add_batch(self, documents: List[Dict[str, Any]], batch_num: int):
        """Add a batch of documents to the buffer"""
        self._batch_buffer.extend(documents)
        
        # Update progress
        self.data["discovery_progress"]["current_batch"] = batch_num
        self.data["discovery_progress"]["documents_discovered"] += len(documents)
        
        # Flush buffer periodically
        if len(self._batch_buffer) >= 500:  # Flush every 500 documents
            self.flush_buffer()
    
    def flush_buffer(self):
        """Flush the buffer to disk"""
        if self._batch_buffer:
            self.data["documents"].extend(self._batch_buffer)
            self.data["discovery_metadata"]["total_documents"] = len(self.data["documents"])
            self._batch_buffer = []
            
            # Save to disk
            with open(self.output_file, 'w') as f:
                json.dump(self.data, f, indent=2)
    
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
                
                break
        
        self.flush_buffer()
    
    def mark_batch_results_applied(self, job_id: str):
        """Mark that batch job results have been applied to documents"""
        for job in self.data["discovery_metadata"]["batch_jobs"]:
            if job["job_id"] == job_id:
                job["results_applied"] = True
                break
        
        self.flush_buffer()
    
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
    
    def save_progress(self, last_path: str, resume_cursor: str = None):
        """Save discovery progress"""
        self.data["discovery_progress"]["last_processed_path"] = last_path
        if resume_cursor:
            self.data["discovery_progress"]["resume_cursor"] = resume_cursor
        
        # Save progress file separately for quick resume
        with open(self.progress_file, 'w') as f:
            json.dump(self.data["discovery_progress"], f, indent=2)
    
    def load_progress(self) -> Dict[str, Any]:
        """Load discovery progress"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        
        return self.data["discovery_progress"]
    
    def mark_discovery_complete(self):
        """Mark discovery as complete"""
        self.flush_buffer()  # Ensure all data is saved
        
        self.data["discovery_metadata"]["discovery_completed"] = datetime.now().isoformat()
        self.data["discovery_metadata"]["discovery_interrupted"] = False
        self.data["discovery_metadata"]["total_batches"] = self.data["discovery_progress"]["current_batch"]
        
        # Save final state
        with open(self.output_file, 'w') as f:
            json.dump(self.data, f, indent=2)
        
        # Clean up progress file
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)
    
    def get_discovery_summary(self) -> Dict[str, Any]:
        """Get comprehensive discovery summary including batch processing info"""
        summary = {
            "source_type": self.data["discovery_metadata"]["source_type"],
            "source_path": self.data["discovery_metadata"]["source_path"],
            "total_documents": len(self.data["documents"]),
            "discovery_started": self.data["discovery_metadata"]["discovery_started"],
            "discovery_completed": self.data["discovery_metadata"]["discovery_completed"],
            "llm_classification_enabled": self.data["discovery_metadata"]["llm_classification_enabled"],
        }
        
        # Add batch processing summary
        batch_info = self.data["discovery_metadata"]["batch_processing"]
        summary["batch_processing"] = {
            "enabled": batch_info["enabled"],
            "jobs_submitted": batch_info["jobs_submitted"],
            "jobs_completed": batch_info["jobs_completed"],
            "estimated_cost": batch_info["estimated_cost"],
            "actual_cost": batch_info["actual_cost"],
            "pending_jobs": len(self.get_pending_batch_jobs()),
            "completed_jobs_pending_application": len(self.get_completed_batch_jobs())
        }
        
        # Calculate statistics
        documents = self.data["documents"]
        classified_documents = sum(1 for doc in documents if "llm_classification" in doc)
        
        if classified_documents > 0:
            # Document type distribution
            doc_types = {}
            total_tokens = 0
            
            for doc in documents:
                if "llm_classification" in doc:
                    doc_type = doc["llm_classification"]["document_type"]
                    doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
                    total_tokens += doc["llm_classification"].get("tokens_used", 0)
            
            summary["statistics"] = {
                "classified_documents": classified_documents,
                "classification_rate": classified_documents / len(documents) if documents else 0,
                "document_types": doc_types,
                "total_tokens_used": total_tokens
            }
        
        return summary
    
    def export_batch_job_report(self, output_file: str = None) -> str:
        """Export detailed batch job report"""
        if not output_file:
            output_file = f"{self.output_file}_batch_report.json"
        
        batch_jobs = self.data["discovery_metadata"]["batch_jobs"]
        batch_processing = self.data["discovery_metadata"]["batch_processing"]
        
        report = {
            "report_generated": datetime.now().isoformat(),
            "discovery_file": self.output_file,
            "batch_processing_summary": batch_processing,
            "batch_jobs": batch_jobs,
            "cost_analysis": {
                "total_estimated_cost": batch_processing["estimated_cost"],
                "total_actual_cost": batch_processing["actual_cost"],
                "cost_savings_vs_immediate": 0.0,  # Would need to calculate based on immediate rates
                "cost_per_document": batch_processing["actual_cost"] / sum(job["document_count"] for job in batch_jobs) if batch_jobs else 0
            }
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        return output_file
    
    def update_document_classification(self, document_index: int, classification_data: Dict[str, Any]):
        """Update a specific document's classification data"""
        if 0 <= document_index < len(self.data["documents"]):
            self.data["documents"][document_index]["llm_classification"] = classification_data
            
            # Save immediately for classification updates
            with open(self.output_file, 'w') as f:
                json.dump(self.data, f, indent=2)
    
    def get_documents_needing_classification(self) -> List[tuple]:
        """Get documents that don't have classification yet"""
        needing_classification = []
        
        for idx, doc in enumerate(self.data["documents"]):
            if "llm_classification" not in doc:
                needing_classification.append((idx, doc))
        
        return needing_classification
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        temp_files = [self.progress_file, self.batch_jobs_file]
        
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    print(f"Warning: Could not remove {temp_file}: {e}")
