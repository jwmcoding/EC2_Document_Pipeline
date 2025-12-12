"""
Enhanced Salesforce File Source Implementation

This module provides file source integration for pre-enriched Salesforce files 
with comprehensive metadata from enhanced JSON. No additional processing needed
as all business intelligence fields are already populated.
"""

import json
from pathlib import Path
from typing import Generator, Dict, Optional, List, Any
import logging
import os

from .file_source_interface import FileSourceInterface, FileMetadata
try:
    from models.document_models import DocumentMetadata
except ImportError:
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from models.document_models import DocumentMetadata


class EnhancedSalesforceFileSource(FileSourceInterface):
    """Enhanced Salesforce source using pre-enriched JSON metadata with 57+ fields per document"""
    
    def __init__(self, 
                 organized_files_dir: str,
                 enhanced_json_path: str):
        """
        Initialize with enhanced JSON metadata.
        
        Args:
            organized_files_dir: Path to /Volumes/Jeff_2TB/organized_salesforce_v2
            enhanced_json_path: Path to enhanced_salesforce_documents_full_v2.json
        """
        super().__init__()
        self.organized_files_dir = Path(organized_files_dir)
        self.enhanced_json_path = enhanced_json_path
        self.logger = logging.getLogger(__name__)
        
        # Validate paths
        if not self.organized_files_dir.exists():
            raise FileNotFoundError(f"Organized files directory not found: {self.organized_files_dir}")
        if not Path(self.enhanced_json_path).exists():
            raise FileNotFoundError(f"Enhanced JSON file not found: {self.enhanced_json_path}")
        
        # Load enhanced metadata
        self._enhanced_metadata: Dict[str, Dict] = {}
        self._load_enhanced_metadata()
    
    def _load_enhanced_metadata(self):
        """Load the enhanced JSON metadata with nested field extraction"""
        self.logger.info(f"Loading enhanced metadata from {self.enhanced_json_path}")
        
        with open(self.enhanced_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract documents from the JSON structure
        if isinstance(data, dict) and 'documents' in data:
            documents = data['documents']
        elif isinstance(data, list):
            documents = data
        else:
            raise ValueError(f"Unexpected JSON structure: {type(data)}")
        
        # Create lookup by path for fast access
        for doc in documents:
            # Use 'path' field as the key
            file_path = doc.get('path')
            if file_path:
                # Convert absolute path to relative path for consistency
                if file_path.startswith('/Volumes/Jeff_2TB/organized_salesforce_v2/'):
                    relative_path = file_path.replace('/Volumes/Jeff_2TB/organized_salesforce_v2/', '')
                else:
                    relative_path = file_path
                
                self._enhanced_metadata[relative_path] = doc
        
        self.logger.info(f"✅ Loaded enhanced metadata for {len(self._enhanced_metadata):,} documents")
        
        # Log optimized field count
        self.logger.info(f"📊 Each document contains 27 optimized metadata fields (deduplicated from 44 original fields)")
    
    def list_documents(self, folder_path: str = "", 
                      file_types: Optional[List[str]] = None,
                      batch_size: Optional[int] = None) -> Generator[FileMetadata, None, None]:
        """List documents from enhanced metadata"""
        
        processed = 0
        for relative_path, doc_data in self._enhanced_metadata.items():
            # Filter by folder if specified
            if folder_path and not relative_path.startswith(folder_path):
                continue
            
            # Get file info
            file_info = doc_data.get('file_info', {})
            file_ext = file_info.get('file_type', '')
            
            # Filter by file type if specified
            if file_types and file_ext not in file_types:
                continue
            
            # Create FileMetadata from enhanced data
            file_metadata = FileMetadata(
                path=relative_path,
                name=doc_data.get('name', Path(relative_path).name),
                size=file_info.get('size', 0),
                modified_time=file_info.get('modified_time', ''),
                file_type=file_ext,
                source_id=doc_data.get('salesforce_id', ''),
                source_type='salesforce_enhanced',
                full_source_path=str(self.organized_files_dir / relative_path),
                content_hash=None,  # Not available in this dataset
                is_downloadable=True
            )
            
            yield file_metadata
            
            processed += 1
            if processed % 5000 == 0:
                self.logger.info(f"📈 Listed {processed:,} documents...")
    
    def list_documents_as_metadata(self, folder_path: str = "") -> Generator[DocumentMetadata, None, None]:
        """List documents with optimized enhanced metadata (27 fields, no duplicates)"""
        
        for file_metadata in self.list_documents(folder_path):
            # Get all enhanced data for this document
            doc_data = self._enhanced_metadata.get(file_metadata.path, {})
            
            # Extract nested data - using source of truth approach
            file_info = doc_data.get('file_info', {})
            deal_metadata = doc_data.get('deal_metadata', {})
            metadata = doc_data.get('metadata', {})
            
            # Create DocumentMetadata with OPTIMIZED fields (only valid DocumentMetadata fields)
            doc_metadata = DocumentMetadata(
                # Required fields
                path=file_metadata.path,
                name=doc_data.get('name', file_metadata.name),
                size=file_info.get('size', file_metadata.size),
                size_mb=file_info.get('size_mb', 0.0),
                file_type=file_info.get('file_type', file_metadata.file_type),
                modified_time=file_info.get('modified_time', file_metadata.modified_time),
                
                # Additional file fields
                full_path=file_metadata.full_source_path,
                content_hash=file_metadata.content_hash,
                is_downloadable=file_metadata.is_downloadable,
                
                # Salesforce fields
                salesforce_content_version_id=doc_data.get('salesforce_id'),
                
                # Deal metadata fields (only fields that exist in DocumentMetadata)
                deal_id=deal_metadata.get('deal_id'),
                deal_subject=deal_metadata.get('subject'),
                deal_name=deal_metadata.get('deal_name'),
                deal_status=deal_metadata.get('status'),
                deal_reason=deal_metadata.get('deal_reason'),
                deal_start_date=deal_metadata.get('created_date'),
                client_id=deal_metadata.get('client_id'),
                
                # Financial data
                proposed_amount=self._safe_float(deal_metadata.get('total_proposed_amount')),
                final_amount=self._safe_float(deal_metadata.get('total_final_amount')),
                savings_1yr=self._safe_float(deal_metadata.get('total_savings_1yr')),
                savings_3yr=self._safe_float(deal_metadata.get('total_savings_3yr')),
                savings_target=self._safe_float(deal_metadata.get('npi_savings_target')),
                savings_achieved=deal_metadata.get('savings_achieved'),
                
                # Narrative content
                current_narrative=deal_metadata.get('current_narrative'),
                customer_comments=deal_metadata.get('customer_comments'),
                
                # LLM classification fields
                document_type=metadata.get('document_type'),
                document_type_confidence=metadata.get('document_type_confidence', 0.0),
                classification_method=metadata.get('classification_method'),
                classification_reasoning=metadata.get('classification_reasoning'),
                content_summary=metadata.get('content_summary'),
                
                # Commercial analysis
                commercial_terms_depth=metadata.get('commercial_terms_depth'),
                product_pricing_depth=metadata.get('product_pricing_depth'),
                
                # Topics and products
                key_topics=metadata.get('key_topics', []),
                vendor_products_mentioned=metadata.get('vendor_products_mentioned', []),
                pricing_indicators=metadata.get('pricing_indicators', [])
            )
            
            # Add enhancement status
            doc_metadata.mapping_status = "enhanced_optimized"
            doc_metadata.mapping_method = "pre_enriched_json_deduplicated"
            
            yield doc_metadata
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float, handling strings and None"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    

    
    def download_file(self, file_path: str) -> bytes:
        """Download file content from organized files directory"""
        full_path = self.organized_files_dir / file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        
        with open(full_path, 'rb') as f:
            return f.read()
    
    def download_document(self, file_path: str) -> bytes:
        """Download document content (compatibility method for DocumentProcessor)"""
        return self.download_file(file_path)
    
    def validate_connection(self) -> bool:
        """Validate that both the files directory and JSON exist"""
        return self.organized_files_dir.exists() and Path(self.enhanced_json_path).exists()
    
    def get_file_content_hash(self, file_path: str) -> str:
        """Get file content hash (not available in enhanced JSON, compute on demand)"""
        import hashlib
        content = self.download_file(file_path)
        return hashlib.sha256(content).hexdigest()
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists in organized files directory"""
        return (self.organized_files_dir / file_path).exists()
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get information about this enhanced file source"""
        return {
            'source_type': 'salesforce_enhanced_optimized',
            'files_directory': str(self.organized_files_dir),
            'json_metadata_file': self.enhanced_json_path,
            'total_documents': len(self._enhanced_metadata),
            'fields_per_document': 27,
            'original_fields': 44,
            'optimization': 'deduplicated_38.6%_reduction',
            'enhancement_status': 'pre_enriched_optimized'
        }
