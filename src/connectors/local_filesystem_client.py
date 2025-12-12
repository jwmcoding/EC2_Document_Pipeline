"""
Local Filesystem Client for Document Processing Pipeline

This module provides local filesystem access that mirrors DropboxClient functionality,
implementing the FileSourceInterface for seamless source switching.
"""

import os
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Generator, Any
import logging
from dataclasses import dataclass

# Import the interface and shared components
from .file_source_interface import FileSourceInterface, FileMetadata, FileSourceError

# Import business metadata extraction (reuse from DropboxClient)
try:
    from .dropbox_client import BusinessMetadataExtractor, DocumentMetadata
except ImportError:
    # Fallback for direct execution
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from connectors.dropbox_client import BusinessMetadataExtractor, DocumentMetadata

# Import LLM classifier (optional)
try:
    from classification.llm_document_classifier import LLMDocumentClassifier
except ImportError:
    LLMDocumentClassifier = None


class LocalFilesystemClient(FileSourceInterface):
    """Local filesystem implementation of FileSourceInterface"""
    
    def __init__(self, base_path: str, openai_api_key: Optional[str] = None):
        """
        Initialize local filesystem client.
        
        Args:
            base_path: Root directory for document discovery
            openai_api_key: Optional OpenAI API key for LLM classification
        """
        super().__init__()
        self.base_path = Path(base_path).resolve()
        self.openai_api_key = openai_api_key
        
        # Reuse business metadata extractor from DropboxClient
        self.metadata_extractor = BusinessMetadataExtractor()
        
        # Initialize LLM classifier if API key provided and module available (same as DropboxClient)
        self.llm_classifier = None
        if self.openai_api_key and LLMDocumentClassifier:
            try:
                self.llm_classifier = LLMDocumentClassifier(self.openai_api_key)
                self.logger.info("✅ Initialized LLM document classifier with GPT-4.1-mini")
            except Exception as e:
                self.logger.error(f"❌ Failed to initialize LLM classifier: {e}")
        elif self.openai_api_key and not LLMDocumentClassifier:
            self.logger.warning("⚠️ OpenAI API key provided but LLM classifier module not available")
        else:
            self.logger.info("ℹ️ LLM document classification disabled (no API key provided)")
        
        self.logger.info(f"📁 LocalFilesystemClient initialized with base path: {self.base_path}")
    
        # Smart detection: Warn if Salesforce export structure detected
        self._check_for_salesforce_structure()
    
    def _check_for_salesforce_structure(self):
        """Detect and warn if this appears to be a Salesforce export directory."""
        salesforce_indicators = [
            self.base_path / 'content_versions.csv',
            self.base_path / 'content_document_links.csv',
            self.base_path / 'deal__cs.csv',
            self.base_path / 'ContentVersions'
        ]
        
        detected_indicators = [ind for ind in salesforce_indicators if ind.exists()]
        
        # Warn if 3+ indicators present (high confidence this is a Salesforce export)
        if len(detected_indicators) >= 3:
            self.logger.warning("\n" + "="*80)
            self.logger.warning("⚠️  SALESFORCE EXPORT DETECTED!")
            self.logger.warning("="*80)
            self.logger.warning("This directory contains Salesforce CSV files and structure:")
            for indicator in detected_indicators:
                self.logger.warning(f"  ✅ {indicator.name}")
            self.logger.warning("")
            self.logger.warning("⚠️  Using --source local will result in NULL METADATA:")
            self.logger.warning("  ❌ deal_id, client_id, vendor_id = NULL")
            self.logger.warning("  ❌ Financial data (final_amount, savings) = NULL")
            self.logger.warning("  ❌ Contract dates and terms = NULL")
            self.logger.warning("")
            self.logger.warning("✅ RECOMMENDED: Use --source salesforce_raw for full enrichment:")
            self.logger.warning("")
            self.logger.warning("  python discover_documents.py --source salesforce_raw \\")
            self.logger.warning(f"    --export-root-dir \"{self.base_path}\" \\")
            self.logger.warning(f"    --content-versions-csv \"{self.base_path}/content_versions.csv\" \\")
            self.logger.warning(f"    --content-documents-csv \"{self.base_path}/content_documents.csv\" \\")
            self.logger.warning(f"    --content-document-links-csv \"{self.base_path}/content_document_links.csv\" \\")
            self.logger.warning(f"    --deal-metadata-csv \"{self.base_path}/deal__cs.csv\" \\")
            self.logger.warning("    --require-deal-association \\")
            self.logger.warning("    --output salesforce_discovery.json")
            self.logger.warning("")
            self.logger.warning("See README.md for complete details.")
            self.logger.warning("="*80 + "\n")
    
    def validate_connection(self) -> bool:
        """Validate that base path exists and is accessible"""
        try:
            if not self.base_path.exists():
                raise FileSourceError(f"Base path does not exist: {self.base_path}")
            
            if not self.base_path.is_dir():
                raise FileSourceError(f"Base path is not a directory: {self.base_path}")
            
            # Test read access
            list(self.base_path.iterdir())
            
            self.logger.info("✅ Local filesystem connection validated")
            return True
            
        except PermissionError as e:
            raise FileSourceError(f"Permission denied accessing base path: {e}")
        except Exception as e:
            raise FileSourceError(f"Failed to validate filesystem access: {e}")
    
    def list_documents(self, folder_path: str = "", 
                      file_types: Optional[List[str]] = None,
                      batch_size: Optional[int] = None) -> Generator[FileMetadata, None, None]:
        """
        List all documents in the specified path.
        
        Args:
            folder_path: Relative path from base_path (empty string for root)
            file_types: Optional list of file extensions to filter
            batch_size: Not used in generator mode
            
        Yields:
            FileMetadata objects for each discovered document
        """
        # Resolve full path
        if folder_path:
            search_path = self.base_path / folder_path
        else:
            search_path = self.base_path
        
        if not search_path.exists():
            self.logger.error(f"Path does not exist: {search_path}")
            return
        
        self.logger.info(f"🔍 Scanning local directory: {search_path}")
        
        # Walk directory tree
        for root, dirs, files in os.walk(search_path):
            root_path = Path(root)
            
            for file_name in files:
                # Skip hidden files and system files
                if file_name.startswith('.'):
                    continue
                
                # Check file type filter
                if not self.is_supported_file_type(file_name):
                    continue
                
                if file_types:
                    ext = os.path.splitext(file_name)[1].lower()
                    if ext not in file_types:
                        continue
                
                try:
                    file_path = root_path / file_name
                    
                    # Get file metadata
                    stat = file_path.stat()
                    
                    # Calculate relative path from base
                    relative_path = file_path.relative_to(self.base_path)
                    
                    # Create FileMetadata
                    metadata = FileMetadata(
                        path=str(relative_path),
                        name=file_name,
                        size=stat.st_size,
                        modified_time=datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        file_type=os.path.splitext(file_name)[1].lower(),
                        source_id=f"local_{file_path.stat().st_ino}",  # Use inode as ID
                        source_type="local",
                        full_source_path=str(file_path),
                        is_downloadable=True
                    )
                    
                    yield metadata
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Error processing file {file_name}: {e}")
                    # Still yield metadata with error
                    yield FileMetadata(
                        path=str(root_path.relative_to(self.base_path) / file_name),
                        name=file_name,
                        size=0,
                        modified_time=datetime.now().isoformat(),
                        file_type=os.path.splitext(file_name)[1].lower(),
                        source_id="error",
                        source_type="local",
                        full_source_path=str(root_path / file_name),
                        is_downloadable=False,
                        error_message=str(e)
                    )
    
    def download_file(self, file_path: str) -> bytes:
        """Download (read) file content"""
        try:
            full_path = self.base_path / file_path
            
            if not full_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            with open(full_path, 'rb') as f:
                return f.read()
                
        except PermissionError:
            raise PermissionError(f"Permission denied reading file: {file_path}")
        except Exception as e:
            raise FileSourceError(f"Error reading file: {e}")
    
    def download_document(self, file_path: str) -> bytes:
        """Compatibility method for DocumentProcessor (same as download_file)"""
        return self.download_file(file_path)
    
    def get_file_content_hash(self, file_path: str) -> str:
        """Get SHA256 hash of file content"""
        try:
            full_path = self.base_path / file_path
            
            if not full_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            sha256_hash = hashlib.sha256()
            with open(full_path, "rb") as f:
                # Read in chunks for large files
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            
            return sha256_hash.hexdigest()
            
        except Exception as e:
            raise FileSourceError(f"Error hashing file: {e}")
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists"""
        full_path = self.base_path / file_path
        return full_path.exists() and full_path.is_file()
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get information about the file source"""
        return {
            "type": "local",
            "base_path": str(self.base_path),
            "total_size_bytes": sum(f.stat().st_size for f in self.base_path.rglob("*") if f.is_file()),
            "accessible": self.base_path.exists(),
            "llm_classification_enabled": self.llm_classifier is not None
        }
    
    def list_documents_as_metadata(self, folder_path: str = "", 
                                 classify_with_llm: bool = False,
                                 file_types: Optional[List[str]] = None) -> Generator[DocumentMetadata, None, None]:
        """
        List documents and return as DocumentMetadata objects (compatible with DropboxClient).
        
        This method provides compatibility with existing code that expects DocumentMetadata objects.
        
        Args:
            folder_path: Relative path from base_path
            classify_with_llm: Whether to classify documents using LLM (compatibility parameter, not used)
            file_types: Optional file type filter
            
        Yields:
            DocumentMetadata objects with business metadata extracted
        """
        for file_meta in self.list_documents(folder_path, file_types):
            try:
                # Extract business metadata from path (match DropboxClient structure)
                path_parts = [part.strip() for part in file_meta.path.split(os.sep) if part.strip()]
                
                # Adjust path structure to match BusinessMetadataExtractor expectations
                # Local: ["Week35-08282023", "Lenovo", "Morgan Stanley", "Deal-52766-Lenovo", "filename"]
                # Expected: ["/", "NPI Data Ownership", "YEAR Deal Docs", "WeekX-DATE", "Vendor", "Client", "Deal-X-Vendor"]
                if len(path_parts) >= 4:
                    adjusted_path_parts = [
                        "/",  # [0] - root marker
                        "NPI Data Ownership",  # [1] - organization
                        "2023 Deal Docs",  # [2] - will be updated with actual year
                        path_parts[0],  # [3] - Week35-08282023 (week info)
                        path_parts[1],  # [4] - Lenovo (vendor)
                        path_parts[2] if len(path_parts) > 2 else "",  # [5] - Morgan Stanley (client)
                        path_parts[3] if len(path_parts) > 3 else ""   # [6] - Deal-52766-Lenovo (deal info)
                    ]
                    
                    # Extract year from week info and update path_parts[2]
                    if path_parts[0]:
                        import re
                        # Look for 4-digit year pattern (2023, 2024, etc.)
                        year_match = re.search(r'(20\d{2})', path_parts[0])
                        if year_match:
                            year = year_match.group(1)
                            adjusted_path_parts[2] = f"{year} Deal Docs"
                    
                    business_metadata = self.metadata_extractor.extract_metadata(adjusted_path_parts, file_meta.path)
                else:
                    # Fallback for shorter paths
                    business_metadata = self.metadata_extractor.extract_metadata(path_parts, file_meta.path)
                
                # Create DocumentMetadata object
                doc_metadata = DocumentMetadata(
                    path=file_meta.path,
                    name=file_meta.name,
                    size=file_meta.size,
                    size_mb=file_meta.size_mb,
                    file_type=file_meta.file_type,
                    modified_time=file_meta.modified_time,
                    
                    # Business metadata
                    year=business_metadata.get('year'),
                    week_number=business_metadata.get('week_number'),
                    week_date=business_metadata.get('week_date'),
                    vendor=business_metadata.get('vendor'),
                    client=business_metadata.get('client'),
                    deal_number=business_metadata.get('deal_number'),
                    deal_name=business_metadata.get('deal_name'),
                    extraction_confidence=business_metadata.get('confidence', 0.0),
                    parsing_errors=business_metadata.get('errors', []),
                    
                    # File metadata
                    full_path=file_meta.full_source_path,
                    path_components=path_parts,
                    dropbox_id=file_meta.source_id,  # Use source_id
                    is_downloadable=file_meta.is_downloadable
                )
                
                # Note: LLM classification moved to processing phase in v3 architecture
                # Classification will be performed during document processing with full content context
                
                yield doc_metadata
                
            except Exception as e:
                self.logger.error(f"Error processing metadata for {file_meta.name}: {e}")
                # Still yield basic metadata
                yield DocumentMetadata(
                    path=file_meta.path,
                    name=file_meta.name,
                    size=file_meta.size,
                    size_mb=file_meta.size_mb,
                    file_type=file_meta.file_type,
                    modified_time=file_meta.modified_time,
                    full_path=file_meta.full_source_path,
                    dropbox_id=file_meta.source_id,
                    is_downloadable=False,
                    parsing_errors=[str(e)]
                ) 