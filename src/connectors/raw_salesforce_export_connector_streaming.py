#!/usr/bin/env python3
"""
Memory-Efficient Raw Salesforce Export Connector (Streaming Version)

This is a drop-in replacement for RawSalesforceExportConnector that:
- Uses lazy/streaming CSV loading to avoid loading 100MB+ files into memory
- Creates quick lookup indexes for fast access
- Processes data in chunks rather than loading entire files

This version is optimized for large exports and merged financial data CSVs.
"""

import os
import csv
import sqlite3
from pathlib import Path
from typing import Generator, Dict, Optional, List, Any
from datetime import datetime
import logging
import hashlib

from .file_source_interface import FileSourceInterface, FileMetadata
try:
    from models.document_models import DocumentMetadata
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from models.document_models import DocumentMetadata


class StreamingDealMetadataDB:
    """Lightweight SQLite-backed deal metadata lookup (memory-efficient)."""
    
    def __init__(self, csv_path: str, db_path: str = ":memory:"):
        """Initialize with CSV path, optionally using disk-backed SQLite."""
        self.csv_path = csv_path
        self.db_path = db_path
        self.conn = None
        self._build_index()
    
    def _build_index(self):
        """Build SQLite index from CSV (streaming to avoid memory issues)."""
        print(f"  📦 Building deal metadata index from {Path(self.csv_path).name}...")
        
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Read header and create table dynamically
        with open(self.csv_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Could not read CSV header")
            
            # Create table with appropriate column types
            columns = []
            for field in reader.fieldnames:
                columns.append(f'"{field}" TEXT')
            
            create_sql = f"CREATE TABLE deals ({', '.join(columns)})"
            cursor.execute(create_sql)
            
            # Stream rows into database (much more efficient than memory dict)
            batch = []
            batch_size = 1000
            
            for i, row in enumerate(reader):
                batch.append(tuple(row.get(field, '') for field in reader.fieldnames))
                
                if len(batch) >= batch_size:
                    placeholders = ','.join(['?' for _ in reader.fieldnames])
                    insert_sql = f"INSERT INTO deals VALUES ({placeholders})"
                    cursor.executemany(insert_sql, batch)
                    batch = []
                    
                    if (i + 1) % 10000 == 0:
                        print(f"    Indexed {i + 1} deals...", end='\r')
            
            # Flush remaining rows
            if batch:
                placeholders = ','.join(['?' for _ in reader.fieldnames])
                insert_sql = f"INSERT INTO deals VALUES ({placeholders})"
                cursor.executemany(insert_sql, batch)
        
        # Create index on Id for fast lookups
        cursor.execute("CREATE INDEX idx_deal_id ON deals(Id)")
        self.conn.commit()
        
        # Count total deals
        cursor.execute("SELECT COUNT(*) FROM deals")
        count = cursor.fetchone()[0]
        print(f"    ✓ Indexed {count} deals")
    
    def get_deal(self, deal_id: str) -> Optional[Dict[str, str]]:
        """Get single deal by ID (lazy lookup, no memory overhead)."""
        if not self.conn:
            return None
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM deals WHERE Id = ?", (deal_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        # Get column names
        cursor.execute("PRAGMA table_info(deals)")
        columns = [col[1] for col in cursor.fetchall()]
        
        return dict(zip(columns, row))
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


class RawSalesforceExportConnectorStreaming(FileSourceInterface):
    """Memory-efficient file source for raw Salesforce exports using streaming."""
    
    def __init__(self,
                 export_root_dir: str,
                 content_versions_csv: str,
                 content_documents_csv: str,
                 content_document_links_csv: str,
                 deal_metadata_csv: str,
                 client_mapping_csv: Optional[str] = None,
                 vendor_mapping_csv: Optional[str] = None,
                 deal_mapping_csv: Optional[str] = None):
        """Initialize streaming connector with large CSV support."""
        super().__init__()
        
        self.export_root_dir = Path(export_root_dir)
        self.content_versions_csv = content_versions_csv
        self.content_documents_csv = content_documents_csv
        self.content_document_links_csv = content_document_links_csv
        self.deal_metadata_csv = deal_metadata_csv
        self.client_mapping_csv = client_mapping_csv
        self.vendor_mapping_csv = vendor_mapping_csv
        self.deal_mapping_csv = deal_mapping_csv
        
        # Use disk-backed SQLite for large CSVs
        db_path = f"{deal_metadata_csv}.deals.db"
        self.deal_db = StreamingDealMetadataDB(deal_metadata_csv, db_path)
        
        # Keep smaller files in memory
        self._content_versions: Optional[Dict] = None
        self._content_documents: Optional[Dict] = None
        self._content_document_links: Optional[Dict] = None
        self._client_mapping: Optional[Dict] = None
        self._vendor_mapping: Optional[Dict] = None
        self._cv_to_deal_mapping: Optional[Dict] = None
        
        self.logger = logging.getLogger(__name__)
        
        # Validate paths
        self._validate_paths()
        
        # Load smaller files (content versions/documents/links)
        self._load_small_files()
    
    def _validate_paths(self):
        """Validate required paths exist."""
        if not self.export_root_dir.exists():
            raise FileNotFoundError(f"Export root directory not found: {self.export_root_dir}")
        
        for f in [self.content_versions_csv, self.content_documents_csv, 
                  self.content_document_links_csv, self.deal_metadata_csv]:
            if not Path(f).exists():
                raise FileNotFoundError(f"Required file not found: {f}")
    
    def _load_small_files(self):
        """Load smaller metadata files into memory (content versions, documents, links)."""
        self.logger.info("Loading content metadata files...")
        
        import pandas as pd
        
        # Load content versions (usually < 50MB)
        self.logger.info(f"Loading ContentVersions from {self.content_versions_csv}")
        cv_df = pd.read_csv(self.content_versions_csv, encoding='utf-8-sig', low_memory=False, dtype={'Id': str})
        self._content_versions = {}
        
        for _, row in cv_df.iterrows():
            cv_id = str(row['Id'])
            is_latest = row.get('IsLatest', False)
            if not (is_latest == True or (isinstance(is_latest, str) and is_latest.lower() == 'true')):
                continue
            
            deal_c = row.get('Deal__c', '')
            if pd.isna(deal_c):
                deal_c = None
            elif deal_c:
                deal_c = str(deal_c).strip()
            
            self._content_versions[cv_id] = {
                'content_document_id': str(row.get('ContentDocumentId', '')),
                'title': row.get('Title', ''),
                'path_on_client': row.get('PathOnClient', ''),
                'file_type': row.get('FileType', ''),
                'file_extension': row.get('FileExtension', ''),
                'content_size': int(row.get('ContentSize', 0)) if pd.notna(row.get('ContentSize')) else 0,
                'deal_id': deal_c,
                'content_modified_date': str(row.get('ContentModifiedDate', '')),
                'created_date': str(row.get('CreatedDate', '')),
                'is_deleted': str(row.get('IsDeleted', False)).lower() == 'true'
            }
        
        self.logger.info(f"Loaded {len(self._content_versions)} ContentVersion records")
        
        # Load content documents
        self.logger.info(f"Loading ContentDocuments from {self.content_documents_csv}")
        cd_df = pd.read_csv(self.content_documents_csv, encoding='utf-8-sig', low_memory=False, dtype={'Id': str})
        self._content_documents = {}
        
        for _, row in cd_df.iterrows():
            doc_id = str(row['Id'])
            self._content_documents[doc_id] = {
                'title': row.get('Title', ''),
                'file_type': row.get('FileType', ''),
                'file_extension': row.get('FileExtension', ''),
                'content_size': int(row.get('ContentSize', 0)) if pd.notna(row.get('ContentSize')) else 0,
                'created_date': str(row.get('CreatedDate', ''))
            }
        
        self.logger.info(f"Loaded {len(self._content_documents)} ContentDocument records")
        
        # Load content document links
        self.logger.info(f"Loading ContentDocumentLinks from {self.content_document_links_csv}")
        cdl_df = pd.read_csv(self.content_document_links_csv, encoding='utf-8-sig', low_memory=False, dtype={'Id': str})
        self._content_document_links = {}
        
        for _, row in cdl_df.iterrows():
            doc_id = str(row['ContentDocumentId'])
            linked_entity = str(row['LinkedEntityId']).strip() if pd.notna(row.get('LinkedEntityId')) else ''
            
            if linked_entity.startswith('a0W'):  # Deal ID prefix
                if doc_id not in self._content_document_links:
                    self._content_document_links[doc_id] = []
                self._content_document_links[doc_id].append(linked_entity)
        
        self.logger.info(f"Loaded {len(self._content_document_links)} ContentDocument to Deal links")
        
        # Build CV → Deal mapping
        self._build_cv_to_deal_mapping()
    
    def _build_cv_to_deal_mapping(self):
        """Build ContentVersion → Deal mapping."""
        self._cv_to_deal_mapping = {}
        for cv_id, cv_data in self._content_versions.items():
            deal_id = cv_data.get('deal_id')
            if deal_id:
                self._cv_to_deal_mapping[cv_id] = deal_id
        
        self.logger.info(f"Built ContentVersion → Deal mapping ({len(self._cv_to_deal_mapping)} links)")
    
    def _to_str_or_none(self, value) -> Optional[str]:
        """Safely convert value to string or None."""
        if value is None or (isinstance(value, float) and value != value):  # NaN check
            return None
        if isinstance(value, str):
            return value.strip() if value else None
        return str(value).strip() if value else None
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float or None."""
        if value is None or value == '':
            return None
        if isinstance(value, float):
            return None if value != value else value  # NaN check
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def list_documents_as_metadata(self, 
                                   require_deal_association: bool = False) -> Generator[DocumentMetadata, None, None]:
        """Stream documents with metadata (memory-efficient)."""
        self.logger.info(f"Streaming documents (require_deal: {require_deal_association})...")
        
        doc_count = 0
        content_version_count = 0
        
        for cv_id, cv_data in self._content_versions.items():
            content_version_count += 1
            
            # Get deal ID from CV or CDL
            deal_id = cv_data.get('deal_id')
            if not deal_id:
                content_doc_id = cv_data.get('content_document_id')
                if content_doc_id in self._content_document_links:
                    deal_ids = self._content_document_links[content_doc_id]
                    if deal_ids:
                        deal_id = deal_ids[0]
            
            # Skip if no deal and filtering enabled
            if require_deal_association and not deal_id:
                continue
            
            # Look up deal metadata from database (lazy lookup)
            deal_data = None
            if deal_id:
                deal_data = self.deal_db.get_deal(deal_id)
            
            if require_deal_association and not deal_data:
                continue
            
            # Build document metadata
            doc_metadata = self._build_document_metadata(cv_id, cv_data, deal_id, deal_data)
            
            if doc_metadata:
                doc_count += 1
                yield doc_metadata
        
        self.logger.info(f"Streamed {doc_count} documents from {content_version_count} content versions")
    
    def _build_document_metadata(self, cv_id: str, cv_data: Dict, 
                                 deal_id: Optional[str], deal_data: Optional[Dict]) -> Optional[DocumentMetadata]:
        """Build DocumentMetadata from content version and deal data."""
        try:
            # File path resolution
            full_path = self._resolve_file_path(cv_id, cv_data, deal_id)
            if not full_path:
                return None
            
            # Extract deal metadata
            deal_name = deal_data.get('Name', '') if deal_data else ''
            
            doc = DocumentMetadata(
                document_id=cv_id,
                file_name=cv_data.get('title', ''),
                file_type=cv_data.get('file_type', ''),
                file_extension=cv_data.get('file_extension', ''),
                file_size_bytes=cv_data.get('content_size', 0),
                full_path=full_path,
                deal_id=deal_name,  # Use friendly "Deal-36801" format from Name field
                salesforce_deal_id=deal_id,  # Raw Salesforce ID (a0W...)
                salesforce_client_id=self._to_str_or_none(deal_data.get('Client__c')) if deal_data else None,
                salesforce_vendor_id=self._to_str_or_none(deal_data.get('Primary_Deal_Vendor__c')) if deal_data else None,
                deal_name=deal_name,
                deal_creation_date=deal_data.get('CreatedDate', '') if deal_data else '',
                proposed_amount=self._safe_float(deal_data.get('Total_Proposed_Amount__c')) if deal_data else None,
                # Prefer new merged fields, fallback to old field names
                final_amount=(
                    self._safe_float(deal_data.get('Total_Final_Amount_Year_1__c')) or
                    self._safe_float(deal_data.get('Total_Final_Amount__c'))
                ) if deal_data else None,
                savings_1yr=(
                    self._safe_float(deal_data.get('Actual_Savings_Year_1__c')) or
                    self._safe_float(deal_data.get('Total_Savings_1yr__c'))
                ) if deal_data else None,
                savings_3yr=self._safe_float(deal_data.get('Total_Savings_3yr__c')) if deal_data else None,
                savings_target=(
                    self._safe_float(deal_data.get('Initial_Quote_Year_1__c')) or
                    self._safe_float(deal_data.get('NPI_Savings_Target__c'))
                ) if deal_data else None,
                savings_target_full_term=(
                    self._safe_float(deal_data.get('Actual_Savings_Full_Contract_Term__c')) or
                    self._safe_float(deal_data.get('NPI_Savings_Target_Full_Contract_Term__c'))
                ) if deal_data else None,
            )
            
            return doc
        
        except Exception as e:
            self.logger.warning(f"Failed to build metadata for {cv_id}: {e}")
            return None
    
    def _resolve_file_path(self, cv_id: str, cv_data: Dict, deal_id: Optional[str]) -> Optional[str]:
        """Resolve file path from export directory."""
        # Primary location: ContentVersions/VersionData/<cv_id>/
        primary_path = self.export_root_dir / "ContentVersions" / "VersionData" / cv_id
        if primary_path.exists() and any(primary_path.iterdir()):
            return str(primary_path.relative_to(self.export_root_dir))
        
        # Fallback: Attachments/Body/<content_doc_id>/
        content_doc_id = cv_data.get('content_document_id')
        if content_doc_id:
            alt_path = self.export_root_dir / "Attachments" / "Body" / content_doc_id
            if alt_path.exists() and any(alt_path.iterdir()):
                return str(alt_path.relative_to(self.export_root_dir))
        
        return None
    
    def print_export_statistics(self):
        """Print export statistics."""
        print(f"\n📊 Export Statistics:")
        print(f"  Content Versions: {len(self._content_versions)}")
        print(f"  Content Documents: {len(self._content_documents)}")
        print(f"  CV→Deal Mappings: {len(self._cv_to_deal_mapping)}")
    
    def __del__(self):
        """Cleanup database on deletion."""
        if hasattr(self, 'deal_db'):
            self.deal_db.close()




