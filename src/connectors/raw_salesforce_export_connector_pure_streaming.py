#!/usr/bin/env python3
"""
Pure Streaming Raw Salesforce Export Connector

This version uses NO pandas and streams everything from CSV files.
Avoids all memory crashes by never loading more than necessary into RAM.
"""

import os
import csv
import sqlite3
from pathlib import Path
from typing import Generator, Dict, Optional, List, Any
from datetime import datetime
import logging

from .file_source_interface import FileSourceInterface, FileMetadata
try:
    from models.document_models import DocumentMetadata
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from models.document_models import DocumentMetadata


class PureStreamingConnector(FileSourceInterface):
    """100% streaming connector - no pandas, no large memory allocations."""
    
    def __init__(self,
                 export_root_dir: str,
                 content_versions_csv: str,
                 content_documents_csv: str,
                 content_document_links_csv: str,
                 deal_metadata_csv: str,
                 client_mapping_csv: Optional[str] = None,
                 vendor_mapping_csv: Optional[str] = None,
                 deal_mapping_csv: Optional[str] = None):
        """Initialize with pure streaming (no pandas)."""
        super().__init__()
        
        self.export_root_dir = Path(export_root_dir)
        self.content_versions_csv = content_versions_csv
        self.content_documents_csv = content_documents_csv
        self.content_document_links_csv = content_document_links_csv
        self.deal_metadata_csv = deal_metadata_csv
        self.client_mapping_csv = client_mapping_csv
        self.vendor_mapping_csv = vendor_mapping_csv
        self.deal_mapping_csv = deal_mapping_csv
        
        self.logger = logging.getLogger(__name__)
        
        # Index deals in SQLite (disk-backed, no memory overhead)
        db_path = f"{deal_metadata_csv}.deals.db"
        if Path(db_path).exists():
            Path(db_path).unlink()  # Clear old DB
        
        self.deal_db = sqlite3.connect(db_path)
        self._index_deals_in_sqlite()
        
        # Build in-memory indexes for smaller files (streaming)
        self._cv_index = {}  # ContentVersion ID → data
        self._cd_index = {}  # ContentDocument ID → data
        self._cd_to_deal = {}  # ContentDocument ID → Deal IDs
        
        self._validate_paths()
        self._build_indexes()
    
    def _validate_paths(self):
        """Validate paths exist."""
        if not self.export_root_dir.exists():
            raise FileNotFoundError(f"Export root not found: {self.export_root_dir}")
        
        for f in [self.content_versions_csv, self.content_documents_csv,
                  self.content_document_links_csv, self.deal_metadata_csv]:
            if not Path(f).exists():
                raise FileNotFoundError(f"File not found: {f}")
    
    def _index_deals_in_sqlite(self):
        """Build SQLite index from deal CSV (streaming, no memory overhead)."""
        print(f"  📦 Indexing deals from {Path(self.deal_metadata_csv).name}...")
        
        cursor = self.deal_db.cursor()
        
        # Read header and create table
        with open(self.deal_metadata_csv, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Could not read CSV header")
            
            # Create table
            columns = [f'"{field}" TEXT' for field in reader.fieldnames]
            cursor.execute(f"CREATE TABLE deals ({', '.join(columns)})")
            
            # Stream rows (1000 at a time)
            batch = []
            batch_size = 1000
            
            for i, row in enumerate(reader):
                batch.append(tuple(row.get(field, '') for field in reader.fieldnames))
                
                if len(batch) >= batch_size:
                    placeholders = ','.join(['?' for _ in reader.fieldnames])
                    sql = f"INSERT INTO deals VALUES ({placeholders})"
                    cursor.executemany(sql, batch)
                    batch = []
                    
                    if (i + 1) % 10000 == 0:
                        print(f"    Indexed {i + 1} deals...", end='\r')
            
            # Flush remaining
            if batch:
                placeholders = ','.join(['?' for _ in reader.fieldnames])
                sql = f"INSERT INTO deals VALUES ({placeholders})"
                cursor.executemany(sql, batch)
        
        # Create index
        cursor.execute("CREATE INDEX idx_deal_id ON deals(Id)")
        self.deal_db.commit()
        
        # Count
        cursor.execute("SELECT COUNT(*) FROM deals")
        count = cursor.fetchone()[0]
        print(f"    ✓ Indexed {count} deals")
    
    def _build_indexes(self):
        """Build indexes for smaller files."""
        self.logger.info("Building content indexes...")
        
        # Index content versions
        self._index_content_versions()
        
        # Index content documents
        self._index_content_documents()
        
        # Index content document links
        self._index_content_document_links()
    
    def _index_content_versions(self):
        """Index content versions (stream from CSV)."""
        print(f"  📝 Indexing content versions...")
        count = 0
        
        with open(self.content_versions_csv, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cv_id = row.get('Id', '')
                if not cv_id:
                    continue
                
                # Only keep latest versions
                is_latest = row.get('IsLatest', 'false').lower()
                if is_latest not in ('true', '1'):
                    continue
                
                # Extract deal ID safely
                deal_c = row.get('Deal__c', '').strip() if row.get('Deal__c') else None
                if deal_c == '' or deal_c is None:
                    deal_c = None
                
                self._cv_index[cv_id] = {
                    'content_document_id': row.get('ContentDocumentId', '').strip(),
                    'title': row.get('Title', ''),
                    'path_on_client': row.get('PathOnClient', ''),
                    'file_type': row.get('FileType', ''),
                    'file_extension': row.get('FileExtension', ''),
                    'content_size': int(row.get('ContentSize', 0)) if row.get('ContentSize', '0').isdigit() else 0,
                    'deal_id': deal_c,
                    'content_modified_date': row.get('ContentModifiedDate', ''),
                    'created_date': row.get('CreatedDate', ''),
                }
                count += 1
                
                if count % 10000 == 0:
                    print(f"    Indexed {count} content versions...", end='\r')
        
        print(f"    ✓ Indexed {count} content versions")
    
    def _index_content_documents(self):
        """Index content documents."""
        print(f"  📄 Indexing content documents...")
        count = 0
        
        with open(self.content_documents_csv, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                doc_id = row.get('Id', '').strip()
                if not doc_id:
                    continue
                
                self._cd_index[doc_id] = {
                    'title': row.get('Title', ''),
                    'file_type': row.get('FileType', ''),
                    'file_extension': row.get('FileExtension', ''),
                    'content_size': int(row.get('ContentSize', 0)) if row.get('ContentSize', '0').isdigit() else 0,
                    'created_date': row.get('CreatedDate', ''),
                }
                count += 1
        
        print(f"    ✓ Indexed {count} content documents")
    
    def _index_content_document_links(self):
        """Index content document to deal links."""
        print(f"  🔗 Indexing document links...")
        count = 0
        
        with open(self.content_document_links_csv, 'r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                doc_id = row.get('ContentDocumentId', '').strip()
                entity_id = row.get('LinkedEntityId', '').strip()
                
                if not doc_id or not entity_id:
                    continue
                
                # Only track deal links (start with 'a0W')
                if entity_id.startswith('a0W'):
                    if doc_id not in self._cd_to_deal:
                        self._cd_to_deal[doc_id] = []
                    self._cd_to_deal[doc_id].append(entity_id)
                    count += 1
        
        print(f"    ✓ Indexed {count} document-to-deal links")
    
    def _get_deal_data(self, deal_id: str) -> Optional[Dict[str, str]]:
        """Get deal data from SQLite index."""
        cursor = self.deal_db.cursor()
        cursor.execute("SELECT * FROM deals WHERE Id = ?", (deal_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        # Get column names
        cursor.execute("PRAGMA table_info(deals)")
        columns = [col[1] for col in cursor.fetchall()]
        
        return dict(zip(columns, row))
    
    def _safe_str(self, val) -> Optional[str]:
        """Safely convert to string."""
        if not val or val == '':
            return None
        return str(val).strip()
    
    def _safe_float(self, val) -> Optional[float]:
        """Safely convert to float."""
        if not val or val == '':
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    
    def list_documents_as_metadata(self,
                                   require_deal_association: bool = False) -> Generator[DocumentMetadata, None, None]:
        """Stream documents with metadata."""
        print(f"  📋 Streaming documents (require_deal: {require_deal_association})...")
        
        doc_count = 0
        
        for cv_id, cv_data in self._cv_index.items():
            # Get deal ID
            deal_id = cv_data.get('deal_id')
            if not deal_id:
                cd_id = cv_data.get('content_document_id')
                if cd_id in self._cd_to_deal:
                    deal_id = self._cd_to_deal[cd_id][0]
            
            # Skip if filtering and no deal
            if require_deal_association and not deal_id:
                continue
            
            # Get deal data from SQLite
            deal_data = None
            if deal_id:
                deal_data = self._get_deal_data(deal_id)
            
            if require_deal_association and not deal_data:
                continue
            
            # Build metadata
            doc = self._build_metadata(cv_id, cv_data, deal_id, deal_data)
            if doc:
                doc_count += 1
                yield doc
        
        print(f"    ✓ Streamed {doc_count} documents")
    
    def _build_metadata(self, cv_id: str, cv_data: Dict,
                       deal_id: Optional[str], deal_data: Optional[Dict]) -> Optional[DocumentMetadata]:
        """Build DocumentMetadata with correct field names."""
        try:
            # Resolve file path
            full_path = self._resolve_path(cv_id, cv_data, deal_id)
            if not full_path:
                return None
            
            # Extract title safely
            title = cv_data.get('title', '')
            file_ext = cv_data.get('file_extension', '')
            
            doc = DocumentMetadata(
                path=full_path,
                name=title,
                size=cv_data.get('content_size', 0),
                size_mb=cv_data.get('content_size', 0) / (1024 * 1024),
                file_type=cv_data.get('file_type', ''),
                modified_time=cv_data.get('content_modified_date', ''),
                deal_creation_date=deal_data.get('CreatedDate', '') if deal_data else None,
                deal_name=deal_data.get('Name', '') if deal_data else None,
                full_path=full_path,
                # Salesforce metadata
                salesforce_deal_id=deal_id,
                salesforce_client_id=self._safe_str(deal_data.get('Client__c')) if deal_data else None,
                salesforce_vendor_id=self._safe_str(deal_data.get('Primary_Deal_Vendor__c')) if deal_data else None,
                # Financial data
                proposed_amount=self._safe_float(deal_data.get('Total_Proposed_Amount__c')) if deal_data else None,
                final_amount=self._safe_float(deal_data.get('Total_Final_Amount__c')) if deal_data else None,
                savings_1yr=self._safe_float(deal_data.get('Total_Savings_1yr__c')) if deal_data else None,
                savings_3yr=self._safe_float(deal_data.get('Total_Savings_3yr__c')) if deal_data else None,
                savings_target=self._safe_float(deal_data.get('NPI_Savings_Target__c')) if deal_data else None,
                savings_target_full_term=self._safe_float(deal_data.get('NPI_Savings_Target_Full_Contract_Term__c')) if deal_data else None,
                final_amount_full_term=self._safe_float(deal_data.get('Final_Amount_Full_Contract_Term__c')) if deal_data else None,
            )
            return doc
        
        except Exception as e:
            self.logger.warning(f"Failed to build metadata for {cv_id}: {e}")
            return None
    
    def _resolve_path(self, cv_id: str, cv_data: Dict, deal_id: Optional[str]) -> Optional[str]:
        """Resolve file path."""
        # Primary
        primary = self.export_root_dir / "ContentVersions" / "VersionData" / cv_id
        if primary.exists() and any(primary.iterdir()):
            return str(primary.relative_to(self.export_root_dir))
        
        # Fallback
        cd_id = cv_data.get('content_document_id')
        if cd_id:
            alt = self.export_root_dir / "Attachments" / "Body" / cd_id
            if alt.exists() and any(alt.iterdir()):
                return str(alt.relative_to(self.export_root_dir))
        
        return None
    
    def print_export_statistics(self):
        """Print statistics."""
        print(f"\n📊 Export Statistics:")
        print(f"  Content Versions: {len(self._cv_index)}")
        print(f"  Content Documents: {len(self._cd_index)}")
        print(f"  Document→Deal Links: {len(self._cd_to_deal)}")
    
    # ========== Required abstract methods from FileSourceInterface ==========
    
    def list_documents(self, folder_path: str = "/", recursive: bool = True) -> Generator[FileMetadata, None, None]:
        """List documents (compatibility method, use list_documents_as_metadata instead)."""
        for doc in self.list_documents_as_metadata(require_deal_association=True):
            yield FileMetadata(
                name=doc.file_name,
                path=doc.full_path or "",
                size_bytes=doc.file_size_bytes or 0,
                modified_date=datetime.now().isoformat()
            )
    
    def download_file(self, file_path: str) -> bytes:
        """Download file content."""
        abs_path = self.export_root_dir / file_path if not Path(file_path).is_absolute() else Path(file_path)
        with open(abs_path, 'rb') as f:
            return f.read()
    
    def validate_connection(self) -> bool:
        """Validate connection (always true for local export)."""
        return self.export_root_dir.exists()
    
    def get_file_content_hash(self, file_path: str) -> str:
        """Get file content hash."""
        import hashlib
        content = self.download_file(file_path)
        return hashlib.sha256(content).hexdigest()
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists."""
        abs_path = self.export_root_dir / file_path if not Path(file_path).is_absolute() else Path(file_path)
        return abs_path.exists()
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get source information."""
        return {
            'source_type': 'raw_salesforce_export',
            'export_root': str(self.export_root_dir),
            'content_versions': len(self._cv_index),
            'content_documents': len(self._cd_index),
            'links': len(self._cd_to_deal),
            'mode': 'pure_streaming'
        }
    
    def __del__(self):
        """Cleanup."""
        if hasattr(self, 'deal_db'):
            self.deal_db.close()

