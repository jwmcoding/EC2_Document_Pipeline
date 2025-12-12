"""
Raw Salesforce Export Connector

This module provides file source integration for raw Salesforce differential export bundles.
Unlike SalesforceFileSource which requires pre-organized files and mapping CSVs, this connector
reads directly from the raw export structure:

- CSV metadata files (content_versions.csv, content_documents.csv, content_document_links.csv, deal__cs.csv)
- File payloads in ContentVersions/VersionData/<ContentVersionId>/ directories (primary location)
- File payloads in Deal__cs/<exportId>/<DealId>/ directories (Deal-specific)
- File payloads in Attachments/Body/<ContentDocumentId> directories (legacy attachments)

Features:
- Direct CSV parsing for ContentVersion → Deal mapping
- File discovery from raw export directory structure
- Deal metadata enrichment from deal__cs.csv (or merged deal_merged_financial_data.csv)
- Client/Vendor name mapping support
- Financial metrics integration (from merged financial data)

Note: This connector supports using a merged deal CSV that combines financial data
from older exports with newer deal records. See merge_deal_financial_data.py for details.
"""

import os
import csv
import pandas as pd
from pathlib import Path
from typing import Generator, Dict, Optional, List, Any, Set
from datetime import datetime
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

from .file_source_interface import FileSourceInterface, FileMetadata
try:
    from models.document_models import DocumentMetadata
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from models.document_models import DocumentMetadata


class RawSalesforceExportConnector(FileSourceInterface):
    """File source for raw Salesforce differential export bundles"""
    
    def __init__(self,
                 export_root_dir: str,
                 content_versions_csv: str,
                 content_documents_csv: Optional[str],  # Now optional - can derive from ContentVersion
                 content_document_links_csv: str,
                 deal_metadata_csv: str,
                 client_mapping_csv: Optional[str] = None,
                 vendor_mapping_csv: Optional[str] = None,
                 deal_mapping_csv: Optional[str] = None):
        """
        Initialize raw Salesforce export connector.
        
        Args:
            export_root_dir: Path to raw Salesforce export root directory
            content_versions_csv: Path to content_versions.csv
            content_documents_csv: Optional path to content_documents.csv (if None, derived from ContentVersion)
            content_document_links_csv: Path to content_document_links.csv
            deal_metadata_csv: Path to deal CSV - can be deal__cs.csv or merged deal_merged_financial_data.csv
                              (merged version includes financial fields from older exports)
            client_mapping_csv: Optional path to Client ID -> Name mapping CSV
            vendor_mapping_csv: Optional path to Vendor ID -> Name mapping CSV
            deal_mapping_csv: Optional path to organized_files_to_deal_mapping.csv for user-friendly deal numbers
        """
        super().__init__()
        
        self.export_root_dir = Path(export_root_dir)
        self.content_versions_csv = content_versions_csv
        self.content_documents_csv = content_documents_csv  # Can be None
        self.content_document_links_csv = content_document_links_csv
        self.deal_metadata_csv = deal_metadata_csv
        self.client_mapping_csv = client_mapping_csv
        self.vendor_mapping_csv = vendor_mapping_csv
        self.deal_mapping_csv = deal_mapping_csv
        
        # Loaded data caches
        self._content_versions: Optional[Dict] = None
        self._content_documents: Optional[Dict] = None
        self._content_document_links: Optional[Dict] = None
        self._deal_metadata: Optional[Dict] = None
        self._client_mapping: Optional[Dict] = None
        self._vendor_mapping: Optional[Dict] = None
        self._cv_to_deal_mapping: Optional[Dict] = None
        
        self.logger = logging.getLogger(__name__)
        
        # Validate paths
        self._validate_paths()
        
        # Load all mapping data during initialization
        self._load_all_mappings()
    
    def _validate_paths(self):
        """Validate that all required paths exist"""
        if not self.export_root_dir.exists():
            raise FileNotFoundError(f"Export root directory not found: {self.export_root_dir}")
        
        required_files = [
            self.content_versions_csv,
            self.content_document_links_csv,
            self.deal_metadata_csv
        ]
        
        for file_path in required_files:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Required CSV file not found: {file_path}")
        
        # ContentDocument.csv is optional - check if provided and exists
        if self.content_documents_csv and not Path(self.content_documents_csv).exists():
            self.logger.warning(f"ContentDocument.csv not found: {self.content_documents_csv}")
            self.logger.info("Will derive ContentDocument data from ContentVersion.csv")
            self.content_documents_csv = None
        
        if self.client_mapping_csv and not Path(self.client_mapping_csv).exists():
            self.logger.warning(f"Client mapping CSV not found: {self.client_mapping_csv}")
            self.client_mapping_csv = None
        
        if self.vendor_mapping_csv and not Path(self.vendor_mapping_csv).exists():
            self.logger.warning(f"Vendor mapping CSV not found: {self.vendor_mapping_csv}")
            self.vendor_mapping_csv = None
    
    def _load_all_mappings(self):
        """Load all CSV mappings into memory for fast lookup"""
        self.logger.info("Loading raw Salesforce export metadata...")
        
        # Load ContentVersions
        self.logger.info(f"Loading ContentVersions from {self.content_versions_csv}")
        # Use utf-8-sig to handle BOM (Byte Order Mark) if present, fallback to latin1 for compatibility
        try:
            cv_df = pd.read_csv(self.content_versions_csv, encoding='utf-8-sig', low_memory=False)
        except UnicodeDecodeError:
            # Fallback to latin1 if utf-8-sig fails
            self.logger.warning("utf-8-sig encoding failed, falling back to latin1")
            cv_df = pd.read_csv(self.content_versions_csv, encoding='latin1', low_memory=False)
        self._content_versions = {}
        
        for _, row in cv_df.iterrows():
            cv_id = row['Id']
            # Only process latest versions (IsLatest can be bool True, string 'true', '1', or numeric 1)
            is_latest = row.get('IsLatest', False)
            if isinstance(is_latest, bool):
                if not is_latest:
                    continue
            elif isinstance(is_latest, str):
                # Handle 'true', 'True', '1', 'TRUE', etc.
                if is_latest.lower() not in ['true', '1', 'yes']:
                    continue
            elif isinstance(is_latest, (int, float)):
                # Handle numeric 1/0
                if is_latest != 1:
                    continue
            else:
                # If IsLatest is missing or None, skip (assume not latest)
                continue
            
            # Get Deal__c and handle NaN properly
            deal_c_value = row.get('Deal__c', '')
            if pd.isna(deal_c_value):
                deal_c_value = None
            elif deal_c_value:
                deal_c_value = str(deal_c_value).strip()
            
            # Derive file extension from PathOnClient or FileType
            path_on_client = row.get('PathOnClient', '') or row.get('Title', '')
            file_type_raw = row.get('FileType', '')
            
            # First try to get extension from filename
            file_ext = ''
            if path_on_client and '.' in str(path_on_client):
                file_ext = '.' + str(path_on_client).rsplit('.', 1)[-1].lower()
            
            # Fallback: map FileType to extension if no extension found
            if not file_ext and file_type_raw:
                filetype_map = {
                    'PDF': '.pdf', 'WORD_X': '.docx', 'WORD': '.doc',
                    'EXCEL_X': '.xlsx', 'EXCEL': '.xls',
                    'POWER_POINT_X': '.pptx', 'POWER_POINT': '.ppt',
                    'MSG': '.msg', 'EML': '.eml',
                    'TEXT': '.txt', 'CSV': '.csv',
                    'PNG': '.png', 'JPEG': '.jpg', 'GIF': '.gif',
                    'HTML': '.html', 'XML': '.xml', 'JSON': '.json'
                }
                file_ext = filetype_map.get(str(file_type_raw).upper(), '')
            
            self._content_versions[cv_id] = {
                'content_document_id': row.get('ContentDocumentId', ''),
                'title': row.get('Title', ''),
                'path_on_client': path_on_client,
                'file_type': file_type_raw,
                'file_extension': file_ext,  # Derived from filename or FileType
                'content_size': row.get('ContentSize', 0),
                'deal_id': deal_c_value,  # Properly handled Deal__c link
                'content_modified_date': row.get('ContentModifiedDate', ''),
                'created_date': row.get('CreatedDate', ''),
                'is_deleted': self._parse_boolean(row.get('IsDeleted', False))
            }
        
        self.logger.info(f"Loaded {len(self._content_versions)} ContentVersion records (latest versions only)")
        
        # Load or derive ContentDocuments
        self._content_documents = {}
        
        if self.content_documents_csv:
            # Load from separate ContentDocument.csv file
            self.logger.info(f"Loading ContentDocuments from {self.content_documents_csv}")
            try:
                cd_df = pd.read_csv(self.content_documents_csv, encoding='utf-8-sig', low_memory=False)
            except UnicodeDecodeError:
                self.logger.warning("utf-8-sig encoding failed, falling back to latin1")
                cd_df = pd.read_csv(self.content_documents_csv, encoding='latin1', low_memory=False)
            
            for _, row in cd_df.iterrows():
                doc_id = row['Id']
                self._content_documents[doc_id] = {
                    'title': row.get('Title', ''),
                    'file_type': row.get('FileType', ''),
                    'file_extension': row.get('FileExtension', ''),
                    'content_size': row.get('ContentSize', 0),
                    'created_date': row.get('CreatedDate', '')
                }
            self.logger.info(f"Loaded {len(self._content_documents)} ContentDocument records from file")
        else:
            # Derive ContentDocument data from ContentVersion records
            self.logger.info("Deriving ContentDocument data from ContentVersion records...")
            for cv_data in self._content_versions.values():
                content_doc_id = cv_data.get('content_document_id')
                if content_doc_id and content_doc_id not in self._content_documents:
                    self._content_documents[content_doc_id] = {
                        'title': cv_data.get('title', ''),
                        'file_type': cv_data.get('file_type', ''),
                        'file_extension': cv_data.get('file_extension', ''),
                        'content_size': cv_data.get('content_size', 0),
                        'created_date': cv_data.get('created_date', '')
                    }
            self.logger.info(f"Derived {len(self._content_documents)} ContentDocument records from ContentVersion")
        
        # Load ContentDocumentLinks (for Deal mapping fallback)
        self.logger.info(f"Loading ContentDocumentLinks from {self.content_document_links_csv}")
        # Use utf-8-sig to handle BOM (Byte Order Mark) if present, fallback to latin1 for compatibility
        try:
            cdl_df = pd.read_csv(self.content_document_links_csv, encoding='utf-8-sig', low_memory=False)
        except UnicodeDecodeError:
            # Fallback to latin1 if utf-8-sig fails
            self.logger.warning("utf-8-sig encoding failed, falling back to latin1")
            cdl_df = pd.read_csv(self.content_document_links_csv, encoding='latin1', low_memory=False)
        self._content_document_links = {}
        
        for _, row in cdl_df.iterrows():
            doc_id = row['ContentDocumentId']
            linked_entity_id = row['LinkedEntityId']
            
            # Handle NaN values
            if pd.isna(linked_entity_id) or pd.isna(doc_id):
                continue
            
            # Convert to string and check if linked entity is a Deal (Deal IDs start with 'a0W')
            linked_entity_str = str(linked_entity_id).strip()
            if linked_entity_str.startswith('a0W'):
                if doc_id not in self._content_document_links:
                    self._content_document_links[doc_id] = []
                self._content_document_links[doc_id].append(linked_entity_str)
        
        self.logger.info(f"Loaded {len(self._content_document_links)} ContentDocument to Deal links")
        
        # Build ContentVersion → Deal mapping
        self._build_cv_to_deal_mapping()
        
        # Load Deal metadata
        self.logger.info(f"Loading Deal metadata from {self.deal_metadata_csv}")
        # Use utf-8-sig to handle BOM (Byte Order Mark) if present, fallback to latin1 for compatibility
        try:
            deal_df = pd.read_csv(self.deal_metadata_csv, encoding='utf-8-sig', low_memory=False)
        except UnicodeDecodeError:
            # Fallback to latin1 if utf-8-sig fails
            deal_df = pd.read_csv(self.deal_metadata_csv, encoding='latin1', low_memory=False)
        self._deal_metadata = {}
        
        for _, row in deal_df.iterrows():
            deal_id = row['Id']
            deal_name = row.get('Name', '')
            
            self._deal_metadata[deal_id] = {
                'deal_name': deal_name,
                'subject': row.get('Subject__c', ''),
                'status': row.get('Status__c', ''),
                'deal_reason': row.get('Deal_Reason__c', ''),
                'start_date': row.get('Start_Date__c', '') or row.get('CreatedDate', ''),
                'creation_date': row.get('CreatedDate', ''),  # Direct CreatedDate from deal__cs.csv
                'negotiated_by': row.get('Negotiated_By__c', ''),
                
                # Financial metrics - prefer new merged fields, fallback to old names
                'proposed_amount': self._safe_float(row.get('Total_Proposed_Amount__c')),
                'final_amount': (
                    self._safe_float(row.get('Total_Final_Amount_Year_1__c')) or
                    self._safe_float(row.get('Total_Final_Amount__c'))
                ),
                'savings_1yr': (
                    self._safe_float(row.get('Actual_Savings_Year_1__c')) or
                    self._safe_float(row.get('Total_Savings_1yr__c'))
                ),
                'savings_3yr': self._safe_float(row.get('Total_Savings_3yr__c')),
                'savings_target': (
                    self._safe_float(row.get('Initial_Quote_Year_1__c')) or
                    self._safe_float(row.get('NPI_Savings_Target__c'))
                ),
                'savings_achieved': row.get('Savings_Achieved__c', ''),
                'fixed_savings': self._safe_float(row.get('Fixed_Savings__c')),
                'savings_target_full_term': (
                    self._safe_float(row.get('Actual_Savings_Full_Contract_Term__c')) or
                    self._safe_float(row.get('NPI_Savings_Target_Full_Contract_Term__c'))
                ),
                'final_amount_full_term': self._safe_float(row.get('Final_Amount_Full_Contract_Term__c')),
                
                # Relationships
                'client_id': row.get('Client__c', ''),
                'vendor_id': row.get('Primary_Deal_Vendor__c', ''),
                
                # Contract info
                'contract_term': row.get('Term__c', ''),
                'contract_start': row.get('Contract_Start_Date__c', ''),
                'contract_end': row.get('Contract_Renewal_Date__c', ''),
                'effort_level': row.get('Effort_Level__c', ''),
                'has_fmv_report': row.get('Formal_PDF_FMV_Delivered__c') == 'Yes',
                'deal_origin': row.get('Deal_Origin__c', ''),
                
                # Narrative content
                'current_narrative': row.get('Current_Narrative__c', ''),
                'customer_comments': row.get('Comments_To_Customer__c', ''),
                
                # Deal classification fields (added December 2025)
                'report_type': self._to_str_or_none(row.get('Report_Type__c')),
                'description': self._to_str_or_none(row.get('Description__c')),
                'project_type': self._to_str_or_none(row.get('Project_Type__c')),
                'competition': self._to_str_or_none(row.get('Competition__c')),
                'npi_analyst': self._to_str_or_none(row.get('NPI_Analyst__c')),
                'dual_multi_sourcing': self._to_str_or_none(row.get('Dual_Multi_sourcing_strategy__c')),
                'time_pressure': self._to_str_or_none(row.get('Time_Pressure__c')),
                'advisor_network_used': self._to_str_or_none(row.get('Was_Advisor_Network_SME_Used__c'))
            }
        
        self.logger.info(f"Loaded {len(self._deal_metadata)} deal records")
        
        # Load client mapping if available
        if self.client_mapping_csv:
            self.logger.info(f"Loading client mapping from {self.client_mapping_csv}")
            client_df = pd.read_csv(self.client_mapping_csv, encoding='utf-8-sig')
            # Support both formats: 'Id'/'Name' (old) or '18 Digit ID'/'Account Name' (new)
            id_col = '18 Digit ID' if '18 Digit ID' in client_df.columns else 'Id'
            name_col = 'Account Name' if 'Account Name' in client_df.columns else 'Name'
            self._client_mapping = dict(zip(client_df[id_col], client_df[name_col]))
            self.logger.info(f"Loaded {len(self._client_mapping)} client mappings")
        
        # Load vendor mapping if available
        if self.vendor_mapping_csv:
            self.logger.info(f"Loading vendor mapping from {self.vendor_mapping_csv}")
            vendor_df = pd.read_csv(self.vendor_mapping_csv, encoding='utf-8-sig')
            # Support both formats: 'Id'/'Name' (old) or '18 Digit ID'/'Account Name' (new)
            id_col = '18 Digit ID' if '18 Digit ID' in vendor_df.columns else 'Id'
            name_col = 'Account Name' if 'Account Name' in vendor_df.columns else 'Name'
            self._vendor_mapping = dict(zip(vendor_df[id_col], vendor_df[name_col]))
            self.logger.info(f"Loaded {len(self._vendor_mapping)} vendor mappings")
        
        # Load deal mapping if available (maps raw Salesforce IDs to user-friendly deal numbers)
        self._deal_id_to_number = {}
        if self.deal_mapping_csv:
            self.logger.info(f"Loading deal mapping from {self.deal_mapping_csv}")
            try:
                deal_map_df = pd.read_csv(self.deal_mapping_csv, encoding='utf-8-sig', low_memory=False)
                # Extract deal number from deal_name (e.g., "Deal-58773" from deal_name)
                for _, row in deal_map_df.iterrows():
                    sf_deal_id = row.get('deal_id', '')  # Raw Salesforce ID
                    deal_name = row.get('deal_name', '')  # User-friendly format like "Deal-58773"
                    if sf_deal_id and deal_name:
                        # Extract just the deal number (e.g., "58773" from "Deal-58773")
                        deal_number = deal_name.replace('Deal-', '').strip()
                        self._deal_id_to_number[sf_deal_id] = deal_number
                        self.logger.debug(f"Mapped {sf_deal_id} -> Deal-{deal_number}")
                self.logger.info(f"Loaded {len(self._deal_id_to_number)} deal ID to number mappings")
            except Exception as e:
                self.logger.warning(f"Failed to load deal mapping from {self.deal_mapping_csv}: {e}")
                self._deal_id_to_number = {}
        
        # OPTIMIZATION: Pre-compute valid file paths using parallel I/O
        self._precompute_valid_file_paths()
    
    def _build_cv_to_deal_mapping(self):
        """Build mapping from ContentVersion ID to Deal ID"""
        self._cv_to_deal_mapping = {}
        
        for cv_id, cv_data in self._content_versions.items():
            deal_id = None
            
            # Strategy 1: Direct Deal__c link from ContentVersion
            cv_deal = cv_data.get('deal_id')
            if cv_deal and not pd.isna(cv_deal) and str(cv_deal).strip():
                deal_id = str(cv_deal).strip()
            
            # Strategy 2: Fallback via ContentDocumentLink
            if not deal_id:
                doc_id = cv_data.get('content_document_id')
                if doc_id and doc_id in self._content_document_links:
                    # Get first Deal link (there may be multiple)
                    deal_links = self._content_document_links[doc_id]
                    if deal_links:
                        deal_id = deal_links[0]
            
            # Only store valid deal IDs (not NaN, not empty)
            if deal_id and not pd.isna(deal_id) and str(deal_id).strip():
                self._cv_to_deal_mapping[cv_id] = str(deal_id).strip()
        
        self.logger.info(f"Built {len(self._cv_to_deal_mapping)} ContentVersion → Deal mappings")
    
    def _precompute_valid_file_paths(self, max_workers: int = 16):
        """
        Pre-compute and validate file paths using parallel I/O.
        
        OPTIMIZATION: Instead of checking file existence in the main discovery loop,
        we do it once upfront using parallel threads. This dramatically reduces
        disk I/O wait time.
        
        Args:
            max_workers: Number of parallel threads for file existence checks
        """
        self.logger.info("🚀 Pre-computing valid file paths (parallel I/O optimization)...")
        
        # First, compute all potential file paths (CPU-bound, fast)
        path_candidates = {}
        for cv_id, cv_data in self._content_versions.items():
            # Skip deleted files early
            if cv_data.get('is_deleted'):
                continue
            
            # Skip unsupported file types early
            filename = cv_data.get('path_on_client', '') or cv_data.get('title', '')
            if pd.isna(filename):
                filename = ''
            else:
                filename = str(filename).strip()
            
            if not filename or not self.is_supported_file_type(filename):
                continue
            
            # Get deal ID for path resolution
            deal_id = self._cv_to_deal_mapping.get(cv_id)
            content_doc_id = cv_data.get('content_document_id', '')
            if pd.isna(content_doc_id):
                content_doc_id = ''
            else:
                content_doc_id = str(content_doc_id).strip()
            
            # Resolve path (still CPU-bound)
            file_path = self._resolve_file_path(cv_id, content_doc_id, deal_id)
            if file_path:
                path_candidates[cv_id] = file_path
        
        self.logger.info(f"   Resolved {len(path_candidates):,} potential file paths")
        
        # Now check existence in parallel (I/O-bound)
        def check_exists(item):
            cv_id, path = item
            return cv_id, path.exists() if path else False
        
        self._valid_file_paths: Dict[str, Path] = {}
        checked = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(check_exists, item): item[0] 
                      for item in path_candidates.items()}
            
            for future in as_completed(futures):
                cv_id, exists = future.result()
                if exists:
                    self._valid_file_paths[cv_id] = path_candidates[cv_id]
                checked += 1
                
                # Progress update every 10,000 checks
                if checked % 10000 == 0:
                    self.logger.info(f"   Validated {checked:,}/{len(path_candidates):,} file paths...")
        
        valid_count = len(self._valid_file_paths)
        valid_pct = (valid_count / len(path_candidates) * 100) if path_candidates else 0
        self.logger.info(f"✅ Pre-computed {valid_count:,} valid file paths ({valid_pct:.1f}% resolution rate)")
    
    def _parse_boolean(self, value) -> bool:
        """Safely parse boolean value from CSV (handles bool, string 'true'/'false', etc.)"""
        if isinstance(value, bool):
            return value
        if pd.isna(value) or value == '':
            return False
        if isinstance(value, str):
            return value.lower() == 'true'
        return bool(value)
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float, return None if not possible"""
        if pd.isna(value) or value == '' or value == '0':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _to_str_or_none(self, value) -> Optional[str]:
        """Convert value to string, handling NaN and None"""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return str(value).strip() if str(value).strip() else None
    
    def _calculate_savings_percentage(self, proposed: Optional[float], final: Optional[float]) -> Optional[float]:
        """Calculate savings percentage from proposed and final amounts"""
        if proposed and final and proposed > 0:
            return round(((proposed - final) / proposed) * 100, 2)
        return None
    
    def _resolve_file_path(self, cv_id: str, content_document_id: str, deal_id: Optional[str] = None) -> Optional[Path]:
        """
        Resolve file path from ContentVersion ID.
        
        Tries multiple strategies (in order):
        1. ContentVersions/VersionData/<ContentVersionId>/<filename> (primary location)
        2. Deal__cs/<exportId>/<DealId>/<filename> (Deal-specific location)
        3. Attachments/Body/<ContentDocumentId> (legacy attachments)
        
        Returns Path object if found, None otherwise.
        """
        # Normalize content_document_id (handle NaN/float)
        if pd.isna(content_document_id) or not content_document_id:
            content_document_id = None
        else:
            content_document_id = str(content_document_id).strip()
            if not content_document_id:
                content_document_id = None
        
        # Normalize deal_id (handle NaN/float)
        if deal_id and not pd.isna(deal_id):
            # Ensure deal_id is a string
            deal_id_str = str(deal_id).strip()
            if not deal_id_str:
                deal_id = None
            else:
                deal_id = deal_id_str
        else:
            deal_id = None
        
        # Strategy 0: ContentVersion/<ContentVersionId> (flat structure - file named by CV ID)
        # Some exports have files directly as ContentVersion/{ContentVersionId} without subdirs or extensions
        if cv_id:
            flat_file = self.export_root_dir / 'ContentVersion' / cv_id
            if flat_file.exists() and flat_file.is_file():
                return flat_file
        
        # Strategy 1: ContentVersions/VersionData/<ContentVersionId>/ (primary location)
        if cv_id:
            cv_version_data_dir = self.export_root_dir / 'ContentVersions' / 'VersionData' / cv_id
            if cv_version_data_dir.exists() and cv_version_data_dir.is_dir():
                # Look for files in this ContentVersion directory
                for file_path in cv_version_data_dir.rglob('*'):
                    if file_path.is_file() and not file_path.name.startswith('.'):  # Skip hidden files
                        return file_path
        
        # Strategy 2: Deal__cs/<exportId>/<DealId>/ (Deal-specific location)
        if deal_id:
            deal_cs_dir = self.export_root_dir / 'Deal__cs'
            if deal_cs_dir.exists():
                # Find export batch directory (starts with 0EM)
                for export_batch_dir in deal_cs_dir.iterdir():
                    if export_batch_dir.is_dir() and export_batch_dir.name.startswith('0EM'):
                        deal_dir = export_batch_dir / deal_id
                        if deal_dir.exists():
                            # Look for files in this deal directory
                            for file_path in deal_dir.rglob('*'):
                                if file_path.is_file() and not file_path.name.startswith('.'):  # Skip hidden files
                                    return file_path
        
        # Strategy 3: Attachments/Body/<ContentDocumentId> (legacy attachments)
        if content_document_id:
            attachments_dir = self.export_root_dir / 'Attachments' / 'Body' / content_document_id
            if attachments_dir.exists() and attachments_dir.is_file():
                return attachments_dir
            
            # Strategy 3b: Try ContentDocumentId as filename in Attachments/Body
            attachments_body_dir = self.export_root_dir / 'Attachments' / 'Body'
            if attachments_body_dir.exists():
                # Check if there's a file with ContentDocumentId as name
                potential_file = attachments_body_dir / content_document_id
                if potential_file.exists() and potential_file.is_file():
                    return potential_file
        
        return None
    
    def _enrich_with_deal_metadata(self, file_metadata: FileMetadata, deal_id: Optional[str] = None) -> DocumentMetadata:
        """Enrich file metadata with Deal information"""
        
        # Start with basic DocumentMetadata
        doc_metadata = DocumentMetadata(
            path=file_metadata.path,
            name=file_metadata.name,
            size=file_metadata.size,
            size_mb=file_metadata.size_mb,
            file_type=file_metadata.file_type,
            modified_time=file_metadata.modified_time,
            full_path=file_metadata.full_source_path,
            content_hash=file_metadata.content_hash,
            is_downloadable=file_metadata.is_downloadable
        )
        
        if not deal_id:
            doc_metadata.mapping_status = "unmapped"
            doc_metadata.mapping_reason = "no_deal_association"
            return doc_metadata
        
        # Get deal metadata
        deal_data = self._deal_metadata.get(deal_id)
        
        if not deal_data:
            self.logger.debug(f"No deal metadata found for deal_id: {deal_id}")
            # Use deal_name from mapping CSV if available, otherwise fall back to raw ID
            user_friendly_id = self._deal_id_to_number.get(deal_id, deal_id)
            doc_metadata.deal_id = user_friendly_id
            doc_metadata.salesforce_deal_id = deal_id  # Store raw ID for tracing
            doc_metadata.mapping_status = "mapped_no_metadata"
            return doc_metadata
        
        # Enrich with full deal metadata
        # Use Name field from deal CSV (e.g., "Deal-36801") as friendly ID
        # Prefer mapping CSV if provided, otherwise use Name field directly
        friendly_deal_name = deal_data.get('deal_name', deal_id)  # Name field has "Deal-XXXXX" format
        if self._deal_id_to_number:
            # If mapping CSV provided, use it (for backwards compatibility)
            friendly_deal_name = self._deal_id_to_number.get(deal_id, friendly_deal_name)
        
        doc_metadata.deal_id = friendly_deal_name  # Use friendly "Deal-36801" format
        doc_metadata.salesforce_deal_id = deal_id  # Store raw Salesforce ID for tracing
        doc_metadata.deal_subject = deal_data['subject']
        doc_metadata.deal_status = deal_data['status']
        doc_metadata.deal_reason = deal_data['deal_reason']
        doc_metadata.deal_start_date = deal_data['start_date']
        # Add creation date for time-based filtering (from CreatedDate field in deal__cs.csv)
        doc_metadata.deal_creation_date = deal_data.get('creation_date', '')
        doc_metadata.negotiated_by = deal_data['negotiated_by']
        
        # Financial metrics
        doc_metadata.proposed_amount = deal_data['proposed_amount']
        doc_metadata.final_amount = deal_data['final_amount']
        doc_metadata.savings_1yr = deal_data['savings_1yr']
        doc_metadata.savings_3yr = deal_data['savings_3yr']
        doc_metadata.savings_target = deal_data['savings_target']
        doc_metadata.savings_percentage = self._calculate_savings_percentage(
            deal_data['proposed_amount'],
            deal_data['final_amount']
        )
        doc_metadata.savings_achieved = deal_data.get('savings_achieved')
        doc_metadata.fixed_savings = deal_data.get('fixed_savings')
        doc_metadata.savings_target_full_term = deal_data.get('savings_target_full_term')
        doc_metadata.final_amount_full_term = deal_data.get('final_amount_full_term')
        
        # Client/Vendor info - store both raw Salesforce IDs and friendly versions
        raw_client_id = self._to_str_or_none(deal_data['client_id'])
        raw_vendor_id = self._to_str_or_none(deal_data['vendor_id'])
        
        # Always store the raw Salesforce IDs for tracing
        doc_metadata.salesforce_client_id = raw_client_id
        doc_metadata.salesforce_vendor_id = raw_vendor_id
        
        # Try to get friendly names from mapping files
        if self._client_mapping and raw_client_id:
            client_name = self._client_mapping.get(raw_client_id)
            doc_metadata.client_name = client_name
            # Use friendly name as the client_id if available, otherwise use shortened raw ID
            doc_metadata.client_id = client_name if client_name else f"Client-{raw_client_id[-8:]}"
        else:
            doc_metadata.client_id = f"Client-{raw_client_id[-8:]}" if raw_client_id else "Unknown Client"
        
        if self._vendor_mapping and raw_vendor_id:
            vendor_name = self._vendor_mapping.get(raw_vendor_id)
            doc_metadata.vendor_name = vendor_name
            # Use friendly name as the vendor_id if available, otherwise use shortened raw ID
            doc_metadata.vendor_id = vendor_name if vendor_name else f"Vendor-{raw_vendor_id[-8:]}"
        else:
            doc_metadata.vendor_id = f"Vendor-{raw_vendor_id[-8:]}" if raw_vendor_id else "Unknown Vendor"
        
        # Contract info
        doc_metadata.contract_term = deal_data['contract_term']
        doc_metadata.contract_start = deal_data['contract_start']
        doc_metadata.contract_end = deal_data['contract_end']
        doc_metadata.effort_level = deal_data['effort_level']
        doc_metadata.has_fmv_report = deal_data['has_fmv_report']
        doc_metadata.deal_origin = deal_data['deal_origin']
        
        # Rich Narrative Content
        doc_metadata.current_narrative = deal_data.get('current_narrative')
        doc_metadata.customer_comments = deal_data.get('customer_comments')
        doc_metadata.content_source = "document_file"
        
        # Deal Classification Fields (added December 2025)
        doc_metadata.report_type = deal_data.get('report_type')
        doc_metadata.description = deal_data.get('description')
        doc_metadata.project_type = deal_data.get('project_type')
        doc_metadata.competition = deal_data.get('competition')
        doc_metadata.npi_analyst = deal_data.get('npi_analyst')
        doc_metadata.dual_multi_sourcing = deal_data.get('dual_multi_sourcing')
        doc_metadata.time_pressure = deal_data.get('time_pressure')
        doc_metadata.advisor_network_used = deal_data.get('advisor_network_used')
        
        doc_metadata.mapping_status = "mapped"
        doc_metadata.mapping_method = "raw_export_csv"
        
        return doc_metadata
    
    def list_documents(self, folder_path: str = "",
                      file_types: Optional[List[str]] = None,
                      batch_size: Optional[int] = None) -> Generator[FileMetadata, None, None]:
        """
        List all documents from raw export with Deal metadata enrichment.
        
        OPTIMIZED: Uses pre-computed valid file paths from _precompute_valid_file_paths()
        to avoid per-file I/O in the main loop.
        
        Args:
            folder_path: Not used for raw exports (all files scanned)
            file_types: File extensions to filter (e.g., ['.pdf', '.docx'])
            batch_size: Not used in this implementation
            
        Yields:
            FileMetadata objects for each discovered document
        """
        
        self.logger.info(f"Discovering files from raw Salesforce export: {self.export_root_dir}")
        
        # Check if pre-computed paths are available
        if not hasattr(self, '_valid_file_paths') or not self._valid_file_paths:
            self.logger.warning("⚠️ Pre-computed file paths not available, falling back to on-demand resolution")
            self._valid_file_paths = {}
        else:
            self.logger.info(f"✅ Using {len(self._valid_file_paths):,} pre-validated file paths (optimized)")
        
        processed_count = 0
        mapped_count = 0
        unmapped_count = 0
        
        # OPTIMIZATION: Iterate only over pre-validated files when available
        cv_ids_to_process = self._valid_file_paths.keys() if self._valid_file_paths else self._content_versions.keys()
        
        for cv_id in cv_ids_to_process:
            cv_data = self._content_versions.get(cv_id, {})
            
            # Skip deleted files (double-check)
            if cv_data.get('is_deleted'):
                continue
            
            # Get file extension (handle NaN/float/string)
            file_ext_raw = cv_data.get('file_extension', '')
            if pd.isna(file_ext_raw) or not file_ext_raw:
                file_ext = ''
            else:
                file_ext = str(file_ext_raw).lower().strip()
            
            if file_ext and not file_ext.startswith('.'):
                file_ext = '.' + file_ext
            
            # Filter by file type if specified
            if file_types and file_ext not in file_types:
                continue
            
            # Get Deal ID
            deal_id = self._cv_to_deal_mapping.get(cv_id)
            
            # OPTIMIZATION: Use pre-computed file path (no I/O needed!)
            if self._valid_file_paths:
                file_path = self._valid_file_paths.get(cv_id)
            else:
                # Fallback: resolve and check existence (slow path)
                content_doc_id = cv_data.get('content_document_id', '')
                if pd.isna(content_doc_id) or not content_doc_id:
                    content_doc_id = ''
                else:
                    content_doc_id = str(content_doc_id).strip()
                
                file_path = self._resolve_file_path(cv_id, content_doc_id, deal_id)
                if not file_path or not isinstance(file_path, Path) or not file_path.exists():
                    self.logger.debug(f"File not found for ContentVersion {cv_id}: {cv_data.get('title', 'N/A')}")
                    continue
            
            if not file_path:
                continue
                
            try:
                # Create relative path for metadata
                relative_path = str(file_path.relative_to(self.export_root_dir))
                
                # Create FileMetadata (normalize all string fields)
                path_on_client = cv_data.get('path_on_client', '')
                if pd.isna(path_on_client):
                    path_on_client = ''
                else:
                    path_on_client = str(path_on_client).strip()
                
                title = cv_data.get('title', '')
                if pd.isna(title):
                    title = ''
                else:
                    title = str(title).strip()
                
                file_name = path_on_client or title or file_path.name
                
                # Use CSV data instead of disk stat() - OPTIMIZATION: avoids I/O per file
                content_size = cv_data.get('content_size', 0)
                if pd.isna(content_size):
                    content_size = 0
                else:
                    content_size = int(content_size)
                
                content_modified_date = cv_data.get('content_modified_date', '')
                if pd.isna(content_modified_date):
                    content_modified_date = ''
                else:
                    content_modified_date = str(content_modified_date).strip()
                
                # Create FileMetadata using CSV data (no disk stat needed!)
                file_metadata = FileMetadata(
                    path=relative_path,
                    name=file_name,
                    size=content_size,  # From CSV - avoids stat() I/O
                    modified_time=content_modified_date,  # From CSV - avoids stat() I/O
                    file_type=file_ext,
                    source_id=cv_id,
                    source_type="salesforce_raw",
                    full_source_path=str(file_path),
                    content_hash=None,
                    is_downloadable=True
                )
                
                processed_count += 1
                if deal_id:
                    mapped_count += 1
                else:
                    unmapped_count += 1
                
                # Log progress every 1000 files
                if processed_count % 1000 == 0:
                    mapped_pct = (mapped_count / processed_count) * 100 if processed_count > 0 else 0
                    self.logger.info(f"📊 Discovery stats: {processed_count:,} files processed, "
                                   f"{mapped_count:,} mapped ({mapped_pct:.1f}%), "
                                   f"{unmapped_count:,} unmapped")
                
                yield file_metadata
                
            except Exception as e:
                self.logger.error(f"Error processing ContentVersion {cv_id}: {e}")
                continue
    
    def list_documents_as_metadata(self, folder_path: str = "", 
                                  require_deal_association: bool = False) -> Generator[DocumentMetadata, None, None]:
        """List documents with Deal metadata enrichment
        
        Args:
            folder_path: Optional path filter
            require_deal_association: If True, only yield documents with valid deal associations.
                                     Useful for ensuring all metadata fields are populated.
                                     Default: False (returns all documents)
        
        Yields:
            DocumentMetadata objects enriched with deal information
        """
        
        skipped_no_deal = 0
        
        for file_metadata in self.list_documents(folder_path):
            # Get ContentVersion ID from source_id
            cv_id = file_metadata.source_id
            cv_data = self._content_versions.get(cv_id, {})
            
            # Get Deal ID
            deal_id = self._cv_to_deal_mapping.get(cv_id)
            
            # Filter: Skip documents without deal associations if required
            if require_deal_association:
                if not deal_id or deal_id == '' or pd.isna(deal_id):
                    skipped_no_deal += 1
                    if skipped_no_deal % 1000 == 0:
                        self.logger.debug(f"Skipped {skipped_no_deal} documents without deal associations")
                    continue
            
            # Enrich with Deal metadata
            doc_metadata = self._enrich_with_deal_metadata(file_metadata, deal_id)
            
            # Add Salesforce-specific fields
            doc_metadata.salesforce_content_version_id = cv_id
            
            yield doc_metadata
        
        if require_deal_association and skipped_no_deal > 0:
            self.logger.info(f"ℹ️  Filtered out {skipped_no_deal:,} documents without deal associations")
    
    def download_file(self, file_path: str) -> bytes:
        """Download file content from raw export directory"""
        full_path = self.export_root_dir / file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        
        try:
            with open(full_path, 'rb') as f:
                return f.read()
        except Exception as e:
            raise IOError(f"Error reading file {full_path}: {e}")
    
    def download_document(self, file_path: str) -> bytes:
        """Compatibility method for DocumentProcessor"""
        return self.download_file(file_path)
    
    def validate_connection(self) -> bool:
        """Validate that export directory and CSV files are accessible"""
        try:
            if not self.export_root_dir.exists():
                return False
            
            required_files = [
                self.content_versions_csv,
                self.content_documents_csv,
                self.content_document_links_csv,
                self.deal_metadata_csv
            ]
            
            for file_path in required_files:
                if not Path(file_path).exists():
                    return False
                # Try to read first line (handle BOM)
                try:
                    with open(file_path, 'r', encoding='utf-8-sig') as f:
                        f.readline()
                except UnicodeDecodeError:
                    with open(file_path, 'r', encoding='latin1') as f:
                        f.readline()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Connection validation failed: {e}")
            return False
    
    def get_file_content_hash(self, file_path: str) -> str:
        """Get SHA256 hash of file content"""
        full_path = self.export_root_dir / file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        
        sha256_hash = hashlib.sha256()
        with open(full_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists in export directory"""
        full_path = self.export_root_dir / file_path
        return full_path.exists()
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get information about the raw Salesforce export source"""
        return {
            "type": "salesforce_raw",
            "export_root_dir": str(self.export_root_dir),
            "content_versions_csv": self.content_versions_csv,
            "content_documents_csv": self.content_documents_csv,
            "content_document_links_csv": self.content_document_links_csv,
            "deal_metadata_csv": self.deal_metadata_csv,
            "client_mapping_csv": self.client_mapping_csv,
            "vendor_mapping_csv": self.vendor_mapping_csv,
            "content_versions_loaded": len(self._content_versions) if self._content_versions else 0,
            "deal_records_loaded": len(self._deal_metadata) if self._deal_metadata else 0,
            "cv_to_deal_mappings": len(self._cv_to_deal_mapping) if self._cv_to_deal_mapping else 0,
            "client_mappings_loaded": len(self._client_mapping) if self._client_mapping else 0,
            "vendor_mappings_loaded": len(self._vendor_mapping) if self._vendor_mapping else 0
        }
    
    def get_export_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the export
        
        Returns detailed counts of files, deals, and mapping coverage
        """
        # Count files with deal associations
        files_with_deals = sum(1 for cv_id in self._content_versions.keys() 
                              if self._cv_to_deal_mapping.get(cv_id) and 
                              not pd.isna(self._cv_to_deal_mapping.get(cv_id)))
        files_without_deals = len(self._content_versions) - files_with_deals
        
        # Count unique deals
        unique_deals = set(deal_id for deal_id in self._cv_to_deal_mapping.values() 
                          if deal_id and not pd.isna(deal_id))
        
        # Count deals with metadata
        deals_with_metadata = sum(1 for deal_id in unique_deals 
                                 if deal_id in self._deal_metadata)
        
        # Count files with friendly deal IDs
        files_with_friendly_ids = sum(1 for cv_id in self._content_versions.keys()
                                     if self._cv_to_deal_mapping.get(cv_id) in self._deal_id_to_number)
        
        stats = {
            'total_files': len(self._content_versions),
            'files_with_deals': files_with_deals,
            'files_without_deals': files_without_deals,
            'deal_association_rate': (files_with_deals / len(self._content_versions) * 100) if self._content_versions else 0,
            'unique_deals': len(unique_deals),
            'deals_with_metadata': deals_with_metadata,
            'files_with_friendly_ids': files_with_friendly_ids,
            'total_deal_records': len(self._deal_metadata),
            'deal_id_mappings': len(self._deal_id_to_number)
        }
        
        return stats
    
    def print_export_statistics(self):
        """Print formatted export statistics"""
        stats = self.get_export_statistics()
        
        self.logger.info("\n" + "="*70)
        self.logger.info("📊 SALESFORCE EXPORT STATISTICS")
        self.logger.info("="*70)
        self.logger.info(f"\n📁 Files:")
        self.logger.info(f"   Total files in export: {stats['total_files']:,}")
        self.logger.info(f"   Files with deal associations: {stats['files_with_deals']:,} ({stats['deal_association_rate']:.1f}%)")
        self.logger.info(f"   Files without deal associations: {stats['files_without_deals']:,}")
        self.logger.info(f"   Files with user-friendly deal IDs: {stats['files_with_friendly_ids']:,}")
        
        self.logger.info(f"\n🤝 Deals:")
        self.logger.info(f"   Unique deals referenced: {stats['unique_deals']:,}")
        self.logger.info(f"   Deals with full metadata: {stats['deals_with_metadata']:,}")
        self.logger.info(f"   Total deal records in CSV: {stats['total_deal_records']:,}")
        
        self.logger.info(f"\n🔗 Mappings:")
        self.logger.info(f"   Deal ID to number mappings: {stats['deal_id_mappings']:,}")
        self.logger.info(f"   ContentVersion to Deal links: {len(self._cv_to_deal_mapping):,}")
        
        self.logger.info("\n" + "="*70 + "\n")
        
        return stats
    
    def get_deal_narrative_content(self, deal_id: str) -> Dict[str, Optional[str]]:
        """
        Get narrative content for a deal for processing as virtual documents.
        
        Args:
            deal_id: Deal ID (e.g., "a0W0y00000YwGf0EAF")
            
        Returns:
            Dict with 'current_narrative' and 'customer_comments' keys
        """
        deal_data = self._deal_metadata.get(deal_id, {})
        
        # Safely get narrative content, handling NaN values
        current_narrative = deal_data.get('current_narrative')
        customer_comments = deal_data.get('customer_comments')
        
        # Handle pandas NaN values
        if pd.isna(current_narrative):
            current_narrative = None
        elif current_narrative:
            current_narrative = str(current_narrative).strip()
            if not current_narrative:
                current_narrative = None
                
        if pd.isna(customer_comments):
            customer_comments = None
        elif customer_comments:
            customer_comments = str(customer_comments).strip()
            if not customer_comments:
                customer_comments = None
        
        return {
            'current_narrative': current_narrative,
            'customer_comments': customer_comments
        }
    
    def get_all_deals_with_narrative_content(self) -> Generator[Dict[str, Any], None, None]:
        """
        Get all deals that have narrative content for processing.
        
        Yields:
            Dict with deal info and narrative content
        """
        for deal_id, deal_data in self._deal_metadata.items():
            current_narrative = deal_data.get('current_narrative', '')
            customer_comments = deal_data.get('customer_comments', '')
            
            # Handle pandas NaN values
            if pd.isna(current_narrative):
                current_narrative = ''
            else:
                current_narrative = str(current_narrative).strip()
                
            if pd.isna(customer_comments):
                customer_comments = ''
            else:
                customer_comments = str(customer_comments).strip()
            
            # Only yield deals that have narrative content
            if current_narrative or customer_comments:
                yield {
                    'deal_id': deal_id,
                    'deal_name': deal_data.get('deal_name', ''),
                    'deal_data': deal_data,
                    'current_narrative': current_narrative,
                    'customer_comments': customer_comments
                }

