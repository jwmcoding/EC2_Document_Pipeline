"""
Salesforce File Source Implementation

This module provides file source integration for Salesforce exported files with
Deal metadata enrichment. It extends the local filesystem approach while adding
comprehensive Deal metadata from Salesforce exports.

Features:
- File discovery from organized Salesforce directory
- Deal metadata enrichment during discovery
- Client/Vendor name mapping
- Financial metrics integration
- Support for narrative content processing
"""

import os
import csv
import pandas as pd
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


class SalesforceFileSource(FileSourceInterface):
    """File source for Salesforce exported and organized files with Deal metadata enrichment"""
    
    def __init__(self, 
                 organized_files_dir: str,
                 file_mapping_csv: str,
                 deal_metadata_csv: str,
                 client_mapping_csv: Optional[str] = None,
                 vendor_mapping_csv: Optional[str] = None,
                 require_deal_association: bool = True,
                 min_file_size_kb: float = 10.0):
        """
        Initialize Salesforce file source with metadata mappings.
        
        Args:
            organized_files_dir: Path to organized Salesforce files directory
            file_mapping_csv: Path to organized_files_to_deal_mapping.csv
            deal_metadata_csv: Path to Deal__c.csv from Salesforce export
            client_mapping_csv: Optional path to Client ID -> Name mapping CSV
            vendor_mapping_csv: Optional path to Vendor ID -> Name mapping CSV
            require_deal_association: If True, only yield files mapped to deals (default: True)
            min_file_size_kb: Minimum file size in KB to process (default: 10KB to skip icons/logos)
        """
        super().__init__()
        
        self.organized_files_dir = Path(organized_files_dir)
        self.file_mapping_csv = file_mapping_csv
        self.deal_metadata_csv = deal_metadata_csv
        self.client_mapping_csv = client_mapping_csv
        self.vendor_mapping_csv = vendor_mapping_csv
        self.require_deal_association = require_deal_association
        self.min_file_size_kb = min_file_size_kb
        self.min_file_size_bytes = int(min_file_size_kb * 1024)  # Convert to bytes
        
        # Loaded data caches
        self._file_to_deal_mapping: Optional[Dict] = None
        self._deal_metadata: Optional[Dict] = None
        self._client_mapping: Optional[Dict] = None
        self._vendor_mapping: Optional[Dict] = None
        
        self.logger = logging.getLogger(__name__)
        
        # Validate paths
        self._validate_paths()
        
        # Load all mapping data during initialization
        self._load_all_mappings()
    
    def _validate_paths(self):
        """Validate that all required paths exist"""
        if not self.organized_files_dir.exists():
            raise FileNotFoundError(f"Organized files directory not found: {self.organized_files_dir}")
        
        if not Path(self.file_mapping_csv).exists():
            raise FileNotFoundError(f"File mapping CSV not found: {self.file_mapping_csv}")
        
        if not Path(self.deal_metadata_csv).exists():
            raise FileNotFoundError(f"Deal metadata CSV not found: {self.deal_metadata_csv}")
        
        if self.client_mapping_csv and not Path(self.client_mapping_csv).exists():
            self.logger.warning(f"Client mapping CSV not found: {self.client_mapping_csv}")
            self.client_mapping_csv = None
        
        if self.vendor_mapping_csv and not Path(self.vendor_mapping_csv).exists():
            self.logger.warning(f"Vendor mapping CSV not found: {self.vendor_mapping_csv}")
            self.vendor_mapping_csv = None
    
    def _load_all_mappings(self):
        """Load all CSV mappings into memory for fast lookup"""
        self.logger.info("Loading Salesforce metadata mappings...")
        
        # Load file-to-deal mapping
        self.logger.info(f"Loading file mapping from {self.file_mapping_csv}")
        file_mapping_df = pd.read_csv(self.file_mapping_csv)
        self._file_to_deal_mapping = {}
        
        for _, row in file_mapping_df.iterrows():
            filename = row['filename']
            self._file_to_deal_mapping[filename] = {
                'deal_name': row['deal_name'],
                'deal_id': row['deal_id'], 
                'subject': row.get('subject', ''),
                'client_id': row.get('client_id', ''),
                'status': row.get('status', ''),
                'salesforce_id': row.get('salesforce_id', ''),
                'relative_path': row.get('relative_path', ''),
                # Enhanced mapping fields (client/vendor names directly from CSV)
                'client_name': row.get('client_name', ''),
                'vendor_name': row.get('vendor_name', '')
            }
        
        self.logger.info(f"Loaded {len(self._file_to_deal_mapping)} file-to-deal mappings")
        
        # Load Deal metadata
        self.logger.info(f"Loading Deal metadata from {self.deal_metadata_csv}")
        deal_df = pd.read_csv(self.deal_metadata_csv, encoding='latin1', low_memory=False)
        self._deal_metadata = {}
        
        for _, row in deal_df.iterrows():
            deal_name = row['Name']
            self._deal_metadata[deal_name] = {
                # Core info
                'subject': row.get('Subject__c', ''),
                'status': row.get('Status__c', ''),
                'deal_reason': row.get('Deal_Reason__c', ''),
                'start_date': row.get('Start_Date__c', ''),
                'creation_date': row.get('CreatedDate', ''),  # Direct CreatedDate from deal__cs.csv
                'negotiated_by': row.get('Negotiated_By__c', ''),
                
                # Financial metrics (CORRECTED COLUMN NAMES!)
                'proposed_amount': self._safe_float(row.get('Total_Proposed_Amount__c')),
                'final_amount': self._safe_float(row.get('Total_Final_Amount__c')),
                'savings_1yr': self._safe_float(row.get('Total_Savings_1yr__c')),
                'savings_3yr': self._safe_float(row.get('Total_Savings_3yr__c')),
                'savings_target': self._safe_float(row.get('NPI_Savings_Target__c')),
                
                # MISSING CRITICAL SAVINGS FIELDS (From EDA Analysis!)
                'savings_achieved': row.get('Savings_Achieved__c', ''),  # 90.9% populated - actual outcomes
                'fixed_savings': self._safe_float(row.get('Fixed_Savings__c')),  # 92.3% populated
                'savings_target_full_term': self._safe_float(row.get('NPI_Savings_Target_Full_Contract_Term__c')),
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
                
                # Narrative content (for processing phase)
                'current_narrative': row.get('Current_Narrative__c', ''),
                'customer_comments': row.get('Comments_To_Customer__c', ''),
                
                # Deal Classification Fields (December 2025 - for Pinecone filtering)
                'report_type': row.get('Report_Type__c', ''),
                'project_type': row.get('Project_Type__c', ''),
                'competition': row.get('Competition__c', ''),
                'npi_analyst': row.get('NPI_Analyst__c', ''),
                'dual_multi_sourcing': row.get('Dual_Multi_sourcing_strategy__c', ''),
                'time_pressure': row.get('Time_Pressure__c', ''),
                'advisor_network_used': row.get('Was_Advisor_Network_SME_Used__c', '')
            }
        
        self.logger.info(f"Loaded {len(self._deal_metadata)} deal records")
        
        # Load client mapping if available
        if self.client_mapping_csv:
            self.logger.info(f"Loading client mapping from {self.client_mapping_csv}")
            client_df = pd.read_csv(self.client_mapping_csv)
            # Handle both column name formats: 'Id'/'Name' (December) vs 'Account ID'/'Account Name' (August)
            id_col = 'Account ID' if 'Account ID' in client_df.columns else 'Id'
            name_col = 'Account Name' if 'Account Name' in client_df.columns else 'Name'
            # Truncate IDs to 15 chars for consistent matching (SF IDs can be 15 or 18 chars)
            self._client_mapping = dict(zip(client_df[id_col].astype(str).str[:15], client_df[name_col]))
            self.logger.info(f"Loaded {len(self._client_mapping)} client mappings (using {id_col}/{name_col} columns)")
        
        # Load vendor mapping if available
        if self.vendor_mapping_csv:
            self.logger.info(f"Loading vendor mapping from {self.vendor_mapping_csv}")
            vendor_df = pd.read_csv(self.vendor_mapping_csv)
            # Handle both column name formats: 'Id'/'Name' (December) vs 'Account ID'/'Account Name' (August)
            id_col = 'Account ID' if 'Account ID' in vendor_df.columns else 'Id'
            name_col = 'Account Name' if 'Account Name' in vendor_df.columns else 'Name'
            # Truncate IDs to 15 chars for consistent matching (SF IDs can be 15 or 18 chars)
            self._vendor_mapping = dict(zip(vendor_df[id_col].astype(str).str[:15], vendor_df[name_col]))
            self.logger.info(f"Loaded {len(self._vendor_mapping)} vendor mappings (using {id_col}/{name_col} columns)")
    
    def _to_str_or_none(self, value) -> Optional[str]:
        """Convert value to string, handling NaN and None"""
        import pandas as pd
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return str(value).strip() if str(value).strip() else None
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float, return None if not possible"""
        if pd.isna(value) or value == '' or value == '0':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _calculate_savings_percentage(self, proposed: Optional[float], final: Optional[float]) -> Optional[float]:
        """Calculate savings percentage from proposed and final amounts"""
        if proposed and final and proposed > 0:
            return round(((proposed - final) / proposed) * 100, 2)
        return None
    
    def _enrich_with_deal_metadata(self, file_metadata: FileMetadata) -> DocumentMetadata:
        """Enrich file metadata with Deal information using improved matching logic"""
        
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
        
        # ENHANCED DEAL MAPPING LOGIC WITH MULTIPLE FALLBACK STRATEGIES
        deal_mapping = None
        match_method = None
        
        # Strategy 1: Exact filename match (fastest)
        if file_metadata.name in self._file_to_deal_mapping:
            deal_mapping = self._file_to_deal_mapping[file_metadata.name]
            match_method = "exact_filename"
        
        # Strategy 2: Exact relative path match
        elif not deal_mapping:
            for mapped_filename, mapping_data in self._file_to_deal_mapping.items():
                if mapping_data['relative_path'] == file_metadata.path:
                    deal_mapping = mapping_data
                    match_method = "exact_path"
                    break
        
        # Strategy 3: Endswith path match (for subdirectory scanning)
        if not deal_mapping:
            for mapped_filename, mapping_data in self._file_to_deal_mapping.items():
                if mapping_data['relative_path'].endswith(file_metadata.path):
                    deal_mapping = mapping_data
                    match_method = "endswith_path"
                    break
        
        # Strategy 4: Fuzzy filename matching (handle version suffixes)
        if not deal_mapping:
            deal_mapping = self._try_fuzzy_filename_match(file_metadata.name)
            if deal_mapping:
                match_method = "fuzzy_filename"
        
        # Strategy 5: Path similarity matching
        if not deal_mapping:
            deal_mapping = self._try_path_similarity_match(file_metadata.path)
            if deal_mapping:
                match_method = "path_similarity"
        
        if not deal_mapping:
            # Expected behavior: ~32% of files don't map to deals (support files, duplicates, etc.)
            # Only log at debug level since this is normal
            self.logger.debug(f"File not mapped to deal: {file_metadata.name} (path: {file_metadata.path})")
            self.logger.debug(f"Expected behavior: ~32% of Salesforce files don't belong to specific deals")
            
            # Add unmapped file metadata for statistics tracking
            doc_metadata.mapping_status = "unmapped"
            doc_metadata.mapping_reason = "no_deal_association"
            return doc_metadata
        
        # Success! Log the match method for monitoring
        self.logger.debug(f"✅ Mapped '{file_metadata.name}' using {match_method} → {deal_mapping['deal_name']}")
        doc_metadata.mapping_status = "mapped"
        doc_metadata.mapping_method = match_method
        
        # Get deal metadata
        deal_name = deal_mapping['deal_name']
        deal_data = self._deal_metadata.get(deal_name)
        
        if not deal_data:
            self.logger.warning(f"No deal metadata found for deal: {deal_name}")
            # Still add basic mapping info
            # Use deal_name for user-friendly ID, keep raw ID in salesforce_deal_id
            user_friendly_id = deal_mapping['deal_name'].replace('Deal-', '')
            doc_metadata.deal_id = user_friendly_id
            doc_metadata.salesforce_deal_id = deal_mapping['deal_id']
            doc_metadata.salesforce_content_version_id = deal_mapping['salesforce_id']
            return doc_metadata
        
        # Enrich with full deal metadata
        # Use deal_name for user-friendly ID, keep raw ID in salesforce_deal_id
        user_friendly_id = deal_mapping['deal_name'].replace('Deal-', '')
        doc_metadata.deal_id = user_friendly_id
        doc_metadata.salesforce_deal_id = deal_mapping['deal_id']
        doc_metadata.deal_subject = deal_data['subject']
        doc_metadata.deal_status = deal_data['status']
        doc_metadata.deal_reason = deal_data['deal_reason']
        doc_metadata.deal_start_date = deal_data['start_date']
        # Add creation date for time-based filtering (from CreatedDate field in deal__cs.csv)
        doc_metadata.deal_creation_date = deal_data.get('creation_date', '')
        doc_metadata.negotiated_by = deal_data['negotiated_by']
        
        # Financial metrics (ENHANCED WITH MISSING FIELDS!)
        doc_metadata.proposed_amount = deal_data['proposed_amount']
        doc_metadata.final_amount = deal_data['final_amount']
        doc_metadata.savings_1yr = deal_data['savings_1yr']
        doc_metadata.savings_3yr = deal_data['savings_3yr']
        doc_metadata.savings_target = deal_data['savings_target']
        doc_metadata.savings_percentage = self._calculate_savings_percentage(
            deal_data['proposed_amount'], 
            deal_data['final_amount']
        )
        
        # MISSING CRITICAL SAVINGS FIELDS (From EDA Analysis!)
        doc_metadata.savings_achieved = deal_data.get('savings_achieved')  # Actual outcomes
        doc_metadata.fixed_savings = deal_data.get('fixed_savings')  # Actual savings amounts
        doc_metadata.savings_target_full_term = deal_data.get('savings_target_full_term')
        doc_metadata.final_amount_full_term = deal_data.get('final_amount_full_term')
        
        # Client/Vendor info - store both raw Salesforce IDs and friendly versions
        raw_client_id = self._to_str_or_none(deal_data['client_id'])
        raw_vendor_id = self._to_str_or_none(deal_data['vendor_id'])
        
        # Always store the raw Salesforce IDs for tracing
        doc_metadata.salesforce_client_id = raw_client_id
        doc_metadata.salesforce_vendor_id = raw_vendor_id
        
        # Set basic vendor/client fields for UI compatibility (legacy path field)
        # Since all our curated data is Salesforce (vendor_id: 0010y00001o6x6YAAQ)
        if raw_vendor_id and raw_vendor_id == '0010y00001o6x6YAAQ':
            doc_metadata.vendor = "Salesforce"  # Set for UI filtering
        elif raw_vendor_id:
            doc_metadata.vendor = f"Vendor-{raw_vendor_id[-8:]}"  # Fallback with last 8 chars
        else:
            doc_metadata.vendor = "Unknown Vendor"
        
        # Simple client placeholder for now
        doc_metadata.client = f"Client-{raw_client_id[-8:]}" if raw_client_id else "Unknown Client"
        
        # Enhanced mapping: Use client/vendor names directly from enhanced mapping file
        # Priority 1: Names from enhanced mapping file (most accurate)
        doc_metadata.client_name = deal_mapping.get('client_name') or None
        doc_metadata.vendor_name = deal_mapping.get('vendor_name') or None
        
        # Priority 2: Fallback to separate mapping files if available and enhanced mapping is empty
        # Note: Salesforce IDs can be 15-char or 18-char; truncate to 15 for consistent matching
        if not doc_metadata.client_name and self._client_mapping and raw_client_id:
            doc_metadata.client_name = self._client_mapping.get(raw_client_id[:15])
        
        if not doc_metadata.vendor_name and self._vendor_mapping and raw_vendor_id:
            doc_metadata.vendor_name = self._vendor_mapping.get(raw_vendor_id[:15])
        
        # Set friendly IDs for filtering/display
        # Use client_name if available, otherwise fallback to shortened raw ID
        if doc_metadata.client_name:
            doc_metadata.client_id = doc_metadata.client_name
        else:
            doc_metadata.client_id = f"Client-{raw_client_id[-8:]}" if raw_client_id else "Unknown"
        
        # Use vendor_name if available, otherwise fallback to shortened raw ID
        if doc_metadata.vendor_name:
            doc_metadata.vendor_id = doc_metadata.vendor_name
        else:
            doc_metadata.vendor_id = f"Vendor-{raw_vendor_id[-8:]}" if raw_vendor_id else "Unknown"
        
        # Contract info
        doc_metadata.contract_term = deal_data['contract_term']
        doc_metadata.contract_start = deal_data['contract_start']
        doc_metadata.contract_end = deal_data['contract_end']
        doc_metadata.effort_level = deal_data['effort_level']
        doc_metadata.has_fmv_report = deal_data['has_fmv_report']
        doc_metadata.deal_origin = deal_data['deal_origin']
        
        # Rich Narrative Content (CRITICAL ADDITION!)
        doc_metadata.current_narrative = deal_data.get('current_narrative')
        doc_metadata.customer_comments = deal_data.get('customer_comments')
        doc_metadata.content_source = "document_file"  # Default for regular files
        
        # Deal Classification Fields (December 2025 - for Pinecone filtering)
        doc_metadata.report_type = self._to_str_or_none(deal_data.get('report_type'))
        doc_metadata.project_type = self._to_str_or_none(deal_data.get('project_type'))
        doc_metadata.competition = self._to_str_or_none(deal_data.get('competition'))
        doc_metadata.npi_analyst = self._to_str_or_none(deal_data.get('npi_analyst'))
        doc_metadata.dual_multi_sourcing = self._to_str_or_none(deal_data.get('dual_multi_sourcing'))
        doc_metadata.time_pressure = self._to_str_or_none(deal_data.get('time_pressure'))
        doc_metadata.advisor_network_used = self._to_str_or_none(deal_data.get('advisor_network_used'))
        
        # Salesforce-specific
        doc_metadata.salesforce_content_version_id = deal_mapping['salesforce_id']
        
        return doc_metadata
    
    def _try_fuzzy_filename_match(self, filename: str) -> Optional[Dict]:
        """Try fuzzy matching for filename with common variations"""
        
        # Remove common version suffixes and try again
        name_without_ext, ext = os.path.splitext(filename)
        
        # Try without common suffixes
        for suffix in ["_v2", "_V2", " v2", " V2", "_v3", "_V3", " v3", " V3", 
                      "_final", "_Final", "_FINAL", "_copy", "_Copy", "_COPY"]:
            if name_without_ext.endswith(suffix):
                base_name = name_without_ext[:-len(suffix)] + ext
                if base_name in self._file_to_deal_mapping:
                    self.logger.debug(f"🔄 Fuzzy match: '{filename}' → '{base_name}'")
                    return self._file_to_deal_mapping[base_name]
        
        # Try without extension
        if name_without_ext in self._file_to_deal_mapping:
            self.logger.debug(f"🔄 Extension match: '{filename}' → '{name_without_ext}'")
            return self._file_to_deal_mapping[name_without_ext]
        
        return None
    
    def _try_path_similarity_match(self, file_path: str) -> Optional[Dict]:
        """Try matching based on path similarity for renamed/moved files"""
        
        # Extract just the filename for backup matching
        filename = os.path.basename(file_path)
        
        # Look for files with same filename in any directory  
        for mapped_filename, mapping_data in self._file_to_deal_mapping.items():
            mapped_path = mapping_data['relative_path']
            mapped_filename_only = os.path.basename(mapped_path)
            
            # If filenames match but paths differ, it might be a moved file
            if mapped_filename_only == filename:
                self.logger.debug(f"🔄 Path similarity match: '{file_path}' → '{mapped_path}' (same filename)")
                return mapping_data
                
        return None
    
    def get_mapping_statistics(self) -> Dict[str, Any]:
        """Get statistics about the file-to-deal mapping coverage"""
        if not self._file_to_deal_mapping:
            return {}
            
        # Count unique deals
        unique_deals = set()
        for mapping in self._file_to_deal_mapping.values():
            unique_deals.add(mapping['deal_name'])
        
        return {
            'total_mapped_files': len(self._file_to_deal_mapping),
            'unique_deals': len(unique_deals),
            'avg_files_per_deal': len(self._file_to_deal_mapping) / len(unique_deals) if unique_deals else 0
        }

    def list_documents(self, folder_path: str = "", 
                      file_types: Optional[List[str]] = None,
                      batch_size: Optional[int] = None) -> Generator[DocumentMetadata, None, None]:
        """
        List all documents with Deal metadata enrichment.
        
        Args:
            folder_path: Subfolder within organized_files_dir (empty for all)
            file_types: File extensions to filter
            batch_size: Not used in this implementation
            
        Yields:
            DocumentMetadata objects enriched with Deal information
        """
        
        search_path = self.organized_files_dir / folder_path if folder_path else self.organized_files_dir
        
        self.logger.info(f"Discovering Salesforce files in: {search_path}")
        
        # Initialize statistics tracking
        mapping_stats = {
            'total_files': 0,
            'mapped_files': 0,
            'unmapped_files': 0,
            'mapping_methods': {}
        }
        
        for root, _, files in os.walk(search_path):
            for filename in files:
                # Skip hidden files and system files
                if filename.startswith('.'):
                    continue
                
                file_path = Path(root) / filename
                
                # Check file type filter
                if file_types:
                    file_ext = file_path.suffix.lower()
                    if file_ext not in file_types:
                        continue
                
                # Skip unsupported file types
                if not self.is_supported_file_type(filename):
                    continue
                
                try:
                    # Get file stats
                    stat = file_path.stat()
                    
                    # Skip files smaller than minimum size (likely icons/logos)
                    if stat.st_size < self.min_file_size_bytes:
                        continue
                    
                    relative_path = str(file_path.relative_to(self.organized_files_dir))
                    
                    # Create basic FileMetadata
                    file_metadata = FileMetadata(
                        path=relative_path,
                        name=filename,
                        size=stat.st_size,
                        modified_time=datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        file_type=file_path.suffix.lower(),
                        source_id=str(file_path),
                        source_type="salesforce",
                        full_source_path=str(file_path),
                        content_hash=None,  # Could add hash calculation here
                        is_downloadable=True
                    )
                    
                    # Enrich with Deal metadata
                    doc_metadata = self._enrich_with_deal_metadata(file_metadata)
                    
                    # Update statistics
                    mapping_stats['total_files'] += 1
                    if doc_metadata.mapping_status == "mapped":
                        mapping_stats['mapped_files'] += 1
                        method = doc_metadata.mapping_method or "unknown"
                        mapping_stats['mapping_methods'][method] = mapping_stats['mapping_methods'].get(method, 0) + 1
                    else:
                        mapping_stats['unmapped_files'] += 1
                    
                    # Log statistics every 1000 files
                    if mapping_stats['total_files'] % 1000 == 0:
                        mapped_pct = (mapping_stats['mapped_files'] / mapping_stats['total_files']) * 100
                        self.logger.info(f"📊 Discovery stats: {mapping_stats['total_files']:,} files processed, "
                                       f"{mapping_stats['mapped_files']:,} mapped ({mapped_pct:.1f}%), "
                                       f"{mapping_stats['unmapped_files']:,} unmapped")
                    
                    # Filter: Skip unmapped files if require_deal_association is True
                    if self.require_deal_association and doc_metadata.mapping_status != "mapped":
                        continue
                    
                    yield doc_metadata
                    
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {e}")
                    continue
    
    def download_file(self, file_path: str) -> bytes:
        """Download file content from local organized files directory"""
        full_path = self.organized_files_dir / file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        
        try:
            with open(full_path, 'rb') as f:
                return f.read()
        except Exception as e:
            raise IOError(f"Error reading file {full_path}: {e}")
    
    def download_document(self, file_path: str) -> bytes:
        """Compatibility method for DocumentProcessor (same as download_file)"""
        return self.download_file(file_path)
    
    def validate_connection(self) -> bool:
        """Validate that organized files directory and mapping files are accessible"""
        try:
            # Check directory access
            if not self.organized_files_dir.exists():
                return False
            
            # Check if we can read mapping files
            if not Path(self.file_mapping_csv).exists():
                return False
            
            if not Path(self.deal_metadata_csv).exists():
                return False
            
            # Try to read a few lines from mapping files
            with open(self.file_mapping_csv, 'r') as f:
                f.readline()
            
            with open(self.deal_metadata_csv, 'r', encoding='latin1') as f:
                f.readline()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Connection validation failed: {e}")
            return False
    
    def get_file_content_hash(self, file_path: str) -> str:
        """Get SHA256 hash of file content"""
        import hashlib
        
        full_path = self.organized_files_dir / file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")
        
        sha256_hash = hashlib.sha256()
        with open(full_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        
        return sha256_hash.hexdigest()
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists in organized files directory"""
        full_path = self.organized_files_dir / file_path
        return full_path.exists()
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get information about the Salesforce file source"""
        return {
            "type": "salesforce",
            "organized_files_dir": str(self.organized_files_dir),
            "file_mapping_csv": self.file_mapping_csv,
            "deal_metadata_csv": self.deal_metadata_csv,
            "client_mapping_csv": self.client_mapping_csv,
            "vendor_mapping_csv": self.vendor_mapping_csv,
            "file_mappings_loaded": len(self._file_to_deal_mapping) if self._file_to_deal_mapping else 0,
            "deal_records_loaded": len(self._deal_metadata) if self._deal_metadata else 0,
            "client_mappings_loaded": len(self._client_mapping) if self._client_mapping else 0,
            "vendor_mappings_loaded": len(self._vendor_mapping) if self._vendor_mapping else 0
        }
    
    def get_deal_narrative_content(self, deal_name: str) -> Dict[str, Optional[str]]:
        """
        Get narrative content for a deal for processing as virtual documents.
        
        Args:
            deal_name: Deal name (e.g., "Deal-36801")
            
        Returns:
            Dict with 'current_narrative' and 'customer_comments' keys
        """
        deal_data = self._deal_metadata.get(deal_name, {})
        
        # Safely get narrative content, handling NaN values
        current_narrative = deal_data.get('current_narrative')
        customer_comments = deal_data.get('customer_comments')
        
        # Handle pandas NaN values
        if pd.isna(current_narrative):
            current_narrative = None
        elif current_narrative:
            current_narrative = str(current_narrative).strip()
            if not current_narrative:  # Empty string after strip
                current_narrative = None
                
        if pd.isna(customer_comments):
            customer_comments = None
        elif customer_comments:
            customer_comments = str(customer_comments).strip()
            if not customer_comments:  # Empty string after strip
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
        for deal_name, deal_data in self._deal_metadata.items():
            # Safely get narrative content, handling NaN values
            current_narrative = deal_data.get('current_narrative', '')
            customer_comments = deal_data.get('customer_comments', '')
            
            # Handle pandas NaN values (which become float)
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
                    'deal_name': deal_name,
                    'deal_data': deal_data,
                    'current_narrative': current_narrative,
                    'customer_comments': customer_comments
                }
