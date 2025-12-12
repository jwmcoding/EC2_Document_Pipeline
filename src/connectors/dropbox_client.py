"""
Enhanced Dropbox Client with Business Metadata Extraction
Processes files from: 2024 Deal Docs / WeekX-MMDDYYYY / Vendor / Client / Deal-XXXXX-Vendor
"""

import dropbox
import re
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Generator
from dataclasses import dataclass, asdict
import logging

# Import LLM classifier (with try/except for optional dependency)
try:
    from classification.llm_document_classifier import LLMDocumentClassifier
except ImportError:
    try:
        # Fallback for direct execution
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        from classification.llm_document_classifier import LLMDocumentClassifier
    except ImportError:
        LLMDocumentClassifier = None

# Import the new authentication manager
try:
    from connectors.dropbox_auth_manager import DropboxAuthManager
except ImportError:
    try:
        from dropbox_auth_manager import DropboxAuthManager
    except ImportError:
        DropboxAuthManager = None

# Import DocumentMetadata from models instead of defining duplicate
try:
    from models.document_models import DocumentMetadata
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from models.document_models import DocumentMetadata

# REMOVED DUPLICATE CLASS - using models.document_models.DocumentMetadata


class BusinessMetadataExtractor:
    """Extract business metadata from Dropbox folder structure"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Patterns for different path components
        self.year_pattern = re.compile(r'(\d{4})\s*deal\s*docs?', re.IGNORECASE)
        self.week_pattern = re.compile(r'week(\d+)-(\d{2})(\d{2})(\d{4})', re.IGNORECASE)
        self.deal_pattern = re.compile(r'deal[_\-]?(\d+)', re.IGNORECASE)
        
        # Common client name variations to handle
        self.client_name_mappings = {
            'p&g': 'Procter & Gamble',
            'procter_gamble': 'Procter & Gamble',
            'pg': 'Procter & Gamble',
            # Add more mappings as needed
        }
    
    def extract_metadata(self, path_parts: List[str], full_path: str) -> Dict[str, Any]:
        """
        Extract metadata from path components
        Expected structure: / / NPI Data Ownership / 2024 Deal Docs / Week1-01012024 / Atlan / Zoom Video Communications / Deal-55317-Atlan
        Path indices:       [0] [1]                [2]              [3]                [4]    [5]                          [6]
        """
        
        metadata = {
            'year': None,
            'week_number': None,
            'week_date': None,
            'vendor': None,
            'client': None,
            'deal_number': None,
            'deal_name': None,
            'confidence': 0.0,
            'errors': []
        }
        
        try:
            confidence_factors = []
            
            # Extract year from third component (e.g., "2024 Deal Docs") 
            # Note: path_parts[0] is "/", path_parts[1] is "NPI Data Ownership", so we start at index 2
            if len(path_parts) >= 3:
                year_match = self.year_pattern.search(path_parts[2])
                if year_match:
                    metadata['year'] = year_match.group(1)
                    confidence_factors.append(0.2)
                else:
                    metadata['errors'].append(f"Could not extract year from: {path_parts[2]}")
            
            # Extract week and date from fourth component (e.g., "Week1-01012024")
            if len(path_parts) >= 4:
                week_info = self._extract_week_info(path_parts[3])
                if week_info:
                    metadata.update(week_info)
                    confidence_factors.append(0.2)
                else:
                    metadata['errors'].append(f"Could not extract week info from: {path_parts[3]}")
            
            # Extract vendor from fifth component (e.g., "Atlan")
            if len(path_parts) >= 5:
                vendor = self._clean_company_name(path_parts[4])
                if vendor:
                    metadata['vendor'] = vendor
                    confidence_factors.append(0.2)
            
            # Extract client from sixth component (e.g., "Zoom Video Communications")
            if len(path_parts) >= 6:
                client = self._clean_company_name(path_parts[5])
                if client:
                    metadata['client'] = client
                    confidence_factors.append(0.2)
            
            # Extract deal information from seventh component (e.g., "Deal-55317-Atlan")
            if len(path_parts) >= 7:
                deal_info = self._extract_deal_info(path_parts[6])
                if deal_info:
                    metadata.update(deal_info)
                    confidence_factors.append(0.2)
                else:
                    metadata['errors'].append(f"Could not extract deal info from: {path_parts[6]}")
            
            # Calculate overall confidence
            metadata['confidence'] = sum(confidence_factors)
            
            # Cross-validate vendor consistency
            if metadata.get('vendor') and metadata.get('deal_name'):
                vendor_in_deal = metadata['vendor'].lower() in metadata['deal_name'].lower()
                if vendor_in_deal:
                    metadata['confidence'] += 0.1
                else:
                    metadata['errors'].append("Vendor name inconsistency detected")
            
            self.logger.info(f"Extracted metadata with {metadata['confidence']:.1f} confidence: {metadata}")
            
        except Exception as e:
            metadata['errors'].append(f"Metadata extraction failed: {str(e)}")
            self.logger.error(f"Error extracting metadata from {full_path}: {e}")
        
        return metadata
    
    def _extract_week_info(self, week_component: str) -> Optional[Dict[str, Any]]:
        """
        Extract week number and date from components like 'Week1-01012024'
        Returns: {'week_number': 1, 'week_date': '01/01/2024'}
        """
        match = self.week_pattern.search(week_component)
        if match:
            week_num = int(match.group(1))
            month = match.group(2)
            day = match.group(3) 
            year = match.group(4)
            
            # Format as MM/DD/YYYY
            formatted_date = f"{month}/{day}/{year}"
            
            return {
                'week_number': week_num,
                'week_date': formatted_date
            }
        return None
    
    def _extract_deal_info(self, deal_component: str) -> Optional[Dict[str, Any]]:
        """
        Extract deal number and name from components like 'Deal-55344-Accrete'
        Returns: {'deal_number': '55344', 'deal_name': 'Deal-55344-Accrete'}
        """
        deal_match = self.deal_pattern.search(deal_component)
        if deal_match:
            return {
                'deal_number': deal_match.group(1),
                'deal_name': deal_component
            }
        return None
    
    def _clean_company_name(self, name: str) -> str:
        """Clean and normalize company names"""
        if not name:
            return ""
        
        # Remove common path artifacts
        cleaned = name.replace('_', ' ').replace('-', ' ').strip()
        
        # Apply known mappings
        cleaned_lower = cleaned.lower()
        for key, mapped_name in self.client_name_mappings.items():
            if key in cleaned_lower:
                return mapped_name
        
        # Basic title case formatting
        return ' '.join(word.capitalize() for word in cleaned.split())


class DropboxClient:
    """Enhanced Dropbox client with business metadata extraction and LLM classification"""
    
    def __init__(self, access_token: str = None, openai_api_key: Optional[str] = None,
                 refresh_token: Optional[str] = None, app_key: Optional[str] = None, 
                 app_secret: Optional[str] = None, use_auth_manager: bool = True):
        """
        Initialize DropboxClient with enhanced authentication.
        
        Args:
            access_token: Current Dropbox access token
            openai_api_key: OpenAI API key for LLM classification
            refresh_token: OAuth2 refresh token for automatic renewal
            app_key: Dropbox app key for OAuth2 flow
            app_secret: Dropbox app secret for OAuth2 flow
            use_auth_manager: Whether to use enhanced authentication manager
        """
        self.logger = logging.getLogger(__name__)
        self.metadata_extractor = BusinessMetadataExtractor()
        
        # Initialize authentication
        if use_auth_manager and DropboxAuthManager:
            try:
                self.auth_manager = DropboxAuthManager(
                    access_token=access_token,
                    refresh_token=refresh_token,
                    app_key=app_key,
                    app_secret=app_secret
                )
                self.client = self.auth_manager.get_authenticated_client()
                self.logger.info("🔐 Using enhanced authentication manager with auto-refresh capability")
            except Exception as e:
                self.logger.warning(f"⚠️ Enhanced auth failed, falling back to simple token: {e}")
                self.auth_manager = None
                self.client = dropbox.Dropbox(access_token)
        else:
            self.auth_manager = None
            self.client = dropbox.Dropbox(access_token)
            if not DropboxAuthManager:
                self.logger.warning("⚠️ Enhanced authentication manager not available")
        
        # Initialize LLM classifier if API key provided and module available
        self.llm_classifier = None
        if openai_api_key and LLMDocumentClassifier:
            try:
                self.llm_classifier = LLMDocumentClassifier(openai_api_key)
                self.logger.info("✅ Initialized LLM document classifier with GPT-4.1-mini")
            except Exception as e:
                self.logger.error(f"❌ Failed to initialize LLM classifier: {e}")
        elif openai_api_key and not LLMDocumentClassifier:
            self.logger.warning("⚠️ OpenAI API key provided but LLM classifier module not available")
        else:
            self.logger.info("ℹ️ LLM document classification disabled (no API key provided)")
    
    def parse_document_path(self, path: str, size: int = 0, modified_time: str = "", 
                          dropbox_id: str = "", content_hash: str = None) -> DocumentMetadata:
        """Extract comprehensive business metadata from Dropbox path"""
        
        # Basic file information
        file_name = Path(path).name
        file_type = Path(path).suffix.lower()
        path_parts = [part.strip() for part in Path(path).parts if part.strip()]
        
        # Create base metadata
        metadata = DocumentMetadata(
            path=path,
            name=file_name,
            size=size,
            size_mb=0.0,  # Will be calculated in __post_init__
            file_type=file_type,
            modified_time=modified_time,
            path_components=path_parts,
            dropbox_id=dropbox_id,
            content_hash=content_hash
        )
        
        # Extract business metadata using the specialized extractor
        business_metadata = self.metadata_extractor.extract_metadata(path_parts, path)
        
        # Update metadata with extracted business information
        metadata.year = business_metadata.get('year')
        metadata.week_number = business_metadata.get('week_number')
        metadata.week_date = business_metadata.get('week_date')
        metadata.vendor = business_metadata.get('vendor')
        metadata.client = business_metadata.get('client')
        metadata.deal_number = business_metadata.get('deal_number')
        metadata.deal_name = business_metadata.get('deal_name')
        metadata.extraction_confidence = business_metadata.get('confidence', 0.0)
        metadata.parsing_errors = business_metadata.get('errors', [])
        
        # Note: LLM classification moved to processing phase in v3 architecture
        # Classification will be performed during document processing with full content context
        
        return metadata
    
    def list_documents(self, folder_path: str) -> Generator[DocumentMetadata, None, None]:
        """Recursively list all documents in Dropbox folder with rich metadata extraction
        
        CRITICAL BUG FIX: Added folder path validation to prevent empty folder processing
        """
        
        # FOLDER PATH VALIDATION - CRITICAL BUG FIX
        # Note: Now that folder_path is required, empty string is explicit choice
        if folder_path == "":
            self.logger.warning("⚠️  Processing ENTIRE Dropbox root directory!")
            self.logger.warning("This will scan ALL files in your Dropbox account.")
            self.logger.warning("Make sure this is intentional - consider using a specific folder path.")
            self.logger.info("💡 Recommended: Use a specific path like '/NPI Data Ownership/2024 Deal Docs'")
        
        # Additional validation for common mistakes
        if folder_path is None:
            self.logger.error("🚨 CRITICAL: None folder path detected!")
            raise ValueError("Folder path cannot be None. Specify a target folder path.")
        
        if not isinstance(folder_path, str):
            self.logger.error(f"🚨 CRITICAL: Invalid folder path type: {type(folder_path)}")
            raise ValueError(f"Folder path must be a string, got {type(folder_path)}")
        
        # Log the folder being processed for debugging
        self.logger.info(f"📂 Processing folder: '{folder_path}'")
        
        try:
            result = self.client.files_list_folder(folder_path, recursive=True)
            
            while True:
                for entry in result.entries:
                    if isinstance(entry, dropbox.files.FileMetadata):
                        metadata = self.parse_document_path(
                            entry.path_display,
                            entry.size,
                            entry.server_modified.isoformat(),
                            entry.id,
                            getattr(entry, 'content_hash', None)
                        )
                        
                        # Additional safety check: verify document is in expected folder
                        if not entry.path_display.startswith(folder_path):
                            self.logger.warning(
                                f"🚨 SAFETY WARNING: Document outside target folder detected! "
                                f"Expected: {folder_path}, Got: {entry.path_display}"
                            )
                        
                        # Validate metadata quality
                        validation = self.validate_extracted_metadata(metadata)
                        if validation['warnings']:
                            self.logger.warning(f"Metadata validation warnings for {entry.path_display}: {validation['warnings']}")
                        
                        yield metadata
                
                if not result.has_more:
                    break
                    
                result = self.client.files_list_folder_continue(result.cursor)
                
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Dropbox API error: {e}")
            raise
    
    def validate_extracted_metadata(self, metadata: DocumentMetadata) -> Dict[str, Any]:
        """Validate the quality of extracted metadata"""
        
        validation_result = {
            'is_valid': True,
            'warnings': [],
            'suggestions': []
        }
        
        # Check required fields
        required_fields = ['year', 'vendor', 'client']
        missing_fields = [field for field in required_fields 
                         if not getattr(metadata, field)]
        
        if missing_fields:
            validation_result['warnings'].append(f"Missing required fields: {missing_fields}")
        
        # Check date validity
        if metadata.week_date:
            try:
                datetime.strptime(metadata.week_date, "%m/%d/%Y")
            except ValueError:
                validation_result['warnings'].append(f"Invalid date format: {metadata.week_date}")
        
        # Check confidence level
        if metadata.extraction_confidence < 0.5:
            validation_result['warnings'].append("Low extraction confidence - manual review recommended")
        
        # Business logic validations
        if metadata.year and int(metadata.year) < 2020:
            validation_result['warnings'].append(f"Unusual year detected: {metadata.year}")
        
        if metadata.week_number and (metadata.week_number < 1 or metadata.week_number > 53):
            validation_result['warnings'].append(f"Invalid week number: {metadata.week_number}")
        
        validation_result['is_valid'] = len(validation_result['warnings']) == 0
        
        return validation_result
    
    def download_document(self, path: str) -> bytes:
        """Download document content"""
        try:
            _, response = self.client.files_download(path)
            return response.content
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Error downloading {path}: {e}")
            raise
    
    def get_document_stream(self, path: str):
        """Get document as a stream for large files"""
        try:
            metadata, response = self.client.files_download(path)
            return response.content, metadata
        except dropbox.exceptions.ApiError as e:
            self.logger.error(f"Error streaming {path}: {e}")
            raise
    
    def export_metadata_report(self, folder_path: str = "", output_format: str = "csv") -> str:
        """Export comprehensive metadata report for all documents"""
        
        documents = list(self.list_documents(folder_path))
        
        if output_format.lower() == "csv":
            return self._export_to_csv(documents)
        elif output_format.lower() == "json":
            return self._export_to_json(documents)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _export_to_csv(self, documents: List[DocumentMetadata]) -> str:
        """Export metadata to CSV format"""
        import io
        
        output = io.StringIO()
        
        if not documents:
            return "No documents found"
        
        # CSV headers
        fieldnames = [
            'file_name', 'full_path', 'file_size_mb', 'file_type', 'modified_date',
            'year', 'week_number', 'week_date', 'vendor', 'client', 
            'deal_number', 'deal_name', 'extraction_confidence', 'parsing_errors'
        ]
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for doc in documents:
            writer.writerow({
                'file_name': doc.name,
                'full_path': doc.path,
                'file_size_mb': doc.size_mb,
                'file_type': doc.file_type,
                'modified_date': doc.modified_time,
                'year': doc.year,
                'week_number': doc.week_number,
                'week_date': doc.week_date,
                'vendor': doc.vendor,
                'client': doc.client,
                'deal_number': doc.deal_number,
                'deal_name': doc.deal_name,
                'extraction_confidence': f"{doc.extraction_confidence:.2f}",
                'parsing_errors': '; '.join(doc.parsing_errors) if doc.parsing_errors else ""
            })
        
        return output.getvalue()
    
    def _export_to_json(self, documents: List[DocumentMetadata]) -> str:
        """Export metadata to JSON format"""
        
        documents_data = [asdict(doc) for doc in documents]
        return json.dumps(documents_data, indent=2, default=str)
    
    def get_metadata_analytics(self, folder_path: str = "") -> Dict[str, Any]:
        """Generate analytics report on document metadata"""
        
        documents = list(self.list_documents(folder_path))
        
        if not documents:
            return {"error": "No documents found"}
        
        # Basic statistics
        total_docs = len(documents)
        total_size_mb = sum(doc.size_mb for doc in documents)
        
        # Group by various dimensions
        by_vendor = {}
        by_client = {}
        by_week = {}
        by_file_type = {}
        by_year = {}
        
        confidence_scores = []
        error_count = 0
        
        for doc in documents:
            # Vendor analysis
            vendor = doc.vendor or "Unknown"
            by_vendor[vendor] = by_vendor.get(vendor, 0) + 1
            
            # Client analysis
            client = doc.client or "Unknown"
            by_client[client] = by_client.get(client, 0) + 1
            
            # Week analysis
            if doc.week_number:
                by_week[doc.week_number] = by_week.get(doc.week_number, 0) + 1
            
            # File type analysis
            file_type = doc.file_type or "Unknown"
            by_file_type[file_type] = by_file_type.get(file_type, 0) + 1
            
            # Year analysis
            year = doc.year or "Unknown"
            by_year[year] = by_year.get(year, 0) + 1
            
            # Quality metrics
            confidence_scores.append(doc.extraction_confidence)
            if doc.parsing_errors:
                error_count += 1
        
        # Calculate quality metrics
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        high_confidence_docs = sum(1 for score in confidence_scores if score >= 0.8)
        
        return {
            "summary": {
                "total_documents": total_docs,
                "total_size_mb": round(total_size_mb, 2),
                "average_confidence": round(avg_confidence, 3),
                "high_confidence_documents": high_confidence_docs,
                "documents_with_errors": error_count,
                "data_quality_score": round((high_confidence_docs / total_docs) * 100, 1)
            },
            "breakdown": {
                "by_vendor": dict(sorted(by_vendor.items(), key=lambda x: x[1], reverse=True)),
                "by_client": dict(sorted(by_client.items(), key=lambda x: x[1], reverse=True)),
                "by_week": dict(sorted(by_week.items())),
                "by_file_type": by_file_type,
                "by_year": by_year
            },
            "recommendations": self._generate_recommendations(by_vendor, by_client, confidence_scores, error_count)
        }
    
    def _generate_recommendations(self, by_vendor, by_client, confidence_scores, error_count) -> List[str]:
        """Generate recommendations based on metadata analysis"""
        recommendations = []
        
        # Data quality recommendations
        low_confidence_count = sum(1 for score in confidence_scores if score < 0.5)
        if low_confidence_count > 0:
            recommendations.append(f"Review {low_confidence_count} documents with low extraction confidence")
        
        if error_count > 0:
            recommendations.append(f"Fix parsing errors in {error_count} documents")
        
        # Business insights
        if len(by_vendor) > 10:
            recommendations.append("Consider vendor consolidation - many vendors detected")
        
        if len(by_client) > 20:
            recommendations.append("High client diversity - consider client segmentation analysis")
        
        # File organization suggestions
        recommendations.append("Ensure consistent folder naming for better metadata extraction")
        
        return recommendations 
    
    def refresh_authentication(self) -> bool:
        """
        Attempt to refresh Dropbox authentication if using enhanced auth manager.
        
        Returns:
            True if refresh successful or not needed, False if failed
        """
        if not self.auth_manager:
            self.logger.warning("⚠️ No authentication manager available for refresh")
            return False
        
        try:
            if self.auth_manager.token_info.is_expired():
                self.logger.info("🔄 Token expired, attempting refresh...")
                if self.auth_manager.refresh_access_token():
                    # Update client with new token
                    self.client = self.auth_manager.get_authenticated_client()
                    self.logger.info("✅ Authentication refreshed successfully")
                    return True
                else:
                    self.logger.error("❌ Authentication refresh failed")
                    return False
            else:
                self.logger.info("✅ Token still valid, no refresh needed")
                return True
        except Exception as e:
            self.logger.error(f"❌ Error during authentication refresh: {e}")
            return False
    
    def get_authentication_status(self) -> Dict[str, Any]:
        """
        Get current authentication status and health information.
        
        Returns:
            Dictionary with authentication status
        """
        if not self.auth_manager:
            return {
                'auth_type': 'simple_token',
                'auto_refresh_available': False,
                'status': 'limited',
                'message': 'Using simple token authentication - manual updates required',
                'recommendations': ['Upgrade to OAuth2 flow for automatic token refresh']
            }
        
        health = self.auth_manager.check_token_health()
        usage_stats = self.auth_manager.get_usage_stats()
        
        return {
            'auth_type': 'enhanced_oauth2' if health['has_refresh_token'] else 'enhanced_simple',
            'auto_refresh_available': health['can_auto_refresh'],
            'status': 'healthy' if not health['requires_manual_update'] else 'needs_attention',
            'token_health': health,
            'usage_stats': usage_stats,
            'recommendations': usage_stats['recommendations']
        }
    
    def test_connection_with_refresh(self) -> Tuple[bool, Optional[str]]:
        """
        Test connection with automatic token refresh if needed.
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        # First try normal connection
        try:
            account = self.client.users_get_current_account()
            self.logger.info(f"✅ Connection successful for {account.email}")
            return True, None
        except dropbox.exceptions.AuthError as e:
            self.logger.warning(f"⚠️ Authentication error: {e}")
            
            # Try to refresh if auth manager available
            if self.auth_manager:
                self.logger.info("🔄 Attempting token refresh...")
                if self.refresh_authentication():
                    # Retry connection with new token
                    try:
                        account = self.client.users_get_current_account()
                        self.logger.info(f"✅ Connection successful after refresh for {account.email}")
                        return True, None
                    except Exception as retry_e:
                        error_msg = f"Connection failed even after refresh: {retry_e}"
                        self.logger.error(f"❌ {error_msg}")
                        return False, error_msg
                else:
                    error_msg = "Authentication failed and refresh unsuccessful"
                    return False, error_msg
            else:
                error_msg = f"Authentication failed and no refresh capability: {e}"
                return False, error_msg
        except Exception as e:
            error_msg = f"Connection test failed: {e}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg
    
    def setup_oauth_flow(self) -> Optional[str]:
        """
        Set up OAuth2 flow for obtaining refresh token.
        Only available if auth manager is configured with app credentials.
        
        Returns:
            Authorization URL if successful, None if not available
        """
        if not self.auth_manager:
            self.logger.error("❌ OAuth2 flow requires enhanced authentication manager")
            return None
        
        try:
            return self.auth_manager.setup_oauth_flow()
        except Exception as e:
            self.logger.error(f"❌ Failed to setup OAuth2 flow: {e}")
            return None
    
    def complete_oauth_flow(self, authorization_code: str) -> bool:
        """
        Complete OAuth2 flow with authorization code.
        
        Args:
            authorization_code: Code received after user authorization
            
        Returns:
            True if successful, False otherwise
        """
        if not self.auth_manager:
            self.logger.error("❌ OAuth2 flow requires enhanced authentication manager")
            return False
        
        if self.auth_manager.complete_oauth_flow(authorization_code):
            # Update client with new authenticated session
            self.client = self.auth_manager.get_authenticated_client()
            self.logger.info("✅ OAuth2 flow completed - automatic token refresh now available")
            return True
        
        return False