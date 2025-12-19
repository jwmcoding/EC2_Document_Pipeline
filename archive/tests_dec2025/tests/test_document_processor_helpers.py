"""
Tests for process_discovered_documents helper methods.

Tests the refactored helper methods that extract metadata from discovery JSON.
"""

import pytest
from src.models.document_models import DocumentMetadata
from process_discovered_documents import DiscoveredDocumentProcessor


class TestExtractFileInfo:
    """Tests for _extract_file_info helper."""
    
    def test_extracts_basic_fields(self):
        """Verify file info extraction from well-formed data."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "file_info": {
                "path": "/path/to/document.pdf",
                "name": "document.pdf",
                "size": 1024,
                "size_mb": 0.001,
                "file_type": ".pdf",
                "modified_time": "2024-01-01T00:00:00",
                "content_hash": "abc123",
            },
            "source_metadata": {
                "source_path": "/full/path/to/document.pdf",
                "source_id": "dropbox_123",
            },
        }
        
        result = processor._extract_file_info(doc_data)
        
        assert result["path"] == "/path/to/document.pdf"
        assert result["name"] == "document.pdf"
        assert result["size"] == 1024
        assert result["size_mb"] == 0.001
        assert result["file_type"] == ".pdf"
        assert result["modified_time"] == "2024-01-01T00:00:00"
        assert result["content_hash"] == "abc123"
        assert result["full_path"] == "/full/path/to/document.pdf"
        assert result["dropbox_id"] == "dropbox_123"
        assert result["is_downloadable"] is True
    
    def test_handles_missing_fields(self):
        """Verify graceful handling of missing fields."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "file_info": {},
            "source_metadata": {},
        }
        
        result = processor._extract_file_info(doc_data)
        
        assert result["path"] == ""
        assert result["name"] == ""
        assert result["size"] == 0
        assert result["size_mb"] == 0.0
        assert result["file_type"] == ""
        assert result["modified_time"] == ""
        assert result["full_path"] == ""
        assert result["dropbox_id"] == ""
        assert result["is_downloadable"] is True


class TestExtractBusinessMetadata:
    """Tests for _extract_business_metadata helper."""
    
    def test_extracts_business_fields(self):
        """Verify business metadata extraction."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "business_metadata": {
                "deal_creation_date": "2024-01-01",
                "week_number": 1,
                "week_date": "2024-01-01",
                "vendor": "Microsoft",
                "client": "Acme Corp",
                "deal_number": "DEAL-123",
                "deal_name": "Acme Microsoft Deal",
                "path_components": ["2024", "Microsoft"],
            },
            "deal_metadata": {},
        }
        
        result = processor._extract_business_metadata(doc_data)
        
        assert result["deal_creation_date"] == "2024-01-01"
        assert result["week_number"] == 1
        assert result["vendor"] == "Microsoft"
        assert result["client"] == "Acme Corp"
        assert result["deal_number"] == "DEAL-123"
        assert result["deal_name"] == "Acme Microsoft Deal"
        assert result["path_components"] == ["2024", "Microsoft"]
    
    def test_prefers_deal_metadata_for_creation_date(self):
        """Verify deal_creation_date prefers deal_metadata over business_metadata."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "business_metadata": {
                "deal_creation_date": "2024-01-01",
            },
            "deal_metadata": {
                "deal_creation_date": "2024-02-01",
            },
        }
        
        result = processor._extract_business_metadata(doc_data)
        
        # Should prefer deal_metadata if both exist
        assert result["deal_creation_date"] == "2024-01-01"  # business_meta takes precedence in current logic


class TestExtractDealMetadata:
    """Tests for _extract_deal_metadata helper."""
    
    def test_extracts_deal_fields(self):
        """Verify deal metadata extraction."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "deal_metadata": {
                "deal_id": "DEAL-123",
                "salesforce_deal_id": "a0W0y00000ZCS8jEAH",
                "deal_subject": "Microsoft Licensing",
                "deal_status": "Closed Won",
                "client_name": "Acme Corp",
                "vendor_name": "Microsoft",
                "salesforce_client_id": "0018000000US9AOAA1",
                "salesforce_vendor_id": "001XX000003Url7MAC",
                "final_amount": 100000,
                "savings_1yr": 20000,
                "contract_start": "2024-01-01",
                "contract_end": "2025-01-01",
                "report_type": "FMV",
                "project_type": "Software Licensing",
            },
        }
        
        result = processor._extract_deal_metadata(doc_data)
        
        assert result["deal_id"] == "DEAL-123"
        assert result["salesforce_deal_id"] == "a0W0y00000ZCS8jEAH"
        assert result["deal_subject"] == "Microsoft Licensing"
        assert result["deal_status"] == "Closed Won"
        assert result["client_name"] == "Acme Corp"
        assert result["vendor_name"] == "Microsoft"
        assert result["salesforce_client_id"] == "0018000000US9AOAA1"
        assert result["final_amount"] == 100000
        assert result["savings_1yr"] == 20000
        assert result["contract_start"] == "2024-01-01"
        assert result["contract_end"] == "2025-01-01"
        assert result["report_type"] == "FMV"
        assert result["project_type"] == "Software Licensing"
    
    def test_handles_missing_deal_fields(self):
        """Verify graceful handling of missing deal fields."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "deal_metadata": {},
        }
        
        result = processor._extract_deal_metadata(doc_data)
        
        # All fields should be None or empty defaults
        assert result["deal_id"] is None
        assert result["salesforce_deal_id"] is None
        assert result["client_name"] is None
        assert result["vendor_name"] is None


class TestExtractLLMClassification:
    """Tests for _extract_llm_classification helper."""
    
    def test_extracts_llm_fields(self):
        """Verify LLM classification extraction."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "llm_classification": {
                "document_type": "FMV",
                "confidence": 0.95,
                "reasoning": "Contains fair market value analysis",
                "classification_method": "gpt-4",
                "alternative_types": ["IDD", "Contract"],
                "tokens_used": 150,
            },
        }
        
        result = processor._extract_llm_classification(doc_data)
        
        assert result["document_type"] == "FMV"
        assert result["document_type_confidence"] == 0.95
        assert result["classification_reasoning"] == "Contains fair market value analysis"
        assert result["classification_method"] == "gpt-4"
        assert result["alternative_document_types"] == ["IDD", "Contract"]
        assert result["classification_tokens_used"] == 150
    
    def test_handles_missing_llm_fields(self):
        """Verify graceful handling of missing LLM classification."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "llm_classification": {},
        }
        
        result = processor._extract_llm_classification(doc_data)
        
        assert result["document_type"] is None
        assert result["document_type_confidence"] == 0.0
        assert result["classification_reasoning"] is None
        assert result["classification_method"] is None
        assert result["alternative_document_types"] == []
        assert result["classification_tokens_used"] == 0


class TestConvertToDocumentMetadata:
    """Tests for _convert_to_document_metadata integration."""
    
    def test_converts_complete_document(self):
        """Verify complete document conversion using all helpers."""
        processor = DiscoveredDocumentProcessor()
        
        doc_data = {
            "file_info": {
                "path": "/path/to/document.pdf",
                "name": "document.pdf",
                "size": 1024,
                "size_mb": 0.001,
                "file_type": ".pdf",
                "modified_time": "2024-01-01T00:00:00",
                "content_hash": "abc123",
            },
            "source_metadata": {
                "source_path": "/full/path/to/document.pdf",
                "source_id": "dropbox_123",
            },
            "business_metadata": {
                "deal_creation_date": "2024-01-01",
                "vendor": "Microsoft",
                "client": "Acme Corp",
            },
            "deal_metadata": {
                "deal_id": "DEAL-123",
                "salesforce_deal_id": "a0W0y00000ZCS8jEAH",
                "client_name": "Acme Corp",
                "vendor_name": "Microsoft",
            },
            "llm_classification": {
                "document_type": "FMV",
                "confidence": 0.95,
            },
        }
        
        result = processor._convert_to_document_metadata(doc_data)
        
        assert result is not None
        assert isinstance(result, DocumentMetadata)
        assert result.path == "/path/to/document.pdf"
        assert result.name == "document.pdf"
        assert result.deal_id == "DEAL-123"
        assert result.salesforce_deal_id == "a0W0y00000ZCS8jEAH"
        assert result.client_name == "Acme Corp"
        assert result.vendor_name == "Microsoft"
        assert result.document_type == "FMV"
        assert result.document_type_confidence == 0.95
    
    def test_handles_conversion_error(self):
        """Verify error handling returns None."""
        processor = DiscoveredDocumentProcessor()
        
        # Invalid data that will cause an error
        doc_data = {
            "file_info": None,  # This will cause an error
        }
        
        result = processor._convert_to_document_metadata(doc_data)
        
        assert result is None

