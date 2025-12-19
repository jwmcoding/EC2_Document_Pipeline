#!/usr/bin/env python3
"""
Test for Metadata Simplification - 26 Field Schema

Tests the new simplified metadata schema by:
1. Loading 10 random documents from enhanced JSON
2. Processing with new 26-field schema
3. Upserting to a test namespace in Pinecone
4. Validating all fields are present and properly formatted

Usage:
    pytest tests/test_metadata_simplification.py -v
    pytest tests/test_metadata_simplification.py::test_metadata_simplification -v -s
"""

import pytest
import json
import random
import sys
import os
from pathlib import Path
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.models.document_models import DocumentMetadata
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings
from src.chunking.semantic_chunker import SemanticChunker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Test configuration
TEST_NAMESPACE = "metadata_simplification_test"
TEST_SAMPLE_SIZE = 10
ENHANCED_JSON_PATH = "/Users/jeffmuscarella/2025_Python/Dropbox/enhanced_salesforce_resume_documents.json"

# Expected 26 fields in simplified schema
EXPECTED_FIELDS = {
    # Core Document (3)
    'file_name', 'file_type', 'deal_creation_date',
    
    # Deal Context (6)
    'deal_id', 'salesforce_deal_id', 'deal_subject', 'deal_status', 'deal_reason', 'deal_start_date',
    
    # Business Relationships (6)
    'client_id', 'client_name', 'salesforce_client_id', 'vendor_id', 'vendor_name', 'salesforce_vendor_id',
    
    # Financial Metrics (7)
    'proposed_amount', 'final_amount', 'savings_1yr', 'savings_3yr', 'savings_target', 
    'savings_achieved', 'savings_target_full_term',
    
    # Rich Content (2)
    'current_narrative', 'customer_comments',
    
    # Email-Specific (1)
    'email_subject',
    
    # Technical (1)
    'chunk_index',
}


@pytest.fixture
def settings():
    """Get application settings"""
    return Settings()


@pytest.fixture
def pinecone_client(settings):
    """Initialize Pinecone client"""
    if not settings.PINECONE_API_KEY:
        pytest.skip("PINECONE_API_KEY not set in environment")
    
    client = PineconeDocumentClient(
        api_key=settings.PINECONE_API_KEY,
        index_name=settings.PINECONE_INDEX_NAME,
        namespace=TEST_NAMESPACE
    )
    return client


@pytest.fixture
def sample_documents() -> List[Dict[str, Any]]:
    """Load and return 10 random documents from enhanced JSON"""
    if not Path(ENHANCED_JSON_PATH).exists():
        pytest.skip(f"Enhanced JSON file not found at {ENHANCED_JSON_PATH}")
    
    with open(ENHANCED_JSON_PATH, 'r') as f:
        all_documents = json.load(f)
    
    # Sample 10 random documents
    sample = random.sample(all_documents, min(TEST_SAMPLE_SIZE, len(all_documents)))
    
    print(f"\n📊 Loaded {len(sample)} sample documents from enhanced JSON")
    for i, doc in enumerate(sample, 1):
        print(f"  {i}. {doc.get('name', 'Unknown')} - Deal: {doc.get('deal_metadata', {}).get('deal_id', 'N/A')}")
    
    return sample


def create_optimized_metadata(doc_data: dict) -> DocumentMetadata:
    """Create optimized DocumentMetadata with new 26-field schema"""
    
    file_info = doc_data.get('file_info', {})
    deal_metadata = doc_data.get('deal_metadata', {})
    
    def safe_float(value) -> float:
        try:
            return float(value) if value else 0.0
        except (TypeError, ValueError):
            return 0.0
    
    # Create metadata with new schema
    metadata = DocumentMetadata(
        # Core Document (3)
        name=doc_data.get('name', ''),
        file_type=file_info.get('file_type', ''),
        deal_creation_date=deal_metadata.get('creation_date', ''),
        
        # Deal Context (6)
        deal_id=deal_metadata.get('deal_id'),
        salesforce_deal_id=deal_metadata.get('salesforce_deal_id', deal_metadata.get('deal_id')),
        deal_subject=deal_metadata.get('subject'),
        deal_status=deal_metadata.get('status'),
        deal_reason=deal_metadata.get('deal_reason'),
        deal_start_date=deal_metadata.get('start_date'),
        
        # Business Relationships (6)
        client_id=deal_metadata.get('client_id'),
        client_name=deal_metadata.get('client_name'),
        salesforce_client_id=deal_metadata.get('salesforce_client_id'),
        vendor_id=deal_metadata.get('vendor_id'),
        vendor_name=deal_metadata.get('vendor_name'),
        salesforce_vendor_id=deal_metadata.get('salesforce_vendor_id'),
        
        # Financial Metrics (7)
        proposed_amount=safe_float(deal_metadata.get('proposed_amount')),
        final_amount=safe_float(deal_metadata.get('final_amount')),
        savings_1yr=safe_float(deal_metadata.get('savings_1yr')),
        savings_3yr=safe_float(deal_metadata.get('savings_3yr')),
        savings_target=safe_float(deal_metadata.get('savings_target')),
        savings_achieved=deal_metadata.get('savings_achieved'),
        savings_target_full_term=safe_float(deal_metadata.get('savings_target_full_term')),
        
        # Rich Content (2)
        current_narrative=deal_metadata.get('current_narrative'),
        customer_comments=deal_metadata.get('customer_comments'),
        
        # Email-Specific (1)
        email_subject=deal_metadata.get('email_subject'),
        
        # Technical (1)
        # chunk_index will be set during chunking
        
        # Required base fields
        path=doc_data.get('path', ''),
        size=file_info.get('size', 0),
        size_mb=file_info.get('size_mb', 0.0),
        modified_time=file_info.get('modified_time', ''),
    )
    
    return metadata


def test_metadata_schema_validation(sample_documents):
    """Test that metadata schema has all required fields"""
    print(f"\n✅ Testing metadata schema with {len(sample_documents)} documents")
    
    for i, doc_data in enumerate(sample_documents, 1):
        metadata = create_optimized_metadata(doc_data)
        
        # Get all non-None fields
        metadata_dict = metadata.__dict__
        populated_fields = {k: v for k, v in metadata_dict.items() if v is not None and v != ''}
        
        print(f"\n  Document {i}: {metadata.name}")
        print(f"    Total fields in schema: {len(metadata_dict)}")
        print(f"    Populated fields: {len(populated_fields)}")
        print(f"    Deal ID: {metadata.deal_id}")
        print(f"    Creation Date: {metadata.deal_creation_date}")
        
        # Assert at least core fields are present
        assert metadata.name, f"Document {i}: name field is empty"
        assert metadata.file_type, f"Document {i}: file_type field is empty"
        assert metadata.deal_id, f"Document {i}: deal_id field is empty"


def test_metadata_field_count(sample_documents):
    """Test that metadata contains expected number of fields"""
    print(f"\n📊 Testing field count across {len(sample_documents)} documents")
    
    total_fields_found = set()
    
    for doc_data in sample_documents:
        metadata = create_optimized_metadata(doc_data)
        
        # Check which expected fields are populated
        for field in EXPECTED_FIELDS:
            value = getattr(metadata, field, None)
            if value is not None and value != '':
                total_fields_found.add(field)
    
    print(f"\n  Expected 26 fields in schema")
    print(f"  Found {len(total_fields_found)} unique populated fields: {sorted(total_fields_found)}")
    
    missing_fields = EXPECTED_FIELDS - total_fields_found
    if missing_fields:
        print(f"  ⚠️  Missing/unpopulated fields: {missing_fields}")
    else:
        print(f"  ✅ All expected fields found!")


def test_pinecone_upsert(pinecone_client, sample_documents):
    """Test upserting documents with new schema to test namespace"""
    print(f"\n🔄 Testing Pinecone upsert with {len(sample_documents)} documents to namespace: {TEST_NAMESPACE}")
    
    # Create semantic chunker
    chunker = SemanticChunker(
        max_chunk_size=500,
        chunk_overlap=75,
        breakpoint_threshold_type="percentile",
        breakpoint_threshold_amount=95
    )
    
    total_chunks = 0
    upserted_docs = 0
    
    for i, doc_data in enumerate(sample_documents, 1):
        metadata = create_optimized_metadata(doc_data)
        
        # Create mock chunks
        content = doc_data.get('content', 'Sample content for testing')[:2000]  # Truncate for testing
        
        try:
            chunks = chunker.chunk(content, metadata.name)
            
            if chunks:
                # Prepare metadata for Pinecone
                metadata_dict = {
                    'file_name': metadata.name or '',
                    'file_type': metadata.file_type or '',
                    'deal_creation_date': metadata.deal_creation_date or '',
                    'deal_id': metadata.deal_id or '',
                    'salesforce_deal_id': metadata.salesforce_deal_id or '',
                    'deal_subject': (metadata.deal_subject or '')[:300],
                    'deal_status': metadata.deal_status or '',
                    'deal_reason': (metadata.deal_reason or '')[:200],
                    'deal_start_date': metadata.deal_start_date or '',
                    'client_id': metadata.client_id or '',
                    'client_name': (metadata.client_name or '')[:100],
                    'salesforce_client_id': metadata.salesforce_client_id or '',
                    'vendor_id': metadata.vendor_id or '',
                    'vendor_name': (metadata.vendor_name or '')[:100],
                    'salesforce_vendor_id': metadata.salesforce_vendor_id or '',
                    'proposed_amount': float(metadata.proposed_amount or 0.0),
                    'final_amount': float(metadata.final_amount or 0.0),
                    'savings_1yr': float(metadata.savings_1yr or 0.0),
                    'savings_3yr': float(metadata.savings_3yr or 0.0),
                    'savings_target': float(metadata.savings_target or 0.0),
                    'savings_achieved': (metadata.savings_achieved or '')[:200],
                    'savings_target_full_term': float(metadata.savings_target_full_term or 0.0),
                    'current_narrative': (metadata.current_narrative or '')[:2000],
                    'customer_comments': (metadata.customer_comments or '')[:2000],
                    'email_subject': (metadata.email_subject or '')[:200],
                    'chunk_index': 0,
                }
                
                # Upsert to Pinecone
                pinecone_client.upsert_chunks(
                    namespace=TEST_NAMESPACE,
                    chunks=chunks,
                    metadata=metadata_dict,
                    document_id=metadata.deal_id or f"doc_{i}"
                )
                
                total_chunks += len(chunks)
                upserted_docs += 1
                
                print(f"  ✅ Document {i}: {metadata.name} → {len(chunks)} chunks")
            
        except Exception as e:
            print(f"  ❌ Document {i}: Error - {str(e)}")
            continue
    
    print(f"\n📈 Summary:")
    print(f"  Documents processed: {upserted_docs}")
    print(f"  Total chunks upserted: {total_chunks}")
    print(f"  Test namespace: {TEST_NAMESPACE}")
    
    # Verify data in Pinecone
    stats = pinecone_client.index.describe_index_stats()
    if TEST_NAMESPACE in stats.namespaces:
        ns_stats = stats.namespaces[TEST_NAMESPACE]
        print(f"  Vectors in test namespace: {ns_stats.get('vector_count', 0)}")
        assert ns_stats.get('vector_count', 0) > 0, "No vectors found in test namespace after upsert"
    
    assert upserted_docs > 0, "No documents were successfully processed"


def test_metadata_field_types(sample_documents):
    """Test that metadata fields have correct types"""
    print(f"\n🔍 Testing field types for {len(sample_documents)} documents")
    
    for i, doc_data in enumerate(sample_documents, 1):
        metadata = create_optimized_metadata(doc_data)
        
        # String fields
        string_fields = ['name', 'file_type', 'deal_creation_date', 'deal_id', 'deal_subject', 
                        'deal_status', 'deal_reason', 'deal_start_date', 'client_name', 
                        'vendor_name', 'savings_achieved', 'current_narrative', 'customer_comments', 'email_subject']
        
        for field in string_fields:
            value = getattr(metadata, field, None)
            if value is not None and value != '':
                assert isinstance(value, str), f"Document {i}: {field} should be str, got {type(value)}"
        
        # Float fields
        float_fields = ['proposed_amount', 'final_amount', 'savings_1yr', 'savings_3yr', 
                       'savings_target', 'savings_target_full_term']
        
        for field in float_fields:
            value = getattr(metadata, field, None)
            if value is not None:
                assert isinstance(value, (int, float)), f"Document {i}: {field} should be float, got {type(value)}"
    
    print(f"  ✅ All field types validated correctly")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

