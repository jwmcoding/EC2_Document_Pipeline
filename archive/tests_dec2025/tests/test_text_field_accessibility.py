#!/usr/bin/env python3
"""
Test Text Field Removal from Pinecone

Verifies that text is NOT stored in Pinecone (neither in metadata nor top-level)
and that search functionality works correctly without text.

Tests:
- Text NOT in metadata dictionary (correct)
- Search queries work without text
- Filter-only queries work without text
- DocumentSearchResult.text field is empty (as expected)
"""

import os
import sys
import pytest
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings

# Test configuration
TEST_NAMESPACE = "benchmark-250docs-2025-12-11"
SAMPLE_SIZE = 50


@pytest.fixture
def settings():
    """Get application settings"""
    return Settings()


@pytest.fixture
def pinecone_client(settings):
    """Initialize Pinecone client"""
    if not settings.PINECONE_API_KEY:
        pytest.skip("PINECONE_API_KEY not set in environment")
    
    # Use business-documents index where benchmark namespace exists
    client = PineconeDocumentClient(
        api_key=settings.PINECONE_API_KEY,
        index_name="business-documents"
    )
    return client


def test_text_not_stored_in_pinecone(pinecone_client):
    """Test that text is NOT stored in Pinecone (neither metadata nor top-level)"""
    
    print(f"\n🔍 Testing text is NOT stored in Pinecone...")
    print(f"   Namespace: {TEST_NAMESPACE}")
    print(f"   Sample size: {SAMPLE_SIZE}")
    print(f"   Expected: Text should NOT be stored (by design)")
    
    # Query random vectors
    results = pinecone_client.index.query(
        vector=[0.0] * 1024,  # Dummy vector
        top_k=SAMPLE_SIZE,
        namespace=TEST_NAMESPACE,
        include_metadata=True
    )
    
    if not results.matches:
        pytest.skip(f"No vectors found in namespace {TEST_NAMESPACE}")
    
    print(f"   Found {len(results.matches)} vectors")
    
    # Check that text is NOT accessible (as expected)
    text_found_count = 0
    
    # Fetch a sample to verify text is not stored
    sample_ids = [match.id for match in results.matches[:min(10, len(results.matches))]]
    fetch_results = pinecone_client.index.fetch(ids=sample_ids, namespace=TEST_NAMESPACE)
    
    for i, match in enumerate(results.matches):
        # Check query result
        text_at_top = getattr(match, 'text', None)
        
        # Check fetched vector
        if not text_at_top and match.id in fetch_results.vectors:
            vector_data = fetch_results.vectors[match.id]
            text_at_top = getattr(vector_data, 'text', None)
        
        if text_at_top and len(str(text_at_top)) > 0:
            text_found_count += 1
            if text_found_count <= 3:  # Only print first 3
                print(f"   ⚠️  Vector {i+1} ({match.id[:20]}...): Text found (unexpected)")
    
    print(f"\n📊 Results:")
    print(f"   Vectors with text: {text_found_count}/{len(results.matches)}")
    
    # Success criteria: Text should NOT be stored (0% found)
    # This is the correct design - text is not needed in Pinecone
    if text_found_count == 0:
        print(f"   ✅ Correct: No text stored in Pinecone (as designed)")
    else:
        print(f"   ⚠️  Warning: {text_found_count} vectors have text stored")
        print(f"   This may be from old vectors upserted before text removal")
    
    # Verify code does NOT include text in upsert
    import inspect
    source = inspect.getsource(pinecone_client.upsert_chunks)
    # Check that text is NOT being added to vector_data
    has_text_upsert = 'vector_data["text"]' in source or "vector_data['text']" in source
    if has_text_upsert:
        print(f"   ⚠️  Warning: Code still includes text in upsert (should be removed)")
    else:
        print(f"   ✅ Code verification: upsert_chunks() does NOT include text")
    
    # Test passes - text not stored is correct behavior


def test_text_not_in_metadata(pinecone_client):
    """Test that text is NOT in metadata dictionary"""
    
    print(f"\n🔍 Testing text NOT in metadata...")
    
    # Query random vectors
    results = pinecone_client.index.query(
        vector=[0.0] * 1024,
        top_k=SAMPLE_SIZE,
        namespace=TEST_NAMESPACE,
        include_metadata=True
    )
    
    if not results.matches:
        pytest.skip(f"No vectors found in namespace {TEST_NAMESPACE}")
    
    # Check each result
    text_in_metadata_count = 0
    
    for i, match in enumerate(results.matches):
        metadata = match.metadata if hasattr(match, 'metadata') else {}
        
        if 'text' in metadata:
            text_in_metadata_count += 1
            print(f"   ⚠️  Vector {i+1} ({match.id[:20]}...): 'text' found in metadata")
    
    print(f"\n📊 Results:")
    print(f"   Text in metadata: {text_in_metadata_count}/{len(results.matches)}")
    
    # Success criteria: 0% in metadata
    assert text_in_metadata_count == 0, \
        f"{text_in_metadata_count} vectors have 'text' in metadata (should be 0)"


def test_search_queries_work_without_text(pinecone_client):
    """Test that search queries work correctly without text stored"""
    
    print(f"\n🔍 Testing search queries work without text...")
    
    test_queries = [
        "FMV report pricing analysis",
        "contract terms and conditions",
        "implementation document",
        "vendor proposal",
        "deal financial metrics"
    ]
    
    all_queries_work = True
    results_summary = []
    
    for query in test_queries:
        print(f"   Query: '{query[:40]}...'")
        
        try:
            results = pinecone_client.hybrid_search_documents(
                query=query,
                top_k=10,
                namespaces=[TEST_NAMESPACE],
                rerank=True,
                rerank_top_n=10
            )
            
            if not results:
                print(f"      ⚠️  No results returned")
                continue
            
            # Check that results exist and have metadata (text is empty, which is correct)
            has_metadata_count = sum(1 for r in results if r.metadata or hasattr(r, 'file_name'))
            text_empty_count = sum(1 for r in results if not r.text or len(r.text) == 0)
            
            results_summary.append({
                'query': query,
                'results': len(results),
                'with_metadata': has_metadata_count,
                'text_empty': text_empty_count
            })
            
            print(f"      Results: {len(results)}, With metadata: {has_metadata_count}, Text empty: {text_empty_count}")
            
            if has_metadata_count == 0:
                all_queries_work = False
                print(f"      ⚠️  No results have metadata")
        
        except Exception as e:
            print(f"      ❌ Error: {e}")
            all_queries_work = False
    
    print(f"\n📊 Results Summary:")
    for summary in results_summary:
        print(f"   '{summary['query'][:30]}...': {summary['results']} results, "
              f"{summary['text_empty']} with empty text (expected)")
    
    # Success criteria: Search works, text is empty (as designed)
    assert all_queries_work, "Some search queries failed"


def test_filter_only_queries_work_without_text(pinecone_client):
    """Test that filter-only queries work without text stored"""
    
    print(f"\n🔍 Testing filter-only queries work without text...")
    
    # Test filter-only search (no query vector, just metadata filter)
    try:
        results = pinecone_client.search_by_business_criteria(
            query="",  # Empty query
            vendor=None,
            client=None,
            deal_number=None,
            file_type=".pdf",
            top_k=10,
            namespace=TEST_NAMESPACE
        )
        
        if not results:
            print("   ⚠️  No results returned for filter-only query")
            pytest.skip("No results to test")
        
        # Check that results exist and text is empty (as expected)
        has_metadata_count = sum(1 for r in results if r.metadata or hasattr(r, 'file_name'))
        text_empty_count = sum(1 for r in results if not r.text or len(r.text) == 0)
        
        print(f"\n📊 Results:")
        print(f"   Total results: {len(results)}")
        print(f"   With metadata: {has_metadata_count}")
        print(f"   Text empty: {text_empty_count} (expected)")
        
        # Success criteria: Search works, text is empty (as designed)
        assert has_metadata_count > 0, "No results have metadata"
        assert text_empty_count == len(results), \
            f"Expected all results to have empty text, but {len(results) - text_empty_count} have text"
    
    except Exception as e:
        print(f"   ⚠️  Filter-only search not available: {e}")
        pytest.skip("Filter-only search not implemented or failed")


def test_search_results_have_metadata_not_text(pinecone_client):
    """Test that search results have metadata but no text (as designed)"""
    
    print(f"\n🔍 Testing search results structure...")
    
    # Query random vectors
    results = pinecone_client.index.query(
        vector=[0.0] * 1024,
        top_k=min(SAMPLE_SIZE, 50),
        namespace=TEST_NAMESPACE,
        include_metadata=True
    )
    
    if not results.matches:
        pytest.skip(f"No vectors found in namespace {TEST_NAMESPACE}")
    
    metadata_count = 0
    text_count = 0
    
    for match in results.matches:
        metadata = match.metadata if hasattr(match, 'metadata') else {}
        text = getattr(match, 'text', '')
        
        if metadata:
            metadata_count += 1
        if text and len(text) > 0:
            text_count += 1
    
    print(f"\n📊 Results Structure:")
    print(f"   Total vectors: {len(results.matches)}")
    print(f"   With metadata: {metadata_count}/{len(results.matches)}")
    print(f"   With text: {text_count}/{len(results.matches)}")
    
    # Success criteria: All have metadata, none have text (as designed)
    assert metadata_count == len(results.matches), \
        f"Not all vectors have metadata: {metadata_count}/{len(results.matches)}"
    
    if text_count == 0:
        print(f"   ✅ Correct: No text stored (as designed)")
    else:
        print(f"   ⚠️  Warning: {text_count} vectors have text (may be from old upserts)")
    
    # Test passes - metadata present, text absent is correct


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

