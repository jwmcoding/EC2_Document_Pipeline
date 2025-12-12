#!/usr/bin/env python3
"""
Test Metadata Size Limits

Ensures no vectors exceed Pinecone's 40KB metadata limit after removing text field.
Samples vectors and calculates JSON serialized size for each.
"""

import os
import sys
import json
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
SAMPLE_SIZE = 1000  # Sample up to 1000 vectors
METADATA_LIMIT_BYTES = 40960  # 40KB Pinecone limit


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


def calculate_metadata_size(metadata: Dict[str, Any]) -> int:
    """Calculate JSON serialized size of metadata in bytes"""
    try:
        json_str = json.dumps(metadata)
        return len(json_str.encode('utf-8'))
    except Exception as e:
        print(f"   ⚠️  Error calculating metadata size: {e}")
        return 0


def test_metadata_size_under_limit(pinecone_client):
    """Test that all sampled vectors have metadata under 40KB limit"""
    
    print(f"\n🔍 Testing metadata size limits...")
    print(f"   Namespace: {TEST_NAMESPACE}")
    print(f"   Sample size: {SAMPLE_SIZE}")
    print(f"   Limit: {METADATA_LIMIT_BYTES:,} bytes (40KB)")
    
    # Get all vector IDs (or sample)
    try:
        all_ids = []
        for ids_batch in pinecone_client.index.list(namespace=TEST_NAMESPACE, limit=100):
            if isinstance(ids_batch, str):
                all_ids.append(ids_batch)
            else:
                all_ids.extend(ids_batch)
            if len(all_ids) >= SAMPLE_SIZE:
                break
        
        if not all_ids:
            pytest.skip(f"No vectors found in namespace {TEST_NAMESPACE}")
        
        # Sample if we have more than SAMPLE_SIZE
        if len(all_ids) > SAMPLE_SIZE:
            import random
            all_ids = random.sample(all_ids, SAMPLE_SIZE)
        
        print(f"   Found {len(all_ids)} vectors to check")
        
    except Exception as e:
        print(f"   ⚠️  Error listing vectors: {e}")
        # Fallback: use query method
        results = pinecone_client.index.query(
            vector=[0.0] * 1024,
            top_k=min(SAMPLE_SIZE, 100),  # Query limit
            namespace=TEST_NAMESPACE,
            include_metadata=True
        )
        all_ids = [match.id for match in results.matches]
        print(f"   Using query method: {len(all_ids)} vectors")
    
    if not all_ids:
        pytest.skip(f"No vectors found in namespace {TEST_NAMESPACE}")
    
    # Fetch vectors and check metadata size
    metadata_sizes = []
    oversized_vectors = []
    
    # Process in batches
    batch_size = 100
    for i in range(0, len(all_ids), batch_size):
        batch_ids = all_ids[i:i+batch_size]
        
        try:
            fetch_results = pinecone_client.index.fetch(
                ids=batch_ids,
                namespace=TEST_NAMESPACE
            )
            
            for vector_id, vector_data in fetch_results.vectors.items():
                metadata = vector_data.metadata if hasattr(vector_data, 'metadata') else {}
                
                # Calculate size
                size_bytes = calculate_metadata_size(metadata)
                metadata_sizes.append(size_bytes)
                
                if size_bytes > METADATA_LIMIT_BYTES:
                    oversized_vectors.append({
                        'id': vector_id,
                        'size': size_bytes,
                        'size_kb': size_bytes / 1024
                    })
        
        except Exception as e:
            print(f"   ⚠️  Error fetching batch {i//batch_size + 1}: {e}")
            continue
        
        if (i // batch_size + 1) % 10 == 0:
            print(f"   Processed {min(i+batch_size, len(all_ids))}/{len(all_ids)} vectors...")
    
    # Calculate statistics
    if not metadata_sizes:
        pytest.skip("No metadata sizes calculated")
    
    avg_size = sum(metadata_sizes) / len(metadata_sizes)
    min_size = min(metadata_sizes)
    max_size = max(metadata_sizes)
    
    print(f"\n📊 Metadata Size Statistics:")
    print(f"   Average: {avg_size:,.0f} bytes ({avg_size/1024:.2f} KB)")
    print(f"   Min: {min_size:,} bytes ({min_size/1024:.2f} KB)")
    print(f"   Max: {max_size:,} bytes ({max_size/1024:.2f} KB)")
    print(f"   Limit: {METADATA_LIMIT_BYTES:,} bytes ({METADATA_LIMIT_BYTES/1024:.2f} KB)")
    print(f"   Samples checked: {len(metadata_sizes)}")
    
    # Report oversized vectors
    if oversized_vectors:
        print(f"\n⚠️  OVERSIZED VECTORS ({len(oversized_vectors)}):")
        for vec in oversized_vectors[:10]:  # Show first 10
            print(f"   {vec['id'][:30]}...: {vec['size']:,} bytes ({vec['size_kb']:.2f} KB)")
        if len(oversized_vectors) > 10:
            print(f"   ... and {len(oversized_vectors) - 10} more")
    else:
        print(f"\n✅ All vectors under limit!")
    
    # Success criteria: 0% exceed limit
    assert len(oversized_vectors) == 0, \
        f"{len(oversized_vectors)} vectors exceed 40KB limit"
    
    # Warn if average is getting close to limit
    if avg_size > METADATA_LIMIT_BYTES * 0.8:  # >80% of limit
        print(f"\n⚠️  Warning: Average metadata size is {avg_size/METADATA_LIMIT_BYTES*100:.1f}% of limit")


def test_filename_truncation_effective(pinecone_client):
    """Test that filename truncation is working (filenames < 200 chars)"""
    
    print(f"\n🔍 Testing filename truncation...")
    
    # Query sample vectors
    results = pinecone_client.index.query(
        vector=[0.0] * 1024,
        top_k=min(SAMPLE_SIZE, 100),
        namespace=TEST_NAMESPACE,
        include_metadata=True
    )
    
    if not results.matches:
        pytest.skip(f"No vectors found in namespace {TEST_NAMESPACE}")
    
    filename_lengths = []
    long_filenames = []
    
    for match in results.matches:
        metadata = match.metadata if hasattr(match, 'metadata') else {}
        filename = metadata.get('file_name', '')
        
        if filename:
            filename_lengths.append(len(filename))
            if len(filename) > 200:
                long_filenames.append({
                    'id': match.id,
                    'filename': filename[:50] + '...',
                    'length': len(filename)
                })
    
    if not filename_lengths:
        pytest.skip("No filenames found in metadata")
    
    avg_length = sum(filename_lengths) / len(filename_lengths)
    max_length = max(filename_lengths)
    
    print(f"\n📊 Filename Length Statistics:")
    print(f"   Average: {avg_length:.1f} chars")
    print(f"   Max: {max_length} chars")
    print(f"   Limit: 200 chars")
    print(f"   Samples: {len(filename_lengths)}")
    
    if long_filenames:
        print(f"\n⚠️  LONG FILENAMES ({len(long_filenames)}):")
        for fn in long_filenames[:5]:
            print(f"   {fn['filename']}: {fn['length']} chars")
    
    # Success criteria: All filenames < 200 chars
    assert max_length <= 200, \
        f"Some filenames exceed 200 char limit (max: {max_length})"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

