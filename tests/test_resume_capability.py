#!/usr/bin/env python3
"""
Test Resume Capability

Verifies that processing can be interrupted and resumed without duplicate processing or data loss.
"""

import os
import sys
import json
import time
import signal
import pytest
from pathlib import Path
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from process_discovered_documents import DiscoveredDocumentProcessor
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings

# Test configuration
TEST_NAMESPACE = "test_resume_capability"
TEST_DISCOVERY_FILE = "test_resume_discovery.json"
NUM_DOCS_TO_PROCESS = 100
INTERRUPT_AFTER = 50  # Interrupt after processing 50 docs

# Export root directory - can be overridden via environment variable
# Defaults to EC2 path, but can be set for local testing
EXPORT_ROOT_DIR = os.getenv("EXPORT_ROOT_DIR", "/data/august-2024")

# Export root directory - can be overridden via environment variable
# Defaults to EC2 path, but can be set for local testing
EXPORT_ROOT_DIR = os.getenv("EXPORT_ROOT_DIR", "/data/august-2024")


class InterruptHandler:
    """Handler for simulating interrupt"""
    def __init__(self):
        self.interrupted = False
        self.doc_count = 0
    
    def should_interrupt(self, doc_count: int) -> bool:
        """Check if we should interrupt"""
        if doc_count >= INTERRUPT_AFTER and not self.interrupted:
            self.interrupted = True
            return True
        return False


@pytest.fixture
def settings():
    """Get application settings"""
    return Settings()


@pytest.fixture
def test_discovery_file(tmp_path):
    """Create a test discovery file with 100 documents"""
    # This would need to be populated with actual test documents
    # For now, we'll skip if file doesn't exist
    discovery_path = Path(TEST_DISCOVERY_FILE)
    if not discovery_path.exists():
        pytest.skip(f"Test discovery file not found: {TEST_DISCOVERY_FILE}")
    return str(discovery_path)


@pytest.fixture
def pinecone_client(settings):
    """Initialize Pinecone client"""
    if not settings.PINECONE_API_KEY:
        pytest.skip("PINECONE_API_KEY not set in environment")
    
    client = PineconeDocumentClient(
        api_key=settings.PINECONE_API_KEY,
        index_name=settings.PINECONE_INDEX_NAME
    )
    return client


def get_vectors_in_namespace(pinecone_client, namespace: str) -> List[str]:
    """Get all vector IDs in a namespace"""
    vector_ids = []
    try:
        for ids_batch in pinecone_client.index.list(namespace=namespace, limit=100):
            if isinstance(ids_batch, str):
                vector_ids.append(ids_batch)
            else:
                vector_ids.extend(ids_batch)
    except Exception as e:
        print(f"   ⚠️  Error listing vectors: {e}")
    return vector_ids


def test_resume_processing(test_discovery_file, pinecone_client):
    """Test that processing can be interrupted and resumed"""
    
    # Skip if export root doesn't exist (for local testing)
    # This test requires access to the actual Salesforce export files
    if not Path(EXPORT_ROOT_DIR).exists():
        pytest.skip(f"Export root directory not found: {EXPORT_ROOT_DIR}. "
                   f"Set EXPORT_ROOT_DIR environment variable or run on EC2.")
    
    print(f"\n🔍 Testing resume capability...")
    print(f"   Discovery file: {test_discovery_file}")
    print(f"   Namespace: {TEST_NAMESPACE}")
    print(f"   Export root: {EXPORT_ROOT_DIR}")
    print(f"   Total docs: {NUM_DOCS_TO_PROCESS}")
    print(f"   Interrupt after: {INTERRUPT_AFTER} docs")
    
    # Step 1: Clear namespace if it exists
    print(f"\n📋 Step 1: Clearing test namespace...")
    try:
        pinecone_client.index.delete(delete_all=True, namespace=TEST_NAMESPACE)
        print(f"   ✅ Namespace cleared")
    except Exception as e:
        print(f"   ⚠️  Namespace may not exist: {e}")
    
    # Step 2: Start processing and interrupt
    print(f"\n📋 Step 2: Starting processing (will interrupt after {INTERRUPT_AFTER} docs)...")
    
    processor = DiscoveredDocumentProcessor()
    
    # Create args for first run
    import argparse
    args = argparse.Namespace(
        input=test_discovery_file,
        namespace=TEST_NAMESPACE,
        export_root_dir=EXPORT_ROOT_DIR,  # Use configured export root
        workers=2,  # Use fewer workers for test
        parser_backend='docling',
        chunking_strategy='business_aware',
        batch_size=10,
        limit=INTERRUPT_AFTER,  # Limit to interrupt point
        resume=False,
        reprocess=False,
        use_batch=False,
        batch_only=False,
        interactive=False,
        filter_type=None,
        filter_file_type=None,
        filter_vendor=None,
        filter_client=None,
        max_size_mb=None
    )
    
    try:
        processor.run(args)
        print(f"   ✅ Processed {INTERRUPT_AFTER} documents")
    except Exception as e:
        print(f"   ⚠️  Processing error (may be expected): {e}")
    
    # Step 3: Check progress file exists
    print(f"\n📋 Step 3: Checking progress file...")
    progress_file = Path(f"{test_discovery_file}.progress.json")
    
    if not progress_file.exists():
        print(f"   ⚠️  Progress file not found: {progress_file}")
        print(f"   This may be okay if processing completed")
    else:
        print(f"   ✅ Progress file exists")
        with open(progress_file, 'r') as f:
            progress_data = json.load(f)
            print(f"   Processed: {progress_data.get('processed_count', 0)}")
    
    # Step 4: Check vectors in namespace
    print(f"\n📋 Step 4: Checking vectors in namespace...")
    vectors_after_interrupt = get_vectors_in_namespace(pinecone_client, TEST_NAMESPACE)
    print(f"   Vectors after interrupt: {len(vectors_after_interrupt)}")
    
    # Step 5: Resume processing
    print(f"\n📋 Step 5: Resuming processing...")
    
    args.resume = True
    args.limit = NUM_DOCS_TO_PROCESS  # Process all remaining
    
    try:
        processor.run(args)
        print(f"   ✅ Resumed processing")
    except Exception as e:
        print(f"   ⚠️  Resume error: {e}")
    
    # Step 6: Verify final state
    print(f"\n📋 Step 6: Verifying final state...")
    vectors_after_resume = get_vectors_in_namespace(pinecone_client, TEST_NAMESPACE)
    print(f"   Vectors after resume: {len(vectors_after_resume)}")
    
    # Step 7: Check for duplicates
    print(f"\n📋 Step 7: Checking for duplicates...")
    
    # Fetch sample vectors and check for duplicate document paths
    if vectors_after_resume:
        sample_ids = vectors_after_resume[:min(100, len(vectors_after_resume))]
        fetch_results = pinecone_client.index.fetch(
            ids=sample_ids,
            namespace=TEST_NAMESPACE
        )
        
        document_paths = {}
        duplicates = []
        
        for vector_id, vector_data in fetch_results.vectors.items():
            metadata = vector_data.metadata if hasattr(vector_data, 'metadata') else {}
            # Use a combination of file_name and chunk_index to identify duplicates
            doc_key = f"{metadata.get('file_name', '')}_{metadata.get('chunk_index', '')}"
            
            if doc_key in document_paths:
                duplicates.append({
                    'key': doc_key,
                    'vector1': document_paths[doc_key],
                    'vector2': vector_id
                })
            else:
                document_paths[doc_key] = vector_id
        
        print(f"\n📊 Duplicate Check:")
        print(f"   Sample checked: {len(sample_ids)}")
        print(f"   Duplicates found: {len(duplicates)}")
        
        if duplicates:
            print(f"\n⚠️  DUPLICATES FOUND:")
            for dup in duplicates[:5]:
                print(f"   {dup['key']}: {dup['vector1'][:20]}... and {dup['vector2'][:20]}...")
        else:
            print(f"   ✅ No duplicates found in sample")
    
    # Success criteria - check discovery file to see processing status
    with open(test_discovery_file, 'r') as f:
        discovery_data = json.load(f)
    
    processed_count = sum(
        1 for doc in discovery_data.get("documents", [])[:NUM_DOCS_TO_PROCESS]
        if doc.get("processing_status", {}).get("processed", False)
    )
    
    print(f"\n📊 Final Statistics:")
    print(f"   Documents marked as processed: {processed_count}/{NUM_DOCS_TO_PROCESS}")
    print(f"   Vectors after interrupt: {len(vectors_after_interrupt)}")
    print(f"   Vectors after resume: {len(vectors_after_resume)}")
    
    # Success criteria:
    # 1. No duplicates (most important - resume should not create duplicates)
    assert len(duplicates) == 0, \
        f"Found {len(duplicates)} duplicate vectors - resume created duplicates!"
    
    # 2. All documents should be marked as processed after resume
    # This verifies that resume actually processed the remaining documents
    assert processed_count == NUM_DOCS_TO_PROCESS, \
        f"Expected {NUM_DOCS_TO_PROCESS} documents processed, but only {processed_count} are marked as processed. Resume may not have processed remaining documents."
    
    # 3. Vectors should exist
    assert len(vectors_after_resume) > 0, "No vectors found after resume"
    
    # 4. If resume worked, we should have processed more docs than the interrupt point
    if processed_count > INTERRUPT_AFTER:
        print(f"   ✅ Resume successfully processed remaining {processed_count - INTERRUPT_AFTER} documents")
        # Vectors should increase if new docs were processed
        assert len(vectors_after_resume) >= len(vectors_after_interrupt), \
            f"Resume processed docs but vectors didn't increase ({len(vectors_after_resume)} < {len(vectors_after_interrupt)})"
    else:
        # If all docs were processed in first run, that's also valid (but unexpected)
        print(f"   ⚠️  All documents were processed in first run (unexpected but valid)")
    
    print(f"\n✅ Resume capability test passed!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

