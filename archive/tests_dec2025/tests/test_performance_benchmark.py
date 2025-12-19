#!/usr/bin/env python3
"""
Performance Benchmark Test

Measures processing time, memory usage, and cost to estimate requirements
for 143K document production run.
"""

import os
import sys
import time
import psutil
import pytest
from pathlib import Path
from typing import Dict, Any, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv()

from process_discovered_documents import DiscoveredDocumentProcessor
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings

# Test configuration
TEST_NAMESPACE_PREFIX = "perf_benchmark"
TEST_DISCOVERY_FILE = "test_perf_discovery.json"
NUM_DOCS_TO_PROCESS = 1000
WORKER_COUNTS = [4, 6, 8]  # Test different worker counts


class PerformanceMonitor:
    """Monitor performance metrics during processing"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.start_memory = None
        self.peak_memory = None
        self.process = psutil.Process()
    
    def start(self):
        """Start monitoring"""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory
    
    def update(self):
        """Update peak memory"""
        current_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
    
    def stop(self):
        """Stop monitoring"""
        self.end_time = time.time()
        self.update()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        if not self.start_time or not self.end_time:
            return {}
        
        duration = self.end_time - self.start_time
        memory_delta = self.peak_memory - self.start_memory
        
        return {
            'duration_seconds': duration,
            'duration_minutes': duration / 60,
            'start_memory_mb': self.start_memory,
            'peak_memory_mb': self.peak_memory,
            'memory_delta_mb': memory_delta
        }


@pytest.fixture
def settings():
    """Get application settings"""
    return Settings()


@pytest.fixture
def test_discovery_file():
    """Check if test discovery file exists"""
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


def get_namespace_stats(pinecone_client, namespace: str) -> Dict[str, Any]:
    """Get statistics about vectors in namespace"""
    try:
        stats = pinecone_client.index.describe_index_stats()
        if namespace in stats.namespaces:
            return {
                'vector_count': stats.namespaces[namespace].vector_count
            }
    except Exception as e:
        print(f"   ⚠️  Error getting stats: {e}")
    return {'vector_count': 0}


def test_performance_benchmark(test_discovery_file, pinecone_client):
    """Run performance benchmark with different worker counts"""
    
    print(f"\n🔍 Performance Benchmark Test")
    print(f"   Discovery file: {test_discovery_file}")
    print(f"   Documents to process: {NUM_DOCS_TO_PROCESS}")
    print(f"   Worker counts to test: {WORKER_COUNTS}")
    
    results = []
    
    for worker_count in WORKER_COUNTS:
        namespace = f"{TEST_NAMESPACE_PREFIX}_{worker_count}workers"
        
        print(f"\n{'='*60}")
        print(f"Testing with {worker_count} workers")
        print(f"{'='*60}")
        
        # Clear namespace
        print(f"\n📋 Clearing namespace: {namespace}")
        try:
            pinecone_client.index.delete(delete_all=True, namespace=namespace)
            print(f"   ✅ Namespace cleared")
        except Exception as e:
            print(f"   ⚠️  Namespace may not exist: {e}")
        
        # Get initial vector count
        initial_stats = get_namespace_stats(pinecone_client, namespace)
        initial_vectors = initial_stats['vector_count']
        
        # Start monitoring
        monitor = PerformanceMonitor()
        monitor.start()
        
        # Process documents
        print(f"\n📋 Processing {NUM_DOCS_TO_PROCESS} documents...")
        
        import argparse
        args = argparse.Namespace(
            input=test_discovery_file,
            namespace=namespace,
            export_root_dir=None,
            workers=worker_count,
            parser_backend='docling',
            chunking_strategy='business_aware',
            batch_size=50,
            limit=NUM_DOCS_TO_PROCESS,
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
        
        processor = DiscoveredDocumentProcessor()
        
        try:
            processor.run(args)
            monitor.stop()
            print(f"   ✅ Processing complete")
        except Exception as e:
            monitor.stop()
            print(f"   ⚠️  Processing error: {e}")
            import traceback
            traceback.print_exc()
        
        # Get final stats
        final_stats = get_namespace_stats(pinecone_client, namespace)
        final_vectors = final_stats['vector_count']
        vectors_added = final_vectors - initial_vectors
        
        # Calculate metrics
        perf_stats = monitor.get_stats()
        
        if perf_stats.get('duration_seconds', 0) > 0 and vectors_added > 0:
            time_per_doc = perf_stats['duration_seconds'] / NUM_DOCS_TO_PROCESS
            time_per_chunk = perf_stats['duration_seconds'] / vectors_added if vectors_added > 0 else 0
            vectors_per_second = vectors_added / perf_stats['duration_seconds'] if perf_stats['duration_seconds'] > 0 else 0
        else:
            time_per_doc = 0
            time_per_chunk = 0
            vectors_per_second = 0
        
        result = {
            'workers': worker_count,
            'namespace': namespace,
            'documents_processed': NUM_DOCS_TO_PROCESS,
            'vectors_added': vectors_added,
            'duration_seconds': perf_stats.get('duration_seconds', 0),
            'duration_minutes': perf_stats.get('duration_minutes', 0),
            'time_per_doc_seconds': time_per_doc,
            'time_per_chunk_seconds': time_per_chunk,
            'vectors_per_second': vectors_per_second,
            'start_memory_mb': perf_stats.get('start_memory_mb', 0),
            'peak_memory_mb': perf_stats.get('peak_memory_mb', 0),
            'memory_delta_mb': perf_stats.get('memory_delta_mb', 0)
        }
        
        results.append(result)
        
        # Print results
        print(f"\n📊 Results for {worker_count} workers:")
        print(f"   Duration: {result['duration_minutes']:.2f} minutes ({result['duration_seconds']:.0f} seconds)")
        print(f"   Time per document: {time_per_doc:.2f} seconds")
        print(f"   Time per chunk: {time_per_chunk:.3f} seconds")
        print(f"   Vectors per second: {vectors_per_second:.1f}")
        print(f"   Vectors added: {vectors_added:,}")
        print(f"   Peak memory: {result['peak_memory_mb']:.0f} MB")
        print(f"   Memory delta: {result['memory_delta_mb']:.0f} MB")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"PERFORMANCE BENCHMARK SUMMARY")
    print(f"{'='*60}")
    
    print(f"\n📊 Comparison:")
    print(f"{'Workers':<10} {'Time/Doc':<12} {'Vectors/Sec':<15} {'Peak Memory':<15}")
    print(f"{'-'*60}")
    for result in results:
        print(f"{result['workers']:<10} {result['time_per_doc_seconds']:<12.2f} "
              f"{result['vectors_per_second']:<15.1f} {result['peak_memory_mb']:<15.0f}")
    
    # Estimate for 143K documents
    if results:
        best_result = min(results, key=lambda x: x['time_per_doc_seconds'])
        
        estimated_time_143k_seconds = best_result['time_per_doc_seconds'] * 143000
        estimated_time_143k_hours = estimated_time_143k_seconds / 3600
        estimated_time_143k_days = estimated_time_143k_hours / 24
        
        print(f"\n📈 Estimates for 143,000 documents:")
        print(f"   Using {best_result['workers']} workers (best performance):")
        print(f"   Estimated time: {estimated_time_143k_hours:.1f} hours ({estimated_time_143k_days:.1f} days)")
        print(f"   Estimated vectors: ~{143000 * (best_result['vectors_added'] / NUM_DOCS_TO_PROCESS):,.0f}")
        
        # Cost estimates (rough)
        print(f"\n💰 Rough Cost Estimates:")
        print(f"   EC2 (g4dn.xlarge): ~${0.50 * estimated_time_143k_hours:.2f}")
        print(f"   Embeddings (OpenAI): ~${estimated_time_143k_seconds * 0.0001:.2f} (rough estimate)")
        print(f"   Pinecone storage: ~${(143000 * (best_result['vectors_added'] / NUM_DOCS_TO_PROCESS)) / 1000000 * 0.096:.2f}/month")
    
    # Success criteria
    if results:
        best_time_per_doc = min(r['time_per_doc_seconds'] for r in results)
        assert best_time_per_doc < 10, \
            f"Processing too slow: {best_time_per_doc:.2f} seconds per document"
        
        # Check memory stability (delta should be reasonable)
        for result in results:
            if result['memory_delta_mb'] > 10000:  # >10GB increase
                print(f"\n⚠️  Warning: Large memory increase ({result['memory_delta_mb']:.0f} MB) with {result['workers']} workers")
    
    print(f"\n✅ Performance benchmark complete!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

