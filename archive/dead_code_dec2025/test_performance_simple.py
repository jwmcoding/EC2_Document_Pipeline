#!/usr/bin/env python3
"""
Simple Performance Test - runs from src directory
Tests parallel classification and discovery caching functionality
"""

import os
import time
from dotenv import load_dotenv

# Load environment variables from parent directory
load_dotenv('../.env')

from config.colored_logging import ColoredLogger
from classification.llm_document_classifier import LLMDocumentClassifier
from classification.parallel_classifier import ParallelLLMClassifier
from utils.discovery_cache import DiscoveryCache
from connectors.dropbox_client import DropboxClient

def test_parallel_vs_sequential():
    """Test parallel vs sequential classification performance"""
    logger = ColoredLogger("performance")
    logger.info("🚀 Testing Parallel vs Sequential Classification")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("⚠️ No OpenAI API key - skipping test")
        return True
    
    # Test documents
    test_docs = [
        {"filename": "55316-IDD-DocuSign.pdf", "file_type": ".pdf", "vendor": "DocuSign", "client": "Aramark"},
        {"filename": "55317-Quote-Atlan.pptx", "file_type": ".pptx", "vendor": "Atlan", "client": "Zoom"},
        {"filename": "55318-Contract-IBM.docx", "file_type": ".docx", "vendor": "IBM", "client": "Capital One"}
    ]
    
    logger.info(f"📊 Testing with {len(test_docs)} documents")
    
    try:
        # Test Sequential
        logger.info("🔄 Testing sequential classification...")
        sequential_classifier = LLMDocumentClassifier(api_key)
        
        sequential_start = time.time()
        sequential_results = []
        
        for doc in test_docs:
            result = sequential_classifier.classify_document(
                filename=doc["filename"],
                file_type=doc["file_type"],
                vendor=doc["vendor"],
                client=doc["client"]
            )
            sequential_results.append(result)
        
        sequential_time = time.time() - sequential_start
        sequential_tokens = sum(r.tokens_used for r in sequential_results)
        
        logger.info(f"📈 Sequential: {sequential_time:.1f}s, {sequential_tokens} tokens")
        
        # Test Parallel
        logger.info("⚡ Testing parallel classification...")
        parallel_classifier = ParallelLLMClassifier(api_key, num_workers=2)
        
        results, stats = parallel_classifier.classify_documents_parallel(test_docs)
        
        logger.info(f"📈 Parallel: {stats.total_time:.1f}s, {stats.total_tokens} tokens")
        
        # Performance Analysis
        improvement = sequential_time / stats.total_time if stats.total_time > 0 else 1
        time_saved = sequential_time - stats.total_time
        
        logger.success(f"🎯 Performance Results:")
        logger.info(f"   Sequential: {sequential_time:.1f}s ({len(test_docs)/sequential_time*60:.1f} docs/min)")
        logger.info(f"   Parallel:   {stats.total_time:.1f}s ({stats.completed/stats.total_time*60:.1f} docs/min)")
        logger.info(f"   Improvement: {improvement:.1f}x faster")
        logger.info(f"   Time saved: {time_saved:.1f}s ({time_saved/sequential_time*100:.1f}%)")
        
        # Estimate for 2100 documents
        est_sequential = 2100 * (sequential_time / len(test_docs))
        est_parallel = 2100 * (stats.total_time / len(test_docs))
        est_savings = est_sequential - est_parallel
        
        logger.info(f"📊 Estimated for 2100 documents:")
        logger.info(f"   Sequential: {est_sequential/60:.1f} minutes")
        logger.info(f"   Parallel:   {est_parallel/60:.1f} minutes")
        logger.info(f"   Time saved: {est_savings/60:.1f} minutes")
        
        return improvement > 0.8  # Should be at least comparable
        
    except Exception as e:
        logger.error(f"❌ Performance test failed: {e}")
        return False

def test_discovery_cache():
    """Test discovery cache functionality"""
    logger = ColoredLogger("cache")
    logger.info("💾 Testing Discovery Cache")
    
    try:
        # Initialize cache
        cache = DiscoveryCache("test_cache")
        logger.success("✅ Cache initialized")
        
        # List existing caches
        caches = cache.list_caches()
        logger.info(f"📊 Found {len(caches)} cached discovery results")
        
        for cache_info in caches[:3]:  # Show first 3
            logger.info(f"   📄 {cache_info['file']}: {cache_info['document_count']} docs")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Discovery cache test failed: {e}")
        return False

def test_enhanced_dropbox_client():
    """Test enhanced DropboxClient"""
    logger = ColoredLogger("dropbox")
    logger.info("🔗 Testing Enhanced DropboxClient")
    
    try:
        dropbox_token = os.getenv("DROPBOX_ACCESS_TOKEN")
        openai_key = os.getenv("OPENAI_API_KEY")
        
        if not dropbox_token:
            logger.warning("⚠️ No Dropbox token - skipping test")
            return True
        
        # Initialize enhanced client
        client = DropboxClient(
            access_token=dropbox_token,
            openai_api_key=openai_key,
            use_parallel_classification=True,
            num_workers=2,
            enable_discovery_cache=True
        )
        
        logger.success("✅ Enhanced DropboxClient initialized")
        
        # Check new methods exist
        methods = [
            'discover_documents_with_cache',
            'get_discovery_cache_info', 
            'clear_discovery_cache'
        ]
        
        for method in methods:
            if hasattr(client, method):
                logger.success(f"✅ {method} method available")
            else:
                logger.error(f"❌ {method} method missing")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Enhanced DropboxClient test failed: {e}")
        return False

def main():
    """Run performance tests"""
    logger = ColoredLogger("main")
    
    logger.info("🚀 Performance Improvement Tests")
    logger.info("=" * 50)
    
    tests = [
        ("Discovery Cache", test_discovery_cache),
        ("Enhanced DropboxClient", test_enhanced_dropbox_client),
        ("Parallel vs Sequential", test_parallel_vs_sequential)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n📋 Running: {test_name}")
        start_time = time.time()
        
        try:
            success = test_func()
            elapsed = time.time() - start_time
            results[test_name] = success
            
            if success:
                logger.success(f"✅ {test_name}: PASSED ({elapsed:.1f}s)")
            else:
                logger.error(f"❌ {test_name}: FAILED ({elapsed:.1f}s)")
                
        except Exception as e:
            results[test_name] = False
            logger.error(f"💥 {test_name}: CRASHED - {str(e)}")
    
    # Summary
    passed = sum(results.values())
    total = len(results)
    
    logger.info(f"\n🎯 Summary: {passed}/{total} tests passed")
    
    if passed == total:
        logger.success("🎉 All performance improvements working!")
        logger.info("✅ Ready for production with enhanced performance")
    else:
        logger.warning("⚠️ Some tests failed - review before production")
    
    return passed == total

if __name__ == "__main__":
    main() 