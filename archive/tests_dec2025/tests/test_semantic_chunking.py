#!/usr/bin/env python3
"""
Test Semantic Chunking Implementation

This script tests the new semantic chunking feature by processing a small set of documents
with both chunking strategies and uploading them to a test Pinecone index.
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Load environment and set up imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from process_discovered_documents import DiscoveredDocumentProcessor
from src.config.colored_logging import ColoredLogger

def create_test_index():
    """Create a test Pinecone index for semantic chunking testing"""
    from src.connectors.pinecone_client import PineconeDocumentClient
    from src.config.settings import Settings
    
    settings = Settings()
    pinecone_client = PineconeDocumentClient(
        settings.PINECONE_API_KEY,
        index_name="business-documents",  # We'll override this
        environment=settings.PINECONE_ENVIRONMENT
    )
    
    # Check if test index exists, create if not
    test_index_name = "test-semantic-deal-docs"
    
    try:
        # Try to get index info
        index_info = pinecone_client.pc.describe_index(test_index_name)
        print(f"✅ Test index '{test_index_name}' already exists")
        return test_index_name
    except:
        # Index doesn't exist, create it
        print(f"🔨 Creating test index '{test_index_name}'...")
        try:
            pinecone_client.pc.create_index(
                name=test_index_name,
                dimension=1024,  # multilingual-e5-large dimension
                metric="cosine",
                spec={
                    "serverless": {
                        "cloud": "aws",
                        "region": "us-east-1"
                    }
                }
            )
            print(f"✅ Test index '{test_index_name}' created successfully")
            return test_index_name
        except Exception as e:
            print(f"❌ Failed to create test index: {e}")
            return None

def test_chunking_strategy(strategy_name: str, discovery_file: str, test_index: str, limit: int = 5):
    """Test a specific chunking strategy"""
    
    logger = ColoredLogger(f"test_{strategy_name}")
    logger.info(f"🧪 Testing {strategy_name} chunking strategy")
    
    # Create arguments
    args = argparse.Namespace()
    args.input = discovery_file
    args.chunking_strategy = strategy_name
    args.namespace = f"test-{strategy_name}"
    args.limit = limit
    args.batch_size = 50
    args.reprocess = False
    args.resume = False
    args.filter_type = None
    args.filter_file_type = None
    args.filter_vendor = None
    args.filter_client = None
    args.max_size_mb = None
    args.use_batch = False
    args.batch_only = False
    args.interactive = False
    
    # Override the Pinecone index name for testing
    original_index = os.environ.get('PINECONE_INDEX_NAME')
    os.environ['PINECONE_INDEX_NAME'] = test_index
    
    try:
        # Initialize processor
        processor = DiscoveredDocumentProcessor()
        
        # Run processing
        start_time = datetime.now()
        processor.run(args)
        end_time = datetime.now()
        
        processing_time = (end_time - start_time).total_seconds()
        
        logger.success(f"✅ {strategy_name} chunking completed in {processing_time:.2f} seconds")
        
        return {
            'strategy': strategy_name,
            'processing_time': processing_time,
            'success': True,
            'namespace': args.namespace
        }
        
    except Exception as e:
        logger.error(f"❌ {strategy_name} chunking failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'strategy': strategy_name,
            'processing_time': 0,
            'success': False,
            'error': str(e),
            'namespace': args.namespace
        }
    finally:
        # Restore original index name
        if original_index:
            os.environ['PINECONE_INDEX_NAME'] = original_index

def compare_chunking_results(test_index: str, results: list):
    """Compare the results from different chunking strategies"""
    
    logger = ColoredLogger("comparison")
    logger.info("📊 Comparing chunking results")
    
    from src.connectors.pinecone_client import PineconeDocumentClient
    from src.config.settings import Settings
    
    settings = Settings()
    # Temporarily override index name
    original_index = settings.PINECONE_INDEX_NAME
    settings.PINECONE_INDEX_NAME = test_index
    
    try:
        pinecone_client = PineconeDocumentClient(
            settings.PINECONE_API_KEY,
            index_name=test_index,
            environment=settings.PINECONE_ENVIRONMENT
        )
        
        print("\n📈 Chunking Strategy Comparison")
        print("=" * 50)
        
        for result in results:
            if not result['success']:
                print(f"❌ {result['strategy']}: FAILED ({result.get('error', 'Unknown error')})")
                continue
                
            namespace = result['namespace']
            
            # Get stats for this namespace
            try:
                stats = pinecone_client.index.describe_index_stats()
                namespace_stats = stats.get('namespaces', {}).get(namespace, {})
                vector_count = namespace_stats.get('vector_count', 0)
                
                print(f"✅ {result['strategy']}:")
                print(f"   - Processing time: {result['processing_time']:.2f} seconds")
                print(f"   - Chunks created: {vector_count}")
                print(f"   - Namespace: {namespace}")
                
                # Sample a few chunks to see the differences
                if vector_count > 0:
                    sample_query = pinecone_client.index.query(
                        vector=[0.0] * 1024,  # Dummy vector for sampling
                        top_k=3,
                        namespace=namespace,
                        include_metadata=True
                    )
                    
                    print(f"   - Sample chunks:")
                    for i, match in enumerate(sample_query.get('matches', [])[:2]):
                        metadata = match.get('metadata', {})
                        chunk_length = metadata.get('chunk_length', 0)
                        chunk_type = metadata.get('chunk_type', 'unknown')
                        print(f"     {i+1}. Length: {chunk_length} chars, Type: {chunk_type}")
                        
            except Exception as e:
                print(f"❌ {result['strategy']}: Error getting stats - {e}")
        
        print("\n🎯 Summary:")
        successful_results = [r for r in results if r['success']]
        if len(successful_results) >= 2:
            business_result = next((r for r in successful_results if r['strategy'] == 'business_aware'), None)
            semantic_result = next((r for r in successful_results if r['strategy'] == 'semantic'), None)
            
            if business_result and semantic_result:
                time_diff = semantic_result['processing_time'] - business_result['processing_time']
                print(f"- Semantic chunking took {time_diff:+.2f} seconds compared to business-aware")
                print("- Both strategies successfully processed the documents")
                print("- Check chunk sizes and types above to see the differences")
        
    except Exception as e:
        logger.error(f"❌ Comparison failed: {e}")
    finally:
        settings.PINECONE_INDEX_NAME = original_index

def main():
    """Main test function"""
    
    print("🧪 Semantic Chunking Feature Test")
    print("=" * 50)
    
    # Configuration
    discovery_file = "2023_fresh_discovery.json"
    test_limit = 5  # Process only 5 documents for testing
    
    # Step 1: Validate discovery file
    if not Path(discovery_file).exists():
        print(f"❌ Discovery file not found: {discovery_file}")
        print("Available discovery files:")
        for f in Path(".").glob("*discovery.json"):
            print(f"  - {f.name}")
        return
    
    # Load discovery file to see what we have
    with open(discovery_file, 'r') as f:
        data = json.load(f)
    
    total_docs = data.get('discovery_metadata', {}).get('total_documents', 0)
    print(f"✅ Discovery file loaded: {total_docs} total documents")
    print(f"🎯 Testing with {test_limit} documents")
    
    # Step 2: Create test index
    test_index = create_test_index()
    if not test_index:
        print("❌ Cannot proceed without test index")
        return
    
    # Step 3: Test both chunking strategies
    strategies = ['business_aware', 'semantic']
    results = []
    
    for strategy in strategies:
        print(f"\n📝 Testing {strategy} chunking...")
        result = test_chunking_strategy(strategy, discovery_file, test_index, test_limit)
        results.append(result)
    
    # Step 4: Compare results
    compare_chunking_results(test_index, results)
    
    # Step 5: Cleanup prompt
    print(f"\n🧹 Test completed!")
    print(f"Test data is stored in Pinecone index '{test_index}'")
    print("You can examine the results or delete the test index when done.")
    
    cleanup = input("\nDelete test index now? (y/N): ").lower().strip()
    if cleanup == 'y':
        try:
            from src.connectors.pinecone_client import PineconeDocumentClient
            from src.config.settings import Settings
            settings = Settings()
            pinecone_client = PineconeDocumentClient(
                settings.PINECONE_API_KEY,
                index_name="business-documents",
                environment=settings.PINECONE_ENVIRONMENT
            )
            pinecone_client.pc.delete_index(test_index)
            print(f"✅ Test index '{test_index}' deleted")
        except Exception as e:
            print(f"❌ Failed to delete test index: {e}")

if __name__ == "__main__":
    main() 