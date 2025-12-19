#!/usr/bin/env python3
"""
Real Document Semantic Chunking Test

This script creates a small discovery of actual documents from the external drive,
then processes them with both chunking strategies to compare results in the test index.
"""

import os
import sys
import json
import tempfile
from pathlib import Path
from datetime import datetime

# Load environment and set up imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from discover_documents import main as discover_main
from process_discovered_documents import DiscoveredDocumentProcessor
from src.config.colored_logging import ColoredLogger
import argparse

def create_test_discovery():
    """Create a small discovery file with real documents from the external drive"""
    
    logger = ColoredLogger("test_discovery")
    
    # Define test path - using a subset for focused testing
    test_path = "/Volumes/Jeff_2TB/2024 Deal Docs/Week1-01012024"
    
    if not Path(test_path).exists():
        logger.error(f"❌ Test path not found: {test_path}")
        return None
    
    # Create temporary discovery file
    discovery_file = "test_real_documents_discovery.json"
    
    logger.info(f"🔍 Creating test discovery from: {test_path}")
    
    # Use our discover_documents.py to create the discovery
    sys.argv = [
        'discover_documents.py',
        '--source', 'local',
        '--path', test_path,
        '--output', discovery_file,
        '--max-docs', '10'  # Just test with first 10 documents
    ]
    
    try:
        discover_main()
        
        if Path(discovery_file).exists():
            # Load and check the discovery
            with open(discovery_file, 'r') as f:
                data = json.load(f)
            
            total_docs = data.get('discovery_metadata', {}).get('total_documents', 0)
            logger.success(f"✅ Test discovery created: {total_docs} documents")
            return discovery_file
        else:
            logger.error("❌ Discovery file was not created")
            return None
            
    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}")
        import traceback
        traceback.print_exc()
        return None

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

def test_real_documents_chunking(discovery_file: str, test_index: str):
    """Test both chunking strategies on real documents"""
    
    logger = ColoredLogger("real_doc_test")
    logger.info("🧪 Testing semantic chunking with real business documents")
    
    # Load discovery to see what we have
    with open(discovery_file, 'r') as f:
        data = json.load(f)
    
    total_docs = data.get('discovery_metadata', {}).get('total_documents', 0)
    logger.info(f"📄 Processing {total_docs} real documents")
    
    # Override the Pinecone index name for testing
    original_index = os.environ.get('PINECONE_INDEX_NAME')
    os.environ['PINECONE_INDEX_NAME'] = test_index
    
    strategies = ['business_aware', 'semantic']
    results = []
    
    for strategy in strategies:
        logger.info(f"📝 Testing {strategy} chunking on real documents...")
        
        # Create arguments
        args = argparse.Namespace()
        args.input = discovery_file
        args.chunking_strategy = strategy
        args.namespace = f"real-{strategy}"
        args.limit = None  # Process all discovered documents
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
        
        try:
            # Initialize processor
            processor = DiscoveredDocumentProcessor()
            
            # Run processing
            start_time = datetime.now()
            processor.run(args)
            end_time = datetime.now()
            
            processing_time = (end_time - start_time).total_seconds()
            
            results.append({
                'strategy': strategy,
                'processing_time': processing_time,
                'success': True,
                'namespace': args.namespace
            })
            
            logger.success(f"✅ {strategy} chunking completed in {processing_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"❌ {strategy} chunking failed: {e}")
            import traceback
            traceback.print_exc()
            results.append({
                'strategy': strategy,
                'processing_time': 0,
                'success': False,
                'error': str(e),
                'namespace': args.namespace
            })
    
    # Restore original index name
    if original_index:
        os.environ['PINECONE_INDEX_NAME'] = original_index
    
    return results

def analyze_real_document_results(test_index: str, results: list):
    """Analyze the results from processing real documents"""
    
    logger = ColoredLogger("analysis")
    logger.info("📊 Analyzing real document chunking results")
    
    from src.connectors.pinecone_client import PineconeDocumentClient
    from src.config.settings import Settings
    
    settings = Settings()
    
    try:
        pinecone_client = PineconeDocumentClient(
            settings.PINECONE_API_KEY,
            index_name=test_index,
            environment=settings.PINECONE_ENVIRONMENT
        )
        
        print("\n📈 Real Document Chunking Results")
        print("=" * 60)
        
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
                
                print(f"\n✅ {result['strategy'].upper()} CHUNKING:")
                print(f"   ⏱️  Processing time: {result['processing_time']:.2f} seconds")
                print(f"   📊 Total chunks created: {vector_count}")
                print(f"   🏷️  Namespace: {namespace}")
                
                # Sample some chunks to show the differences
                if vector_count > 0:
                    sample_query = pinecone_client.index.query(
                        vector=[0.0] * 1024,  # Dummy vector for sampling
                        top_k=5,
                        namespace=namespace,
                        include_metadata=True
                    )
                    
                    print(f"   📝 Sample chunks:")
                    for i, match in enumerate(sample_query.get('matches', [])[:3]):
                        metadata = match.get('metadata', {})
                        chunk_length = metadata.get('chunk_length', 0)
                        chunk_type = metadata.get('chunk_type', 'unknown')
                        section_name = metadata.get('section_name', 'unknown')
                        document_name = metadata.get('document_name', 'unknown')
                        
                        print(f"     {i+1}. {document_name}")
                        print(f"        Length: {chunk_length} chars | Type: {chunk_type} | Section: {section_name}")
                        
            except Exception as e:
                print(f"❌ {result['strategy']}: Error getting stats - {e}")
        
        print(f"\n🎯 COMPARISON SUMMARY:")
        successful_results = [r for r in results if r['success']]
        if len(successful_results) >= 2:
            business_result = next((r for r in successful_results if r['strategy'] == 'business_aware'), None)
            semantic_result = next((r for r in successful_results if r['strategy'] == 'semantic'), None)
            
            if business_result and semantic_result:
                time_diff = semantic_result['processing_time'] - business_result['processing_time']
                print(f"- 🚀 Processing time difference: {time_diff:+.2f} seconds (semantic vs business-aware)")
                
                # Get chunk counts for comparison
                stats = pinecone_client.index.describe_index_stats()
                business_chunks = stats.get('namespaces', {}).get(business_result['namespace'], {}).get('vector_count', 0)
                semantic_chunks = stats.get('namespaces', {}).get(semantic_result['namespace'], {}).get('vector_count', 0)
                
                print(f"- 📊 Chunk count comparison: Business-aware={business_chunks}, Semantic={semantic_chunks}")
                if business_chunks > 0 and semantic_chunks > 0:
                    ratio = semantic_chunks / business_chunks
                    print(f"- 📈 Semantic creates {ratio:.1f}x {'more' if ratio > 1 else 'fewer'} chunks than business-aware")
                
                print("- ✅ Both strategies processed real business documents successfully!")
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main test function for real documents"""
    
    print("🧪 Real Document Semantic Chunking Test")
    print("=" * 60)
    print("Testing with actual business documents from external drive")
    
    # Step 1: Create test discovery
    discovery_file = create_test_discovery()
    if not discovery_file:
        print("❌ Cannot proceed without discovery file")
        return
    
    # Step 2: Create test index
    test_index = create_test_index()
    if not test_index:
        print("❌ Cannot proceed without test index")
        return
    
    # Step 3: Test both chunking strategies on real documents
    print(f"\n📝 Processing real documents with both chunking strategies...")
    results = test_real_documents_chunking(discovery_file, test_index)
    
    # Step 4: Analyze results
    analyze_real_document_results(test_index, results)
    
    # Step 5: Cleanup prompt
    print(f"\n🧹 Test completed!")
    print(f"Real document test data is stored in Pinecone index '{test_index}'")
    print("You can examine the results in different namespaces or delete the test index when done.")
    
    cleanup_discovery = input(f"\nDelete discovery file '{discovery_file}'? (y/N): ").lower().strip()
    if cleanup_discovery == 'y':
        try:
            Path(discovery_file).unlink()
            print(f"✅ Discovery file '{discovery_file}' deleted")
        except Exception as e:
            print(f"❌ Failed to delete discovery file: {e}")
    
    cleanup_index = input(f"\nDelete test index '{test_index}'? (y/N): ").lower().strip()
    if cleanup_index == 'y':
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