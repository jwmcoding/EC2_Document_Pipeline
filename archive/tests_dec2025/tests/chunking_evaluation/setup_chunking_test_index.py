#!/usr/bin/env python3
"""
Setup Chunking Test Index

Enhanced version of the real document test script that creates a proper test index
with documents processed using both chunking strategies for A/B testing.
"""

import os
import sys
import json
import time
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
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings
import argparse

def create_test_discovery(sample_size: int = 50):
    """Create a discovery file with real documents from the external drive"""
    
    logger = ColoredLogger("test_discovery")
    
    # Define test path - using a broader path for more diverse documents
    test_path = "/Volumes/Jeff_2TB/2024 Deal Docs"
    
    if not Path(test_path).exists():
        logger.error(f"❌ Test path not found: {test_path}")
        logger.info("Available paths:")
        base_path = Path("/Volumes/Jeff_2TB")
        if base_path.exists():
            for item in base_path.iterdir():
                if item.is_dir():
                    logger.info(f"   📁 {item}")
        return None
    
    # Create discovery file
    discovery_file = f"chunking_test_discovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    logger.info(f"🔍 Creating test discovery from: {test_path}")
    logger.info(f"📊 Sample size: {sample_size} documents")
    
    # Use our discover_documents.py to create the discovery
    sys.argv = [
        'discover_documents.py',
        '--source', 'local',
        '--path', test_path,
        '--output', discovery_file,
        '--max-docs', str(sample_size)
    ]
    
    try:
        discover_main()
        
        if Path(discovery_file).exists():
            # Load and check the discovery
            with open(discovery_file, 'r') as f:
                data = json.load(f)
            
            total_docs = data.get('discovery_metadata', {}).get('total_documents', 0)
            logger.success(f"✅ Test discovery created: {total_docs} documents")
            
            # Show document types
            documents = data.get('documents', [])
            type_counts = {}
            for doc in documents:
                doc_type = doc.get('document_type', 'Unknown')
                type_counts[doc_type] = type_counts.get(doc_type, 0) + 1
            
            logger.info("📊 Document type distribution:")
            for doc_type, count in sorted(type_counts.items()):
                logger.info(f"   {doc_type}: {count}")
            
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
    """Create a test Pinecone index for chunking comparison"""
    
    logger = ColoredLogger("test_index")
    settings = Settings()
    
    # Use main pinecone client to manage test index
    pinecone_client = PineconeDocumentClient(
        settings.PINECONE_API_KEY,
        index_name="business-documents",  # We'll override this for operations
        environment=settings.PINECONE_ENVIRONMENT
    )
    
    # Test index configuration
    test_index_name = "chunking-strategy-comparison"
    
    try:
        # Check if test index exists
        index_info = pinecone_client.pc.describe_index(test_index_name)
        logger.info(f"✅ Test index '{test_index_name}' already exists")
        
        # For non-interactive mode, just use existing index
        logger.info("📋 Using existing test index (non-interactive mode)")
        if False:  # Skip recreation for now
            logger.info(f"🗑️ Deleting existing test index...")
            pinecone_client.pc.delete_index(test_index_name)
            
            # Wait for deletion
            logger.info("⏳ Waiting for deletion to complete...")
            time.sleep(15)
            
            # Create new index
            _create_new_index(pinecone_client, test_index_name, logger)
        else:
            logger.info("📋 Using existing test index")
            
        return test_index_name
        
    except Exception as e:
        if "not found" in str(e).lower():
            # Index doesn't exist, create it
            return _create_new_index(pinecone_client, test_index_name, logger)
        else:
            logger.error(f"❌ Error checking test index: {e}")
            return None

def _create_new_index(pinecone_client, test_index_name, logger):
    """Create a new test index"""
    
    logger.info(f"🔨 Creating test index '{test_index_name}'...")
    
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
        
        # Wait for index to be ready
        logger.info("⏳ Waiting for index to be ready...")
        time.sleep(30)
        
        logger.success(f"✅ Test index '{test_index_name}' created successfully")
        return test_index_name
        
    except Exception as e:
        logger.error(f"❌ Failed to create test index: {e}")
        return None

def process_documents_with_both_strategies(discovery_file: str, test_index: str):
    """Process documents with both chunking strategies"""
    
    logger = ColoredLogger("chunking_test")
    logger.info("🧪 Processing documents with both chunking strategies")
    
    # Load discovery to see what we have
    with open(discovery_file, 'r') as f:
        data = json.load(f)
    
    total_docs = data.get('discovery_metadata', {}).get('total_documents', 0)
    logger.info(f"📄 Processing {total_docs} real documents")
    
    # Override the Pinecone index name for testing
    original_index = os.environ.get('PINECONE_INDEX_NAME')
    os.environ['PINECONE_INDEX_NAME'] = test_index
    
    strategies = ['business_aware', 'semantic']
    results = {'business_aware': 0, 'semantic': 0}
    
    for strategy in strategies:
        logger.info(f"📝 Processing with {strategy} chunking strategy...")
        
        # Create arguments for the processor
        args = argparse.Namespace()
        args.input = discovery_file
        args.chunking_strategy = strategy
        args.namespace = f"{strategy}-test"  # Use strategy-specific namespaces
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
        
        # Track processing time
        start_time = time.time()
        
        try:
            # Create and run processor
            processor = DiscoveredDocumentProcessor()
            processor.run(args)
            
            processing_time = time.time() - start_time
            logger.success(f"✅ {strategy} processing completed in {processing_time:.2f}s")
            
            # Count chunks in the namespace (simplified approach)
            try:
                settings = Settings()
                test_client = PineconeDocumentClient(
                    settings.PINECONE_API_KEY,
                    index_name=test_index,
                    environment=settings.PINECONE_ENVIRONMENT
                )
                
                # Get index stats
                stats = test_client.index.describe_index_stats()
                namespace_stats = stats.get('namespaces', {})
                chunk_count = namespace_stats.get(f"{strategy}-test", {}).get('vector_count', 0)
                results[strategy] = chunk_count
                
                logger.info(f"📊 {strategy}: {chunk_count} chunks created")
                
            except Exception as e:
                logger.warning(f"⚠️ Could not get chunk count for {strategy}: {e}")
                
        except Exception as e:
            logger.error(f"❌ {strategy} processing failed: {e}")
            import traceback
            traceback.print_exc()
    
    # Restore original index
    if original_index:
        os.environ['PINECONE_INDEX_NAME'] = original_index
    
    return results

def verify_test_index(test_index: str):
    """Verify the test index has data in both namespaces"""
    
    logger = ColoredLogger("verification")
    logger.info("🔍 Verifying test index data...")
    
    try:
        settings = Settings()
        pinecone_client = PineconeDocumentClient(
            settings.PINECONE_API_KEY,
            index_name=test_index,
            environment=settings.PINECONE_ENVIRONMENT
        )
        
        stats = pinecone_client.index.describe_index_stats()
        namespaces = stats.get('namespaces', {})
        
        business_count = namespaces.get('business_aware-test', {}).get('vector_count', 0)
        semantic_count = namespaces.get('semantic-test', {}).get('vector_count', 0)
        
        logger.info(f"📊 Test index statistics:")
        logger.info(f"   business_aware-test namespace: {business_count} chunks")
        logger.info(f"   semantic-test namespace: {semantic_count} chunks")
        
        if business_count > 0 and semantic_count > 0:
            logger.success("✅ Test index ready for comparison testing!")
            return True
        else:
            logger.error("❌ Test index missing data in one or both namespaces")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error verifying test index: {e}")
        return False

def save_test_info(discovery_file: str, test_index: str, results: dict):
    """Save test information for reference"""
    
    test_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    test_info = {
        'test_id': test_id,
        'created_at': datetime.now().isoformat(),
        'discovery_file': discovery_file,
        'test_index_name': test_index,
        'chunk_counts': results,
        'namespaces': {
            'business_aware': 'business_aware-test',
            'semantic': 'semantic-test'
        }
    }
    
    info_file = f"test_index_info_{test_id}.json"
    with open(info_file, 'w') as f:
        json.dump(test_info, f, indent=2, default=str)
    
    print(f"💾 Test info saved to: {info_file}")

def main():
    """Main setup function"""
    
    print("🧪 Chunking Strategy Test Index Setup")
    print("=" * 50)
    
    # Get sample size from user
    try:
        sample_size = int(input("Enter number of documents to process (default 50): ") or "50")
    except ValueError:
        sample_size = 50
    
    print(f"\nSetting up test index with {sample_size} documents...")
    
    try:
        # Step 1: Create discovery
        print("\n📋 Step 1: Creating document discovery...")
        discovery_file = create_test_discovery(sample_size)
        if not discovery_file:
            print("❌ Failed to create discovery. Exiting.")
            return
        
        # Step 2: Create test index
        print("\n🔨 Step 2: Setting up test index...")
        test_index = create_test_index()
        if not test_index:
            print("❌ Failed to create test index. Exiting.")
            return
        
        # Step 3: Process documents with both strategies
        print("\n⚙️ Step 3: Processing documents with both chunking strategies...")
        results = process_documents_with_both_strategies(discovery_file, test_index)
        
        # Step 4: Verify the test index
        print("\n🔍 Step 4: Verifying test index...")
        if verify_test_index(test_index):
            # Step 5: Save test info
            save_test_info(discovery_file, test_index, results)
            
            print("\n🎉 Test index setup complete!")
            print(f"📊 Test index: {test_index}")
            print(f"📊 Business-aware chunks: {results.get('business_aware', 0)}")
            print(f"📊 Semantic chunks: {results.get('semantic', 0)}")
            print(f"📊 Ready for retrieval comparison testing!")
            
            # Ask about cleanup
            cleanup = input(f"\nKeep discovery file '{discovery_file}'? (y/N): ").lower().strip()
            if cleanup != 'y':
                Path(discovery_file).unlink(missing_ok=True)
                print(f"🗑️ Discovery file deleted")
            
        else:
            print("❌ Test index verification failed. Check the logs.")
            
    except KeyboardInterrupt:
        print("\n\n⏹️ Setup interrupted by user")
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 