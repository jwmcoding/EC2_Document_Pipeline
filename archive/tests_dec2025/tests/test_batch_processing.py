#!/usr/bin/env python3
"""
Simple test script to debug batch processing workflow
"""

import os
import sys
import json
from pathlib import Path

# Load environment and set up imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from process_discovered_documents import DiscoveredDocumentProcessor
import argparse

def test_batch_processing():
    """Test batch processing step by step"""
    
    print("🧪 Testing Batch Processing Workflow")
    print("=" * 50)
    
    # Step 1: Check discovery file
    discovery_file = "2022_discovery.json"
    if not Path(discovery_file).exists():
        print(f"❌ Discovery file not found: {discovery_file}")
        return
    
    # Load and check discovery data
    with open(discovery_file, 'r') as f:
        data = json.load(f)
    
    print(f"✅ Discovery file loaded: {len(data.get('documents', []))} documents")
    
    # Step 2: Create arguments
    args = argparse.Namespace()
    args.input = discovery_file
    args.batch_only = True
    args.use_batch = False
    args.limit = 3
    args.interactive = False
    args.namespace = 'documents'
    args.batch_size = 50
    args.reprocess = False
    args.resume = False
    args.filter_type = None
    args.filter_file_type = None
    args.filter_vendor = None
    args.filter_client = None
    args.max_size_mb = None
    
    print(f"✅ Arguments configured: batch_only={args.batch_only}, limit={args.limit}")
    
    # Step 3: Initialize processor
    processor = DiscoveredDocumentProcessor()
    print(f"✅ Processor initialized")
    
    # Step 4: Test discovery loading
    from src.utils.discovery_persistence import DiscoveryPersistence
    persistence = DiscoveryPersistence(args.input)
    summary = persistence.get_discovery_summary()
    print(f"✅ Discovery summary: {summary['total_documents']} total documents")
    
    # Step 5: Test document filtering
    documents = persistence.get_unprocessed_documents()
    print(f"✅ Unprocessed documents: {len(documents)}")
    
    # Show first few documents
    for i, doc in enumerate(documents[:3]):
        file_info = doc.get('file_info', {})
        print(f"   Doc {i+1}: {file_info.get('name', 'Unknown')} ({file_info.get('file_type', 'Unknown')})")
    
    # Step 6: Test initialization 
    print("\n🔧 Testing Component Initialization")
    print("-" * 30)
    
    try:
        processor._initialize_clients(summary, args)
        print("✅ Clients initialized successfully")
        
        # Check batch manager
        if hasattr(processor.document_processor, 'batch_manager') and processor.document_processor.batch_manager:
            print("✅ Batch manager is available")
        else:
            print("❌ Batch manager is NOT available")
            
        # Check batch mode
        if hasattr(processor.document_processor, 'batch_mode'):
            print(f"✅ Batch mode: {processor.document_processor.batch_mode}")
        else:
            print("❌ Batch mode not set")
            
    except Exception as e:
        print(f"❌ Initialization error: {e}")
        return
    
    # Step 7: Test batch collection
    print("\n📦 Testing Batch Collection")
    print("-" * 30)
    
    try:
        test_docs = documents[:args.limit]
        print(f"Testing with {len(test_docs)} documents")
        
        if args.batch_only:
            processor._collect_batch_requests_only(test_docs, args)
        else:
            processor._process_documents(test_docs, args)
            
    except Exception as e:
        print(f"❌ Batch collection error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_batch_processing() 