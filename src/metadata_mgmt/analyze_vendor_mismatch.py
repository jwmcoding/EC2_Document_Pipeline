#!/usr/bin/env python3
"""
Analyze Vendor ID/Name Mismatches in Pinecone

This script specifically looks for records that have vendor_name but missing vendor_id,
or vice versa, to understand the impact of previous processing attempts.
"""

import sys
import os
import argparse
import random

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from dotenv import load_dotenv
load_dotenv()

from connectors.pinecone_client import PineconeDocumentClient
from config.settings import Settings
from config.colored_logging import setup_colored_logging, ColoredLogger

def analyze_vendor_mismatches(namespace: str, sample_size: int = 3000):
    """Analyze vendor ID/name mismatches"""
    setup_colored_logging()
    logger = ColoredLogger("VendorMismatchAnalyzer")
    
    # Initialize Pinecone client
    settings = Settings()
    pinecone_client = PineconeDocumentClient(settings.PINECONE_API_KEY, settings.PINECONE_INDEX_NAME)
    
    logger.info(f"🔍 Analyzing vendor ID/name mismatches in {namespace}")
    
    # Categories
    categories = {
        'has_both': [],
        'has_id_no_name': [],
        'has_name_no_id': [],
        'has_neither': []
    }
    
    # Sample documents
    batch_size = 1000
    num_batches = sample_size // batch_size
    
    for batch_num in range(num_batches):
        random_vector = [random.random() * 0.01 for _ in range(1024)]
        
        try:
            results = pinecone_client.index.query(
                namespace=namespace,
                vector=random_vector,
                top_k=batch_size,
                include_metadata=True
            )
            
            for match in results.matches:
                metadata = match.metadata or {}
                
                vendor_id = str(metadata.get('vendor_id', '')).strip()
                vendor_name = str(metadata.get('vendor_name', '')).strip()
                
                has_id = vendor_id and vendor_id != 'None' and vendor_id != 'nan'
                has_name = vendor_name and vendor_name != 'None' and vendor_name != 'nan'
                
                record = {
                    'id': match.id[:50],
                    'vendor_id': vendor_id[:30] if has_id else f"EMPTY: '{vendor_id}'",
                    'vendor_name': vendor_name[:30] if has_name else f"EMPTY: '{vendor_name}'",
                    'deal_id': metadata.get('deal_id', '')
                }
                
                if has_id and has_name:
                    categories['has_both'].append(record)
                elif has_id and not has_name:
                    categories['has_id_no_name'].append(record)
                elif not has_id and has_name:
                    categories['has_name_no_id'].append(record)
                else:
                    categories['has_neither'].append(record)
        
        except Exception as e:
            logger.error(f"Batch {batch_num} failed: {e}")
    
    # Print results
    total = sum(len(cat) for cat in categories.values())
    
    print(f"\n🔍 VENDOR ID/NAME MISMATCH ANALYSIS")
    print(f"📊 Total analyzed: {total:,} documents")
    print("="*60)
    
    for category, records in categories.items():
        count = len(records)
        percentage = (count / total * 100) if total > 0 else 0
        
        print(f"\n📈 {category.upper().replace('_', ' ')}: {count:,} ({percentage:.1f}%)")
        
        # Show sample records
        for i, record in enumerate(records[:5]):
            print(f"  {i+1}. {record['id']}...")
            print(f"     vendor_id: {record['vendor_id']}")
            print(f"     vendor_name: {record['vendor_name']}")
            print(f"     deal_id: {record['deal_id']}")
    
    print("="*60)
    
    # Key insights
    has_name_no_id = len(categories['has_name_no_id'])
    has_id_no_name = len(categories['has_id_no_name'])
    
    if has_name_no_id > 0:
        print(f"🎯 {has_name_no_id:,} records have vendor_name but missing vendor_id")
        print("   → These were likely populated by the old script")
    
    if has_id_no_name > 0:
        print(f"🎯 {has_id_no_name:,} records have vendor_id but missing vendor_name")
        print("   → These need vendor_name population")
    
    return categories

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--sample-size", type=int, default=3000)
    
    args = parser.parse_args()
    analyze_vendor_mismatches(args.namespace, args.sample_size)
