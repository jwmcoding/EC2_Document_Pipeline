#!/usr/bin/env python3
"""
Check progress of 250-file benchmark test
"""

import os
import sys
from dotenv import load_dotenv

sys.path.insert(0, 'src')

from src.config.settings import Settings
from pinecone import Pinecone

def check_progress(namespace: str):
    """Check progress of test by querying Pinecone namespace"""
    
    load_dotenv()
    
    settings = Settings()
    api_key = os.getenv("PINECONE_API_KEY")
    
    if not api_key:
        print("❌ PINECONE_API_KEY not found")
        return
    
    try:
        pc = Pinecone(api_key=api_key)
        index = pc.Index(settings.PINECONE_INDEX_NAME)
        
        # Get namespace stats
        stats = index.describe_index_stats()
        
        print("=" * 60)
        print("📊 BENCHMARK TEST PROGRESS REPORT")
        print("=" * 60)
        print(f"\nNamespace: {namespace}")
        print(f"Index: {settings.PINECONE_INDEX_NAME}")
        
        if namespace in stats.namespaces:
            ns_stats = stats.namespaces[namespace]
            vector_count = ns_stats.vector_count
            print(f"\n✅ Vectors uploaded: {vector_count:,}")
            
            # Estimate progress (assuming ~13 chunks per document average)
            expected_chunks = 250 * 13  # Rough estimate
            progress_pct = min(100, (vector_count / expected_chunks) * 100)
            print(f"📈 Estimated progress: {progress_pct:.1f}%")
            print(f"   (Assuming ~13 chunks per document)")
            
            # Query a sample to check metadata
            if vector_count > 0:
                print(f"\n🔍 Sampling metadata...")
                results = index.query(
                    vector=[0.0] * 1024,
                    top_k=min(5, vector_count),
                    namespace=namespace,
                    include_metadata=True
                )
                
                if results.matches:
                    sample = results.matches[0]
                    metadata = sample.metadata if hasattr(sample, 'metadata') else {}
                    
                    print(f"\n📋 Sample metadata fields:")
                    print(f"   Total fields: {len(metadata)}")
                    
                    # Check for text field (should NOT be present)
                    has_text = 'text' in metadata
                    if has_text:
                        print(f"   ⚠️  WARNING: 'text' field found in metadata (should be removed)")
                    else:
                        print(f"   ✅ 'text' field NOT in metadata (correct)")
                    
                    # Check filename truncation
                    file_name = metadata.get('file_name', '')
                    if len(file_name) > 200:
                        print(f"   ⚠️  WARNING: filename exceeds 200 chars: {len(file_name)} chars")
                    elif file_name:
                        print(f"   ✅ Filename length: {len(file_name)} chars (within limit)")
                    
                    # Check metadata size
                    import json
                    try:
                        metadata_json = json.dumps(metadata)
                        metadata_size_bytes = len(metadata_json.encode('utf-8'))
                        metadata_size_kb = metadata_size_bytes / 1024
                        print(f"   Metadata size: {metadata_size_bytes:,} bytes ({metadata_size_kb:.2f} KB)")
                        if metadata_size_bytes > 40000:
                            print(f"   ⚠️  WARNING: Metadata exceeds 40KB limit!")
                        else:
                            print(f"   ✅ Metadata within 40KB limit")
                    except Exception as e:
                        print(f"   ⚠️  Could not calculate metadata size: {e}")
                    
                    # Show key fields
                    key_fields = ['file_name', 'deal_id', 'client_name', 'vendor_name', 'chunk_index']
                    print(f"\n   Key fields present:")
                    for field in key_fields:
                        value = metadata.get(field, 'MISSING')
                        if value != 'MISSING':
                            display_value = str(value)[:50] if len(str(value)) > 50 else str(value)
                            print(f"      ✅ {field}: {display_value}")
                        else:
                            print(f"      ❌ {field}: MISSING")
        else:
            print(f"\n⚠️  Namespace not found (may be empty or test not started)")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    namespace = "benchmark-250docs-2025-12-11"
    check_progress(namespace)

