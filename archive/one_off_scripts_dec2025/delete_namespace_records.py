#!/usr/bin/env python3
"""
Delete all records from a Pinecone namespace
"""

import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, 'src')

from src.config.settings import Settings
from pinecone import Pinecone

def delete_namespace(namespace: str):
    """Delete all records from a Pinecone namespace"""
    
    load_dotenv()
    
    settings = Settings()
    api_key = os.getenv("PINECONE_API_KEY")
    
    if not api_key:
        print("❌ PINECONE_API_KEY not found in environment")
        return False
    
    print(f"🗑️  Deleting all records from namespace: {namespace}")
    print(f"   Index: {settings.PINECONE_INDEX_NAME}")
    
    try:
        pc = Pinecone(api_key=api_key)
        index = pc.Index(settings.PINECONE_INDEX_NAME)
        
        # Get stats before deletion
        stats = index.describe_index_stats()
        if namespace in stats.namespaces:
            vector_count = stats.namespaces[namespace].vector_count
            print(f"   Found {vector_count:,} vectors in namespace")
        else:
            print(f"   ⚠️  Namespace not found (may be empty)")
            return True
        
        # Delete all vectors in namespace
        print(f"   Deleting...")
        index.delete(delete_all=True, namespace=namespace)
        
        # Verify deletion
        stats_after = index.describe_index_stats()
        if namespace in stats_after.namespaces:
            remaining = stats_after.namespaces[namespace].vector_count
            if remaining > 0:
                print(f"   ⚠️  Warning: {remaining} vectors still remain")
                return False
        else:
            print(f"   ✅ Namespace cleared successfully")
            return True
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    namespace = "benchmark-250docs-2025-12-11"
    
    if len(sys.argv) > 1:
        namespace = sys.argv[1]
    
    success = delete_namespace(namespace)
    sys.exit(0 if success else 1)

