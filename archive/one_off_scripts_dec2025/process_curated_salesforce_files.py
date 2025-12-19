#!/usr/bin/env python3
"""
Process Curated Salesforce Files

This script processes only our curated Salesforce files to create a
high-quality discovery JSON for embedding and query testing.

Usage:
    python process_curated_salesforce_files.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, 'src')

def process_curated_salesforce_files():
    """Process only our curated Salesforce files"""
    print("🎯 PROCESSING CURATED SALESFORCE FILES")
    print("=" * 70)
    
    # Load our vendor-based curated dataset
    with open("salesforce_vendor_curated_dataset.json", 'r') as f:
        curated_data = json.load(f)
    
    curated_files = curated_data['files']
    print(f"📋 Loaded {len(curated_files)} curated Salesforce files")
    
    # Initialize SalesforceFileSource
    from src.connectors.salesforce_file_source import SalesforceFileSource
    
    organized_files_dir = "/Volumes/Jeff_2TB/organized_salesforce_v2"
    file_mapping_csv = "organized_files_to_deal_mapping.csv"
    deal_metadata_csv = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/Deal__c.csv"
    
    print(f"📋 Initializing SalesforceFileSource...")
    sf_source = SalesforceFileSource(
        organized_files_dir=organized_files_dir,
        file_mapping_csv=file_mapping_csv,
        deal_metadata_csv=deal_metadata_csv
    )
    
    # Create set of target file paths for fast lookup
    target_paths = set(f['relative_path'] for f in curated_files)
    print(f"🎯 Targeting {len(target_paths)} specific files")
    
    # Process only our curated files
    print(f"\n🔍 Processing curated files (this may take a moment)...")
    discovered_docs = []
    total_scanned = 0
    
    for doc_metadata in sf_source.list_documents():
        total_scanned += 1
        
        # Only include files that are in our curated list
        if doc_metadata.path in target_paths:
            discovered_docs.append(doc_metadata)
            print(f"   ✅ Found curated file {len(discovered_docs)}: {doc_metadata.name}")
            
            # Remove from target set (for efficiency)
            target_paths.remove(doc_metadata.path)
            
            # Stop when we've found all our curated files
            if not target_paths:
                print(f"   🎉 Found all {len(curated_files)} curated files!")
                break
        
        # Progress update every 5000 files
        if total_scanned % 5000 == 0:
            print(f"   📈 Scanned {total_scanned:,} files, found {len(discovered_docs)} curated files...")
        
        # Safety break 
        if total_scanned > 100000:
            print(f"   ⚠️  Safety break: Scanned {total_scanned:,} files, found {len(discovered_docs)}/{len(curated_files)} curated files")
            break
    
    print(f"✅ Successfully found {len(discovered_docs)}/{len(curated_files)} curated files after scanning {total_scanned:,} total files")
    
    # Convert to discovery JSON format using same approach as discover_documents.py
    print(f"\n💾 Converting to discovery JSON format...")
    
    from discover_documents import DocumentDiscovery
    discovery_instance = DocumentDiscovery()
    discovery_instance.source_client = sf_source  # Set for proper source type detection
    
    discovery_data = {
        "discovery_metadata": {
            "source_type": "salesforce",
            "source_path": "vendor_curated_salesforce_files", 
            "discovery_started": datetime.now().isoformat(),
            "discovery_completed": datetime.now().isoformat(),
            "discovery_interrupted": False,
            "total_documents": len(discovered_docs),
            "total_batches": 1,
            "llm_classification_enabled": False,
            "llm_model": "gpt-4.1-mini",
            "schema_version": "2.1",
            "vendor_focus": {
                "vendor_name": "Salesforce",
                "vendor_id": "0010y00001o6x6YAAQ",
                "total_vendor_deals": 762,
                "curated_deals": len(set(f['deal_name'] for f in curated_files)),
                "files_found": len(discovered_docs),
                "files_targeted": len(curated_files)
            },
            "batch_processing": {
                "enabled": False,
                "jobs_submitted": 0,
                "jobs_completed": 0,
                "estimated_cost": 0.0,
                "actual_cost": 0.0
            },
            "batch_jobs": []
        },
        "discovery_progress": {
            "last_processed_path": None,
            "current_batch": 1,
            "documents_discovered": len(discovered_docs),
            "resume_cursor": None
        },
        "documents": []
    }
    
    # Convert each DocumentMetadata to discovery format
    for doc_metadata in discovered_docs:
        doc_dict = discovery_instance._convert_metadata_to_dict(doc_metadata)
        discovery_data["documents"].append(doc_dict)
    
    # Save curated discovery
    output_file = "salesforce_curated_final_discovery.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(discovery_data, f, indent=2, default=str)
    
    print(f"✅ Saved curated discovery to: {output_file}")
    
    # Analyze results
    mapped_count = sum(1 for doc in discovery_data["documents"] 
                      if doc['deal_metadata'].get('mapping_status') == 'mapped')
    
    print(f"\n📊 Curated Discovery Results:")
    print(f"   📄 Total curated files processed: {len(discovered_docs)}")
    print(f"   ✅ Files with Salesforce deal metadata: {mapped_count}")
    print(f"   📊 Success rate: {(mapped_count / len(discovered_docs)) * 100:.1f}%")
    
    # Show sample mapped files
    mapped_docs = [doc for doc in discovery_data["documents"] 
                   if doc['deal_metadata'].get('mapping_status') == 'mapped']
    
    if mapped_docs:
        print(f"\n📋 Sample Mapped Salesforce Files:")
        for i, doc in enumerate(mapped_docs[:5], 1):
            deal_meta = doc['deal_metadata']
            file_info = doc['file_info']
            print(f"   {i}. {file_info['name']} ({file_info['file_type']})")
            print(f"      💰 {deal_meta.get('deal_subject', 'N/A')} - ${deal_meta.get('proposed_amount', 0):,.0f}")
    
    return output_file, len(discovered_docs), mapped_count

def main():
    """Process curated Salesforce files"""
    output_file, total_files, mapped_files = process_curated_salesforce_files()
    
    print(f"\n" + "=" * 70)
    print("🎉 CURATED SALESFORCE DISCOVERY COMPLETE")
    print("=" * 70)
    print(f"✅ Output: {output_file}")
    print(f"📊 Files: {mapped_files}/{total_files} with rich deal metadata")
    print(f"🎯 Ready for embedding and natural language query testing!")
    
    print(f"\n🚀 Next Commands:")
    print(f"   # Process for embeddings:")
    print(f"   python process_discovered_documents.py --input {output_file}")
    print(f"   ")
    print(f"   # Test natural language queries:")
    print(f"   # Use Streamlit interface with this processed data")

if __name__ == "__main__":
    main()
