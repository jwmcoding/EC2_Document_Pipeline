#!/usr/bin/env python3
"""
Test script for 10 files to verify metadata updates (8 new deal classification fields)
Tests the full pipeline: discovery → processing → Pinecone verification
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from discover_documents import DocumentDiscovery
from process_discovered_documents import DiscoveredDocumentProcessor
from src.config.colored_logging import ColoredLogger
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings

logger = ColoredLogger("test_10_files")

def create_test_discovery(export_dir: str, output_file: str, max_docs: int = 10) -> bool:
    """Create discovery file with full metadata enrichment"""
    
    logger.info("🔍 Step 1: Creating discovery with full metadata...")
    logger.info(f"   Source: {export_dir}")
    logger.info(f"   Output: {output_file}")
    
    try:
        discovery = DocumentDiscovery()
        
        # Use raw_salesforce_export_connector to get full metadata
        # Auto-detect CSV paths
        export_path = Path(export_dir)
        content_versions_csv = str(export_path / "ContentVersion.csv")
        content_documents_csv = str(export_path / "ContentDocument.csv") if (export_path / "ContentDocument.csv").exists() else None
        content_document_links_csv = str(export_path / "ContentDocumentLink.csv")
        deal_metadata_csv = str(export_path / "Deal__c.csv")
        
        # Auto-detect client/vendor mapping CSVs
        client_mapping_csv = str(export_path / "SF-Cust-Mapping.csv") if (export_path / "SF-Cust-Mapping.csv").exists() else None
        vendor_mapping_csv = str(export_path / "SF-Vendor_mapping.csv") if (export_path / "SF-Vendor_mapping.csv").exists() else None
        
        logger.info(f"   Client mapping: {'Found' if client_mapping_csv else 'Not found'}")
        logger.info(f"   Vendor mapping: {'Found' if vendor_mapping_csv else 'Not found'}")
        
        discovery_args = argparse.Namespace(
            source='salesforce_raw',
            export_root_dir=export_dir,
            content_versions_csv=content_versions_csv,
            content_documents_csv=content_documents_csv,
            content_document_links_csv=content_document_links_csv,
            deal_metadata_csv=deal_metadata_csv,
            client_mapping_csv=client_mapping_csv,
            vendor_mapping_csv=vendor_mapping_csv,
            deal_mapping_csv=None,
            require_deal_association=False,
            output=output_file,
            max_docs=max_docs,
            limit=None,
            batch_size=100,  # Required for discovery
            resume=False  # Don't resume existing discovery
        )
        
        discovery.run(discovery_args)
        
        # Verify discovery file
        if not Path(output_file).exists():
            logger.error(f"❌ Discovery file not created: {output_file}")
            return False
        
        with open(output_file, 'r') as f:
            data = json.load(f)
        
        doc_count = len(data.get('documents', []))
        logger.info(f"✅ Discovery created: {doc_count} documents")
        
        # Check if documents have deal metadata
        docs_with_deals = sum(1 for doc in data.get('documents', []) 
                             if doc.get('deal_metadata', {}).get('deal_id'))
        logger.info(f"   Documents with deal metadata: {docs_with_deals}/{doc_count}")
        
        # Check for new fields in first document
        if data.get('documents'):
            first_doc = data['documents'][0]
            deal_meta = first_doc.get('deal_metadata', {})
            new_fields = ['report_type', 'description', 'project_type', 'competition', 
                         'npi_analyst', 'dual_multi_sourcing', 'time_pressure', 'advisor_network_used']
            found_fields = [f for f in new_fields if deal_meta.get(f) is not None]
            logger.info(f"   New fields found in sample: {len(found_fields)}/8")
            if found_fields:
                logger.info(f"      Found: {', '.join(found_fields[:3])}...")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating discovery: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_test_files(discovery_file: str, namespace: str = "test-metadata-dec2025", export_dir: str = None, limit: int = 10, workers: int = 6) -> bool:
    """Process the test files"""
    
    logger.info(f"⚙️  Step 2: Processing {discovery_file}...")
    
    try:
        processor = DiscoveredDocumentProcessor()
        
        args = argparse.Namespace(
            input=discovery_file,
            namespace=namespace,
            export_root_dir=export_dir,  # Pass export root for salesforce_raw source
            workers=workers,  # Configurable workers (default: 6)
            parser_backend='docling',
            chunking_strategy='business_aware',
            batch_size=10,  # Batch size for progress reporting
            limit=limit,
            resume=False,
            reprocess=False,
            use_batch=False,
            batch_only=False,
            interactive=False,
            filter_type=None,
            filter_file_type=None,
            filter_vendor=None,
            filter_client=None,
            max_size_mb=None,
            # Docling OCR mode and quality thresholds
            docling_ocr_mode='auto',
            docling_timeout_seconds=240,
            docling_min_text_chars=800,
            docling_min_word_count=150,
            docling_alnum_threshold=0.5,
            # Additional filter arguments
            exclude_file_type=None,
            modified_after=None,
            modified_before=None,
            min_size_kb=None,
        )
        
        processor.run(args)
        
        logger.info("✅ Processing complete")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error processing: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_metadata_fields(namespace: str) -> bool:
    """Verify expected Pinecone metadata fields are present.

    Notes:
    - We do NOT store chunk text in Pinecone (neither in metadata nor “top-level”).
    - This function samples a few vectors to validate schema; it does not rely on text storage.
    """
    
    logger.info(f"🔍 Step 3: Verifying metadata fields in Pinecone...")
    
    try:
        settings = Settings()
        pc_client = PineconeDocumentClient(
            api_key=os.getenv("PINECONE_API_KEY"),
            index_name=settings.PINECONE_INDEX_NAME
        )
        
        # First, get an accurate vector count for the namespace (if available).
        # Then sample a few vectors to validate metadata schema.
        total_vectors: int | None = None
        try:
            stats = pc_client.index.describe_index_stats()
            # SDK may return dict or an object with .namespaces
            namespaces = getattr(stats, "namespaces", None)
            if namespaces is None and isinstance(stats, dict):
                namespaces = stats.get("namespaces", {})
            if isinstance(namespaces, dict) and namespace in namespaces:
                ns_obj = namespaces[namespace]
                total_vectors = getattr(ns_obj, "vector_count", None)
                if total_vectors is None and isinstance(ns_obj, dict):
                    total_vectors = ns_obj.get("vector_count")
        except Exception:
            # Non-fatal: stats may fail depending on permissions/SDK behavior.
            total_vectors = None

        # Query a few vectors to check metadata.
        # Note: Using 1024 dimensions to match multilingual-e5-large embeddings.
        results = pc_client.index.query(
            vector=[0.0] * 1024,  # Dummy vector (1024 dim for multilingual-e5-large)
            top_k=5,
            namespace=namespace,
            include_metadata=True
        )
        
        if not results.matches:
            logger.warning("⚠️  No vectors found in namespace. Processing may have failed.")
            return False
        
        if total_vectors is not None:
            logger.info(f"   Namespace vector count: {total_vectors}")
            logger.info(f"   Sampled {len(results.matches)} vectors for schema check")
        else:
            logger.info(f"   Sampled {len(results.matches)} vectors for schema check")
        
        # Check metadata fields in first result
        first_match = results.matches[0]
        metadata = first_match.metadata if hasattr(first_match, 'metadata') else {}
        
        # Expected fields (align with Pinecone upsert metadata schema).
        expected_fields = {
            # Core document (3)
            "file_name",
            "file_type",
            "deal_creation_date",

            # Identifiers (4)
            "deal_id",
            "salesforce_deal_id",
            "salesforce_client_id",
            "salesforce_vendor_id",

            # Financial (6)
            "final_amount",
            "savings_1yr",
            "savings_3yr",
            "savings_achieved",
            "fixed_savings",
            "savings_target_full_term",

            # Contract (3)
            "contract_term",
            "contract_start",
            "contract_end",

            # Processing (1)
            "chunk_index",

            # Search (2)
            "client_name",
            "vendor_name",

            # Quality (2)
            "has_parsing_errors",
            "deal_status",

            # Email (1)
            "email_has_attachments",

            # Deal Classification (8)
            "report_type",
            "description",
            "project_type",
            "competition",
            "npi_analyst",
            "dual_multi_sourcing",
            "time_pressure",
            "advisor_network_used",
        }
        
        found_fields = set(metadata.keys())
        missing_fields = expected_fields - found_fields
        
        logger.info(f"   Metadata fields found: {len(found_fields)}")
        logger.info(f"   Expected fields: {len(expected_fields)}")
        
        if missing_fields:
            logger.warning(f"   ⚠️  Missing fields ({len(missing_fields)}): {', '.join(list(missing_fields)[:5])}...")
        else:
            logger.info("   ✅ All expected fields present!")
        
        # Specifically check the 8 new fields
        new_fields = ['report_type', 'description', 'project_type', 'competition',
                     'npi_analyst', 'dual_multi_sourcing', 'time_pressure', 'advisor_network_used']
        found_new_fields = [f for f in new_fields if f in metadata]
        
        logger.info(f"\n   📊 New Deal Classification Fields:")
        logger.info(f"      Found: {len(found_new_fields)}/8")
        for field in new_fields:
            value = metadata.get(field, 'MISSING')
            status = "✅" if field in metadata else "❌"
            logger.info(f"      {status} {field}: {str(value)[:50]}")
        
        return len(found_new_fields) == 8
        
    except Exception as e:
        logger.error(f"❌ Error verifying metadata: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Test 10 files with metadata updates')
    parser.add_argument('--export-dir', type=str, 
                       default='/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1',
                       help='Salesforce export directory')
    parser.add_argument('--discovery-file', type=str,
                       default='test_10files_metadata_discovery.json',
                       help='Output discovery file')
    parser.add_argument('--namespace', type=str,
                       default='test-metadata-dec2025',
                       help='Pinecone namespace for test')
    parser.add_argument('--skip-discovery', action='store_true',
                       help='Skip discovery step (use existing file)')
    parser.add_argument('--skip-processing', action='store_true',
                       help='Skip processing step (only verify)')
    parser.add_argument('--max-docs', type=int, default=10,
                       help='Maximum number of documents to discover (default: 10)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of documents to process (default: same as --max-docs)')
    parser.add_argument('--workers', type=int, default=6,
                       help='Number of parallel workers for processing (default: 6)')
    
    args = parser.parse_args()
    
    # If limit not specified, use max_docs
    if args.limit is None:
        args.limit = args.max_docs
    
    logger.info("=" * 60)
    logger.info(f"🧪 Testing {args.max_docs} Files with Updated Metadata Schema")
    logger.info("=" * 60)
    logger.info(f"   Export Dir: {args.export_dir}")
    logger.info(f"   Discovery File: {args.discovery_file}")
    logger.info(f"   Namespace: {args.namespace}")
    logger.info(f"   Max Docs: {args.max_docs}")
    logger.info(f"   Process Limit: {args.limit}")
    logger.info(f"   Workers: {args.workers}")
    logger.info("")
    
    success = True
    
    # Step 1: Create discovery
    if not args.skip_discovery:
        if not create_test_discovery(args.export_dir, args.discovery_file, args.max_docs):
            logger.error("❌ Discovery failed")
            return 1
    else:
        logger.info("⏭️  Skipping discovery (using existing file)")
    
    # Step 2: Process files
    if not args.skip_processing:
        if not process_test_files(args.discovery_file, args.namespace, args.export_dir, args.limit, args.workers):
            logger.error("❌ Processing failed")
            return 1
    else:
        logger.info("⏭️  Skipping processing (verification only)")
    
    # Step 3: Verify metadata
    if not verify_metadata_fields(args.namespace):
        logger.error("❌ Metadata verification failed")
        return 1
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ Test Complete!")
    logger.info("=" * 60)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())

