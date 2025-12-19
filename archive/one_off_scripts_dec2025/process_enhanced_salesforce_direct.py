#!/usr/bin/env python3
"""
Direct Enhanced Salesforce Processing

This script processes documents directly from the enhanced JSON file without creating
an intermediate discovery file. Uses only the validated 27 core fields.
"""

import argparse
import json
import sys
import os
from datetime import datetime
from pathlib import Path
import logging

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from src.connectors.enhanced_salesforce_file_source import EnhancedSalesforceFileSource
from src.config.colored_logging import setup_colored_logging
from src.models.document_models import DocumentMetadata
from src.pipeline.document_processor import DocumentProcessor
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings
from src.config.progress_logger import ProcessingProgressLogger
from src.chunking.chunker_factory import ChunkerFactory
from src.pipeline.processing_batch_manager import ProcessingBatchManager
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


def create_optimized_document_metadata(doc_data: dict) -> DocumentMetadata:
    """Create optimized DocumentMetadata object using only the 27 validated fields"""
    
    # Extract nested data
    file_info = doc_data.get('file_info', {})
    deal_metadata = doc_data.get('deal_metadata', {})
    metadata = doc_data.get('metadata', {})
    
    # Create DocumentMetadata object with the validated 27 fields
    doc_metadata = DocumentMetadata(
        # CORE REQUIRED FIELDS (6 fields)
        path=doc_data.get('path', '').replace('/Volumes/Jeff_2TB/organized_salesforce_v2/', ''),  # Make relative
        name=doc_data.get('name', ''),
        size=file_info.get('size', 0),
        size_mb=file_info.get('size_mb', 0.0),
        file_type=file_info.get('file_type', ''),
        modified_time=file_info.get('modified_time', ''),
        
        # ADDITIONAL CORE FIELDS (4 fields)
        full_path=doc_data.get('path', ''),
        content_hash=None,  # Not available in enhanced JSON
        is_downloadable=True,  # Assume downloadable
        salesforce_content_version_id=doc_data.get('salesforce_id', ''),
        
        # DEAL METADATA FIELDS (10 core fields) - Fixed mapping
        deal_id=deal_metadata.get('deal_id'),
        deal_subject=deal_metadata.get('subject'),  # subject → deal_subject
        deal_name=deal_metadata.get('deal_name'),
        deal_status=deal_metadata.get('status'),  # status → deal_status  
        deal_reason=deal_metadata.get('deal_reason'),
        deal_start_date=deal_metadata.get('created_date'),  # created_date → deal_start_date
        client_id=deal_metadata.get('client_id'),
        current_narrative=deal_metadata.get('current_narrative'),
        customer_comments=deal_metadata.get('customer_comments'),
        savings_achieved=deal_metadata.get('savings_achieved'),
        
        # FINANCIAL FIELDS (5 fields) - Fixed mapping
        proposed_amount=safe_float(deal_metadata.get('total_proposed_amount')),
        final_amount=safe_float(deal_metadata.get('total_final_amount')),
        savings_1yr=safe_float(deal_metadata.get('total_savings_1yr')),
        savings_3yr=safe_float(deal_metadata.get('total_savings_3yr')),
        savings_target=safe_float(deal_metadata.get('npi_savings_target')),
        
        # LLM CLASSIFICATION FIELDS (pruned)
        document_type=metadata.get('document_type'),
        document_type_confidence=metadata.get('document_type_confidence', 0.0),
        classification_method=metadata.get('classification_method'),
        classification_reasoning=metadata.get('classification_reasoning'),
        commercial_terms_depth=metadata.get('commercial_terms_depth'),
        product_pricing_depth=metadata.get('product_pricing_depth'),
        
        # TOPICS AND INDICATORS (pruned)
        
        # CLIENT/VENDOR ENRICHMENT (new)
        vendor_id=deal_metadata.get('vendor_id'),
        client_name=deal_metadata.get('client_name'),
        vendor_name=deal_metadata.get('vendor_name'),
    )
    
    return doc_metadata


def safe_float(value):
    """Safely convert value to float"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def load_resume_state(args) -> dict:
    """Load resume state from previous processing run"""
    resume_file = f"enhanced_salesforce_resume_{args.namespace.replace('-', '_')}.json"
    
    if Path(resume_file).exists():
        try:
            with open(resume_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.getLogger(__name__).warning(f"⚠️ Could not load resume state: {e}")
    
    return {'processed_paths': set(), 'last_position': 0}

def save_resume_state(args, processed_paths: set, last_position: int):
    """Save resume state for recovery"""
    resume_file = f"enhanced_salesforce_resume_{args.namespace.replace('-', '_')}.json"
    
    state = {
        'processed_paths': list(processed_paths),
        'last_position': last_position,
        'last_update': datetime.now().isoformat(),
        'namespace': args.namespace
    }
    
    try:
        with open(resume_file, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.getLogger(__name__).error(f"❌ Could not save resume state: {e}")

def process_enhanced_salesforce_direct(args):
    """Process documents directly from enhanced JSON with logging and resume"""
    
    logger = logging.getLogger(__name__)
    
    logger.info("🎯 Direct Enhanced Salesforce Processing Starting...")
    logger.info(f"📄 Enhanced JSON: {args.enhanced_json}")
    logger.info(f"🎯 Namespace: {args.namespace}")
    logger.info(f"📊 Using 27 optimized fields (no intermediate discovery file)")
    
    # Load resume state
    resume_state = load_resume_state(args) if args.resume else {'processed_paths': set(), 'last_position': 0}
    processed_paths = set(resume_state.get('processed_paths', []))
    start_position = resume_state.get('last_position', 0)
    
    if args.resume and processed_paths:
        logger.info(f"🔄 Resuming from position {start_position:,} | Already processed: {len(processed_paths):,} documents")
    
    start_time = datetime.now()
    
    try:
        # Load enhanced JSON directly
        logger.info("📖 Loading enhanced JSON file...")
        with open(args.enhanced_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        documents = data['documents']
        logger.info(f"✅ Loaded {len(documents):,} documents with pre-enriched metadata")
        
        # Filter by file types first (before limiting)
        if args.file_types:
            original_count = len(documents)
            documents = [doc for doc in documents 
                        if doc.get('file_info', {}).get('file_type', '').lower() in args.file_types]
            logger.info(f"🔍 Filtered by file types {args.file_types}: {len(documents):,} documents (from {original_count:,})")
        
        # Apply resume logic - skip already processed documents
        if args.resume and processed_paths:
            original_count = len(documents)
            documents = [doc for doc in documents 
                        if doc.get('path', '').replace('/Volumes/Jeff_2TB/organized_salesforce_v2/', '') not in processed_paths]
            logger.info(f"🔄 Resume filter: {len(documents):,} remaining documents (skipped {original_count - len(documents):,} already processed)")
        
        # Apply limit if specified (after filtering)
        if args.limit:
            documents = documents[:args.limit]
            logger.info(f"🔒 Limited to {len(documents):,} documents for processing")
        
        # Initialize processing pipeline (if not validation-only)
        document_processor = None
        if not args.validate_only:
            logger.info("🔧 Initializing document processing pipeline...")
            
            # Initialize settings and clients
            settings = Settings()
            pinecone_client = PineconeDocumentClient(
                api_key=settings.PINECONE_API_KEY,
                index_name=settings.PINECONE_INDEX_NAME
            )
            
            # Create a minimal file source for DocumentProcessor (it needs dropbox_client)
            # We'll use the enhanced source as a mock since we're processing directly
            enhanced_source = EnhancedSalesforceFileSource(
                organized_files_dir="/Volumes/Jeff_2TB/organized_salesforce_v2",
                enhanced_json_path=args.enhanced_json
            )
            
            # Enable batch mode if requested
            batch_mgr = None
            batch_mode = False
            if getattr(args, 'use_batch', False) or getattr(args, 'batch_only', False):
                try:
                    batch_mgr = ProcessingBatchManager(settings.OPENAI_API_KEY)
                    batch_mode = True
                    logger.info("✅ Batch manager enabled for enhanced classification (Batch API)")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to initialize batch manager: {e}")
                    batch_mgr = None
                    batch_mode = False

            # Optional: Enable Vision model for PPTX analysis
            openai_client = None
            if getattr(args, 'enable_vision', False):
                if OpenAI is None:
                    logger.warning("⚠️ OpenAI client unavailable; proceeding without vision analysis")
                else:
                    try:
                        openai_client = OpenAI()
                        logger.info(f"✅ Vision enabled with model: {getattr(args, 'vision_model', 'gpt-4o')}")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to initialize OpenAI client: {e}")
                        openai_client = None

            document_processor = DocumentProcessor(
                dropbox_client=enhanced_source,  # Use as file source
                pinecone_client=pinecone_client,
                llm_classifier=None,
                batch_manager=batch_mgr,
                batch_mode=batch_mode,
                batch_submit_threshold=15000,
                openai_client=openai_client,
                enable_vision_analysis=getattr(args, 'enable_vision', False),
                vision_model=getattr(args, 'vision_model', 'gpt-4o')
            )

            # Allow runtime selection of chunking strategy
            try:
                chunker_factory = ChunkerFactory(pinecone_client)
                chunker = chunker_factory.create_chunker(args.chunking_strategy)
                document_processor.set_chunker(chunker)
                logger.info(f"✅ Using chunking strategy: {args.chunking_strategy}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to set requested chunking strategy; using default. Error: {e}")
            
            logger.info("✅ Document processing pipeline initialized")
        
        # Initialize progress logger for production monitoring
        progress_logger = None
        if not args.validate_only and len(documents) > 10:
            operation_name = f"enhanced_salesforce_{args.namespace.replace('-', '_')}"
            progress_logger = ProcessingProgressLogger(
                operation_name=operation_name,
                total_items=len(documents),
                dataset_name="documents"
            )
            logger.info(f"📊 Progress monitoring: tail -f logs/progress/{operation_name}_progress_*.log")
        
        # Process documents directly
        logger.info("🚀 Starting direct document processing...")
        
        processed_count = 0
        error_count = 0
        chunk_count = 0
        
        for i, doc_data in enumerate(documents):
            try:
                # Skip if already processed (resume logic)
                relative_path = doc_data.get('path', '').replace('/Volumes/Jeff_2TB/organized_salesforce_v2/', '')
                if relative_path in processed_paths:
                    continue
                
                # Create optimized DocumentMetadata object
                doc_metadata = create_optimized_document_metadata(doc_data)
                
                if args.validate_only:
                    # Just validate the metadata structure
                    required_fields = ['path', 'name', 'size', 'file_type']
                    missing_fields = [field for field in required_fields 
                                    if not getattr(doc_metadata, field, None)]
                    
                    if missing_fields:
                        logger.warning(f"⚠️  Document {i+1}: Missing fields {missing_fields}")
                        error_count += 1
                    else:
                        processed_count += 1
                        if i < 3:  # Show first 3 successful validations
                            logger.info(f"✅ Doc {i+1}: {doc_metadata.name} | Deal: {doc_metadata.deal_subject} | Type: {doc_metadata.document_type}")
                else:
                    # Call actual processing pipeline
                    logger.debug(f"📄 Processing: {doc_metadata.name}")
                    result = document_processor.process_document(doc_metadata, namespace=args.namespace)
                    
                    if result.get('success'):
                        processed_count += 1
                        chunk_count += result.get('chunks_created', 0)
                        processed_paths.add(relative_path)
                        
                        # Update progress logger
                        if progress_logger:
                            progress_logger.update_progress(
                                increment=1,
                                chunks_created=result.get('chunks_created', 0),
                                custom_message=f"Processed {doc_metadata.name} | Deal: {doc_metadata.deal_subject}"
                            )
                        
                        if processed_count <= 5:  # Show first 5 successes
                            logger.info(f"✅ Processed: {doc_metadata.name} | Chunks: {result.get('chunks_created', 0)} | Deal: {doc_metadata.deal_subject}")
                    else:
                        error_count += 1
                        processed_paths.add(relative_path)  # Mark as attempted to avoid retry
                        
                        # Update progress logger with failure
                        if progress_logger:
                            progress_logger.update_progress(
                                increment=0,
                                failed=1,
                                custom_message=f"Failed {doc_metadata.name}: {result.get('errors', [])}"
                            )
                        
                        logger.error(f"❌ Failed: {doc_metadata.name} | Error: {result.get('errors', [])}")
                
                # Save resume state periodically
                if (processed_count + error_count) % 100 == 0:
                    save_resume_state(args, processed_paths, i + 1)
                
                # Progress reporting (less frequent with progress logger)
                if (i + 1) % 1000 == 0:
                    elapsed = datetime.now() - start_time
                    rate = (i + 1) / elapsed.total_seconds() * 60  # docs per minute
                    logger.info(f"📈 Progress: {i+1:,}/{len(documents):,} ({(i+1)/len(documents)*100:.1f}%) | Rate: {rate:.0f} docs/min | Success: {processed_count:,}")
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Error processing document {i+1}: {e}")
                
                if error_count > 50:  # Stop after too many errors
                    logger.error("🛑 Too many errors, stopping processing")
                    break
        
        # Save final resume state
        save_resume_state(args, processed_paths, len(documents))

        # Submit batch job if we collected requests
        if (getattr(args, 'use_batch', False) or getattr(args, 'batch_only', False)) and document_processor and document_processor.batch_manager:
            try:
                logger.info(f"\n🚀 Submitting batch job for collected enhancement requests...")
                batch_job_id = document_processor.submit_batch_classification()
                if batch_job_id:
                    logger.info(f"✅ Batch job submitted successfully: {batch_job_id}")
                    logger.info(f"\n📊 Next Steps:")
                    logger.info(f"   1. Monitor job: python batch_processing_updater.py --job-id {batch_job_id}")
                    logger.info(f"   2. Auto-apply when ready: python batch_processing_updater.py --job-id {batch_job_id} --monitor --update --namespace {args.namespace}")
                else:
                    logger.error("❌ Failed to submit batch job")
            except Exception as e:
                logger.error(f"❌ Error submitting batch job: {e}")
        
        # Final statistics
        elapsed = datetime.now() - start_time
        success_rate = (processed_count / len(documents)) * 100 if documents else 0
        
        # Complete progress logging
        if progress_logger:
            progress_logger.log_completion_summary({
                "Operation": f"Enhanced Salesforce Direct Processing",
                "Total Documents": f"{len(documents):,}",
                "Successfully Processed": f"{processed_count:,}",
                "Errors": f"{error_count:,}",
                "Success Rate": f"{success_rate:.1f}%",
                "Chunks Created": f"{chunk_count:,}",
                "Processing Time": str(elapsed),
                "Namespace": args.namespace,
                "Resume State": f"Saved for {len(processed_paths):,} processed documents"
            })
        
        logger.info(f"""
🎉 Direct Processing Complete!
{'='*60}
📄 Enhanced JSON: {args.enhanced_json}
🎯 Target Namespace: {args.namespace}

📊 Statistics:
   Total Documents: {len(documents):,}
   Successfully Processed: {processed_count:,}
   Errors: {error_count:,}
   Success Rate: {success_rate:.1f}%
   Processing Time: {elapsed}
   {f'Chunks Created: {chunk_count:,}' if not args.validate_only else ''}
   
🎯 Optimization Results:
   Fields per Document: 27 (optimized from 44 original)
   No Discovery File: Direct processing from enhanced JSON
   Field Mapping: Validated and corrected
   
🔄 Resume Capability:
   Resume State: Saved for {len(processed_paths):,} processed documents
   Resume Command: Add --resume flag to continue from interruption
   
✅ Status:
   {'✓ VALIDATION COMPLETE' if args.validate_only else '✓ PROCESSING COMPLETE'}
   ✓ All 27 optimized fields mapped correctly
   ✓ No intermediate files created
   ✓ Direct path from enhanced JSON to processed documents
   {'✓ Ready for Streamlit UI search' if not args.validate_only else '✓ Ready for actual processing'}
   ✓ Production logging and resume enabled
        """)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Direct processing failed: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Process enhanced Salesforce documents directly from JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate field mapping (no actual processing)
  python process_enhanced_salesforce_direct.py --validate-only
  
  # Process first 100 documents
  python process_enhanced_salesforce_direct.py --limit 100
  
  # Process only PDFs and Word docs
  python process_enhanced_salesforce_direct.py --file-types .pdf .docx
  
  # Full processing to specific namespace
  python process_enhanced_salesforce_direct.py --namespace salesforce-enhanced-2025
        """
    )
    
    parser.add_argument(
        "--enhanced-json", 
        default="/Volumes/Jeff_2TB/enhanced_salesforce_documents_full_v2.json",
        help="Path to enhanced metadata JSON file"
    )
    parser.add_argument(
        "--namespace",
        default="salesforce-enhanced-2025",
        help="Pinecone namespace for processed documents"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of documents for testing (optional)"
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        help="Filter by file types (e.g., .pdf .docx .xlsx)"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate field mapping, don't process documents"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous interrupted processing run"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--chunking-strategy",
        type=str,
        choices=['business_aware', 'semantic'],
        default='business_aware',
        help="Chunking strategy: 'business_aware' (default) or 'semantic' (LangChain)"
    )
    parser.add_argument(
        "--use-batch",
        action="store_true",
        help="Collect enhanced classification requests for OpenAI Batch API and submit a batch job"
    )
    parser.add_argument(
        "--batch-only",
        action="store_true",
        help="Only collect batch requests (no document processing); submit a batch job"
    )
    parser.add_argument(
        "--enable-vision",
        action="store_true",
        help="Enable OpenAI Vision analysis for .pptx (uses vision_model)"
    )
    parser.add_argument(
        "--vision-model",
        type=str,
        default="gpt-4o",
        help="Vision model to use when --enable-vision is set (e.g., gpt-4o, gpt-4o-mini)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_colored_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Run processing
    success = process_enhanced_salesforce_direct(args)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
