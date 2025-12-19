#!/usr/bin/env python3
"""
Document Processing Tool - Process documents from discovery JSON files

This script processes documents that were previously discovered and saved to JSON files.
It supports both Dropbox and local filesystem sources and integrates with the existing
document processing pipeline.

Features:
- Read discovery JSON files from discover_documents.py
- Process documents in batches with resume capability
- Support filtering by document type, file type, vendor, client, etc.
- Update discovery JSON with processing status
- Integrate with existing DocumentProcessor pipeline
- Support both Dropbox and local file sources
"""

import os
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

# Import components
from src.config.colored_logging import ColoredLogger
from src.config.settings import Settings
from src.connectors.dropbox_client import DropboxClient
from src.models.document_models import DocumentMetadata
from src.connectors.local_filesystem_client import LocalFilesystemClient
from src.connectors.salesforce_file_source import SalesforceFileSource
from src.connectors.pinecone_client import PineconeDocumentClient
from src.pipeline.document_processor import DocumentProcessor
from src.utils.discovery_persistence import DiscoveryPersistence
from src.pipeline.processing_batch_manager import ProcessingBatchManager
from src.config.progress_logger import ProcessingProgressLogger
from src.chunking.chunker_factory import ChunkerFactory

# Processing defaults
DEFAULT_BATCH_SIZE: int = 50
DEFAULT_NAMESPACE: str = "documents"
DEFAULT_REDACTION_MODEL: str = "gpt-5-mini-2025-08-07"
EXCLUDED_FILE_TYPES_DEFAULT: str = ".png"

__all__ = ["DiscoveredDocumentProcessor", "create_argument_parser", "main"]

class DiscoveredDocumentProcessor:
    """Processes documents from discovery JSON files"""
    
    def __init__(self):
        self.logger = ColoredLogger("document_processor")
        self.settings = Settings()
        self.persistence: Optional[DiscoveryPersistence] = None
        self.source_client = None
        self.pinecone_client = None
        self.document_processor = None
        
        # Processing statistics
        self.stats = {
            "documents_processed": 0,
            "documents_failed": 0,
            "documents_skipped": 0,
            "total_chunks_created": 0,
            "total_processing_time": 0.0
        }

    def run(self, args: argparse.Namespace) -> None:
        """Run document processing based on arguments.
        
        Args:
            args: Parsed command line arguments.
        """
        # Validate input file
        if not Path(args.input).exists():
            self.logger.error(f"❌ Discovery file not found: {args.input}")
            return
        
        # Load discovery data
        self.persistence = DiscoveryPersistence(args.input)
        summary = self.persistence.get_discovery_summary()
        
        # Display summary
        self._display_discovery_summary(summary)
        
        # Initialize clients based on discovery source type
        self._initialize_clients(summary, args)
        
        # Get documents to process
        documents_to_process = self._get_documents_to_process(args)
        
        if not documents_to_process:
            self.logger.warning("⚠️ No documents to process after filtering")
            return

        self.logger.info(f"📋 Selected {len(documents_to_process)} documents for processing")

        # Interactive mode for batch processing
        if args.interactive and (args.use_batch or args.batch_only):
            if not self._interactive_batch_prompt(len(documents_to_process), args):
                self.logger.info("❌ Processing cancelled by user")
                return

        # Process documents or collect batch requests
        if args.batch_only:
            self._collect_batch_requests_only(documents_to_process, args)
        else:
            self._process_documents(documents_to_process, args)
    
    def _display_discovery_summary(self, summary: Dict[str, Any]) -> None:
        """Display discovery summary information.
        
        Args:
            summary: Discovery summary dictionary.
        """
        self.logger.info(f"\n📊 Discovery Summary:")
        self.logger.info(f"📂 Source: {summary['source_type']} ({summary['source_path']})")
        self.logger.info(f"📄 Total documents: {summary['total_documents']}")
        self.logger.info(f"🏷️ LLM classification: {'Enabled' if summary['llm_classification_enabled'] else 'Disabled'}")
    
    def _initialize_clients(self, summary: Dict[str, Any], args: argparse.Namespace) -> None:
        """Initialize source and processing clients.
        
        Sets up the appropriate source client (Dropbox, local filesystem, or Salesforce)
        and the document processor with configured parsers and chunkers.
        
        Args:
            summary: Discovery summary dictionary containing source type and path.
            args: Parsed command line arguments.
            
        Raises:
            ValueError: If required configuration is missing for Salesforce source.
        """
        source_type = summary['source_type']
        source_path = summary['source_path']
        
        # Initialize source client
        if source_type == "dropbox":
            self.source_client = DropboxClient(
                self.settings.DROPBOX_ACCESS_TOKEN,
                openai_api_key=self.settings.OPENAI_API_KEY
            )
            self.logger.info("✅ Dropbox client initialized")
        elif source_type == "local":
            self.source_client = LocalFilesystemClient(
                source_path,
                openai_api_key=self.settings.OPENAI_API_KEY
            )
            self.logger.info(f"✅ Local filesystem client initialized: {source_path}")
        elif source_type == "salesforce":
            # For Salesforce organized exports, require paths via CLI args or env vars.
            organized_dir = (
                getattr(args, "salesforce_files_dir", None)
                or os.getenv("SALESFORCE_FILES_DIR")
                or source_path
            )
            file_mapping_csv = (
                getattr(args, "file_mapping_csv", None)
                or os.getenv("SALESFORCE_FILE_MAPPING_CSV")
            )
            deal_metadata_csv = (
                getattr(args, "deal_metadata_csv", None)
                or os.getenv("SALESFORCE_DEAL_METADATA_CSV")
            )
            client_mapping_csv = (
                getattr(args, "client_mapping_csv", None)
                or os.getenv("SALESFORCE_CLIENT_MAPPING_CSV")
            )
            vendor_mapping_csv = (
                getattr(args, "vendor_mapping_csv", None)
                or os.getenv("SALESFORCE_VENDOR_MAPPING_CSV")
            )

            missing = []
            if not organized_dir:
                missing.append("--salesforce-files-dir / SALESFORCE_FILES_DIR")
            if not file_mapping_csv:
                missing.append("--file-mapping-csv / SALESFORCE_FILE_MAPPING_CSV")
            if not deal_metadata_csv:
                missing.append("--deal-metadata-csv / SALESFORCE_DEAL_METADATA_CSV")
            if missing:
                raise ValueError(
                    "Salesforce (organized) processing requires configuration: "
                    + ", ".join(missing)
                )

            self.source_client = SalesforceFileSource(
                organized_files_dir=organized_dir,
                file_mapping_csv=file_mapping_csv,
                deal_metadata_csv=deal_metadata_csv,
                client_mapping_csv=client_mapping_csv,
                vendor_mapping_csv=vendor_mapping_csv,
            )
            self.logger.info(f"✅ Salesforce file source initialized: {organized_dir}")
        elif source_type == "salesforce_raw":
            # For salesforce_raw, use LocalFilesystemClient since files are already on disk
            # The metadata is already enriched in the discovery file
            # Get export root from discovery metadata or infer from the first document.
            if not source_path:
                # First preference: environment variables (useful on EC2)
                source_path = os.getenv("SALESFORCE_EXPORT_ROOT") or os.getenv("EXPORT_DIR") or ""

                if not source_path:
                    # Try to load first document from discovery file (summary may not carry docs)
                    with open(args.input, 'r') as f:
                        discovery_data = json.load(f)
                    first_doc = discovery_data.get('documents', [{}])[0]

                    doc_path = first_doc.get('source_metadata', {}).get('source_path', '')
                    full_path = first_doc.get('source_metadata', {}).get('full_source_path', '')

                    # Try to infer export root from full_source_path
                    if full_path:
                        # e.g., /data/august-2024/ContentVersion/068xxx -> /data/august-2024
                        path_parts = Path(full_path).parts
                        for i, part in enumerate(path_parts):
                            if part in ('ContentVersion', 'ContentVersions'):
                                source_path = str(Path(*path_parts[:i]))
                                break

                    if not source_path:
                        if doc_path.startswith('ContentVersion/') or doc_path.startswith('ContentVersions/'):
                            raise ValueError(
                                "Could not determine export root directory. "
                                "Ensure discovery was run with --export-root-dir (so discovery_metadata.source_path is set), "
                                "or set SALESFORCE_EXPORT_ROOT/EXPORT_DIR in the environment."
                            )
                        raise ValueError("Could not determine export root directory from discovery file")
            
            self.source_client = LocalFilesystemClient(
                source_path,
                openai_api_key=self.settings.OPENAI_API_KEY
            )
            self.logger.info(f"✅ Local filesystem client initialized for salesforce_raw: {source_path}")
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Initialize Pinecone client
        self.pinecone_client = PineconeDocumentClient(
            self.settings.PINECONE_API_KEY,
            index_name=self.settings.PINECONE_INDEX_NAME,
            environment=self.settings.PINECONE_ENVIRONMENT
        )
        self.logger.info("✅ Pinecone client initialized")
        
        # Initialize document processor
        batch_mgr = None
        batch_mode = False
        if args.use_batch or args.batch_only:
            # Enable batch mode wiring with ProcessingBatchManager
            try:
                batch_mgr = ProcessingBatchManager(self.settings.OPENAI_API_KEY)
                batch_mode = True
                self.logger.info("✅ Batch manager enabled for enhanced classification (Batch API)")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to initialize batch manager: {e}")
                batch_mgr = None
                batch_mode = False

        # Initialize Docling parser with custom OCR mode/thresholds if provided
        docling_kwargs = {}
        if args.parser_backend == "docling":
            docling_kwargs = {
                "ocr_mode": args.docling_ocr_mode,
                "timeout_seconds": args.docling_timeout_seconds,
                "min_text_chars": args.docling_min_text_chars,
                "min_word_count": args.docling_min_word_count,
                "alnum_threshold": args.docling_alnum_threshold,
            }
        
        # Initialize redaction service if enabled
        redaction_service = None
        if args.enable_redaction and args.client_redaction_csv:
            try:
                from src.redaction.client_registry import ClientRegistry
                from src.redaction.llm_span_detector import LLMSpanDetector
                from src.redaction.redaction_service import RedactionService
                
                client_registry = ClientRegistry(args.client_redaction_csv)
                llm_detector = LLMSpanDetector(
                    api_key=self.settings.OPENAI_API_KEY,
                    model=args.redaction_model or DEFAULT_REDACTION_MODEL
                )
                redaction_service = RedactionService(
                    client_registry=client_registry,
                    llm_span_detector=llm_detector,
                    strict_mode=True
                )
                self.logger.info("✅ Redaction service initialized")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to initialize redaction service: {e}")
                if args.enable_redaction:
                    raise  # Fail if redaction was explicitly requested
        
        self.document_processor = DocumentProcessor(
            dropbox_client=self.source_client,
            pinecone_client=self.pinecone_client,
            llm_classifier=None,
            batch_manager=batch_mgr,
            batch_mode=batch_mode,
            enable_discovery_cache=False,
            parser_backend=args.parser_backend,
            docling_kwargs=docling_kwargs if docling_kwargs else None,
            redaction_service=redaction_service
        )
        
        # Initialize and set the chunker using the factory
        chunker_factory = ChunkerFactory(self.pinecone_client)
        chunker = chunker_factory.create_chunker(args.chunking_strategy)
        self.document_processor.set_chunker(chunker)
        
        self.logger.info(f"✅ Document processor initialized with {args.chunking_strategy} chunking")
    
    def _get_documents_to_process(self, args: argparse.Namespace) -> List[Dict[str, Any]]:
        """Get filtered list of documents to process"""
        documents = self.persistence.get_documents()

        # Normalize include/exclude file types
        include_file_types: Set[str] = set()
        if args.filter_file_type:
            include_file_types = {x.strip() for x in args.filter_file_type.split(",") if x.strip()}

        exclude_file_types: Set[str] = {EXCLUDED_FILE_TYPES_DEFAULT}
        if args.exclude_file_type:
            exclude_file_types = {x.strip() for x in args.exclude_file_type.split(",") if x.strip()}

        # NOTE: filter-type/vendor/client are still supported but apply only if present in the discovery JSON
        # (discovery phase does not run LLM classification by default).
        filtered = DiscoveryPersistence.filter_documents(
            documents,
            include_processed=bool(args.reprocess),
            include_file_types=include_file_types or None,
            exclude_file_types=exclude_file_types or None,
            modified_after=args.modified_after,
            modified_before=args.modified_before,
            deal_created_after=args.deal_created_after,
            deal_created_before=args.deal_created_before,
            min_size_kb=args.min_size_kb,
            max_size_mb=args.max_size_mb,
        )

        filtered_docs: List[Dict[str, Any]] = filtered["documents"]
        stats: Dict[str, Any] = filtered["stats"]

        # Optional: apply higher-level metadata filters (vendor/client/doc_type) AFTER basic safety filters
        def _split_csv(value: Optional[str]) -> Set[str]:
            if not value:
                return set()
            return {v.strip().lower() for v in str(value).split(",") if v.strip()}

        type_filter = _split_csv(args.filter_type)
        vendor_filter = _split_csv(args.filter_vendor)
        client_filter = _split_csv(args.filter_client)

        if type_filter or vendor_filter or client_filter:
            extra_filtered: List[Dict[str, Any]] = []
            for doc in filtered_docs:
                llm_type = str(doc.get("llm_classification", {}).get("document_type", "") or "").strip().lower()
                business_vendor = str(doc.get("business_metadata", {}).get("vendor", "") or "").strip().lower()
                business_client = str(doc.get("business_metadata", {}).get("client", "") or "").strip().lower()
                deal_vendor = str(doc.get("deal_metadata", {}).get("vendor_name", "") or "").strip().lower()
                deal_client = str(doc.get("deal_metadata", {}).get("client_name", "") or "").strip().lower()

                if type_filter and llm_type not in type_filter:
                    continue
                if vendor_filter and (deal_vendor not in vendor_filter and business_vendor not in vendor_filter):
                    continue
                if client_filter and (deal_client not in client_filter and business_client not in client_filter):
                    continue

                extra_filtered.append(doc)
            filtered_docs = extra_filtered

        # Apply limit last (after all filters)
        if args.limit and len(filtered_docs) > args.limit:
            filtered_docs = filtered_docs[: args.limit]

        self.logger.info(
            "🔎 Selection summary | "
            f"in={stats.get('input_total')} "
            f"out={len(filtered_docs)} "
            f"excluded_processed={stats.get('excluded_processed')} "
            f"excluded_type={stats.get('excluded_file_type')} "
            f"excluded_date_invalid={stats.get('excluded_modified_time_missing_or_invalid')} "
            f"excluded_after={stats.get('excluded_modified_after')} "
            f"excluded_before={stats.get('excluded_modified_before')} "
            f"excluded_deal_date_missing={stats.get('excluded_deal_date_missing')} "
            f"excluded_deal_after={stats.get('excluded_deal_created_after')} "
            f"excluded_deal_before={stats.get('excluded_deal_created_before')} "
            f"excluded_min_size={stats.get('excluded_min_size')} "
            f"excluded_max_size={stats.get('excluded_max_size')}"
        )

        return filtered_docs
    
    def _process_documents(self, documents: List[Dict[str, Any]], args: argparse.Namespace) -> None:
        """Process the selected documents.
        
        Converts discovery data to DocumentMetadata, processes each document
        through the pipeline (parse, chunk, embed, upsert), and updates
        processing status in the discovery JSON.
        
        Args:
            documents: List of document dictionaries from discovery JSON.
            args: Parsed command line arguments.
        """
        start_time = datetime.now()
        total_docs = len(documents)
        
        # Check for resume
        start_index = 0
        if args.resume:
            progress = self.persistence.load_progress()
            # Find last processed document index
            for i, doc in enumerate(documents):
                if doc.get("processing_status", {}).get("processed", False):
                    start_index = i + 1
            
            if start_index > 0:
                self.logger.info(f"🔄 Resuming from document {start_index + 1}/{total_docs}")
        
        try:
            processed_count = 0
            batch_start_time = datetime.now()
            
            for i, doc_data in enumerate(documents[start_index:], start_index):
                # Convert discovery data to DocumentMetadata
                doc_metadata = self._convert_to_document_metadata(doc_data)
                
                if not doc_metadata:
                    self.stats["documents_skipped"] += 1
                    continue
                
                # Process single document
                self.logger.info(f"📄 Processing {i+1}/{total_docs}: {doc_metadata.name}")
                
                result = self.document_processor.process_document(
                    doc_metadata, 
                    namespace=args.namespace
                )
                
                # Update processing status
                # Determine content_parser based on file type
                file_type = doc_data.get("file_info", {}).get("file_type", "").lower()
                if file_type == ".pdf":
                    content_parser = args.parser_backend  # mistral, docling, or pdfplumber
                elif file_type in [".xlsx", ".xls", ".csv"]:
                    content_parser = "pandas_openpyxl"
                elif file_type == ".docx":
                    content_parser = "python_docx"
                elif file_type == ".doc":
                    content_parser = "docx2txt"  # Primary method
                elif file_type == ".msg":
                    content_parser = "extract_msg"
                elif file_type == ".pptx":
                    content_parser = "python_pptx"
                elif file_type in [".png", ".jpg", ".jpeg"]:
                    content_parser = f"image_to_pdf_{args.parser_backend}"
                elif file_type == ".txt":
                    content_parser = "direct_text"
                else:
                    content_parser = "unknown"
                
                processing_status = {
                    "processed": result["success"],
                    "processing_date": datetime.now().isoformat(),
                    "processor_version": "2.0",
                    "parser_backend": args.parser_backend,  # PDF parser selection
                    "content_parser": content_parser,  # Actual parser used for this file type
                    "chunks_created": result.get("chunks_created", 0),
                    "vectors_created": result.get("chunks_created", 0),  # Same as chunks
                    "pinecone_namespace": args.namespace,
                    "processing_errors": result.get("errors", []),
                    "processing_time_seconds": result.get("processing_time", 0.0)
                }
                
                # Add docling OCR decision metadata if available
                if "docling_metadata" in result:
                    processing_status.update(result["docling_metadata"])
                
                # Update document in discovery JSON
                doc_path = doc_data.get("file_info", {}).get("path")
                if doc_path:
                    self.persistence.update_document_metadata(
                        doc_path, 
                        {"processing_status": processing_status}
                    )
                
                # Update stats
                if result["success"]:
                    self.stats["documents_processed"] += 1
                    self.stats["total_chunks_created"] += result.get("chunks_created", 0)
                else:
                    self.stats["documents_failed"] += 1
                
                self.stats["total_processing_time"] += result.get("processing_time", 0.0)
                processed_count += 1
                
                # Progress update every batch
                if processed_count % args.batch_size == 0:
                    elapsed = datetime.now() - batch_start_time
                    self.logger.progress(f"✅ Batch complete: {processed_count}/{total_docs} documents "
                                       f"({elapsed.total_seconds():.1f}s)")
                    batch_start_time = datetime.now()
                
                # Check if we should stop for testing
                if args.limit and processed_count >= args.limit:
                    break
            
            # Submit batch job if we collected requests
            if (args.use_batch or args.batch_only) and hasattr(self.document_processor, 'batch_manager'):
                self._handle_batch_submission(args)
            
            # Final summary
            elapsed = datetime.now() - start_time
            self._display_final_summary(elapsed)
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️ Processing interrupted by user")
            self.persistence.flush_buffer()
            self.logger.info(f"💾 Progress saved. Use --resume to continue.")
        except Exception as e:
            self.logger.error(f"❌ Processing error: {e}")
            self.persistence.flush_buffer()
            raise
    
    def _collect_batch_requests_only(self, documents: List[Dict[str, Any]], args: argparse.Namespace) -> None:
        """Collect LLM requests for batch processing without processing documents"""
        start_time = datetime.now()
        total_docs = len(documents)
        
        self.logger.info(f"📦 Collecting LLM requests for {total_docs} documents (batch-only mode)")
        
        if not hasattr(self.document_processor, 'batch_manager') or not self.document_processor.batch_manager:
            self.logger.error("❌ Batch manager not available for batch-only mode")
            return
        
        try:
            collected_count = 0
            
            for i, doc_data in enumerate(documents):
                # Convert discovery data to DocumentMetadata
                doc_metadata = self._convert_to_document_metadata(doc_data)
                
                if not doc_metadata:
                    self.stats["documents_skipped"] += 1
                    continue
                
                # Simulate content preview for LLM (we'd need to parse the document minimally)
                # For now, use basic document info as content preview
                content_preview = f"Document: {doc_metadata.name}\nFile type: {doc_metadata.file_type}\nVendor: {doc_metadata.vendor or 'Unknown'}\nClient: {doc_metadata.client or 'Unknown'}"
                
                # Collect enhancement request
                try:
                    enhancement_request = self.document_processor.batch_manager.collect_enhancement_request(
                        doc_metadata=doc_metadata,
                        content_preview=content_preview,
                        page_count=None,  # Would need parsing for actual page count
                        word_count=None   # Would need parsing for actual word count
                    )
                    
                    self.document_processor.enhancement_requests.append(enhancement_request)
                    self.document_processor.processed_documents.append({
                        'document_path': doc_metadata.path,
                        'request_index': len(self.document_processor.enhancement_requests) - 1
                    })
                    
                    collected_count += 1
                    self.document_processor.stats["batch_requests_collected"] += 1
                    
                    self.logger.info(f"📦 Collected request {collected_count}/{total_docs}: {doc_metadata.name}")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to collect request for {doc_metadata.name}: {e}")
                    self.stats["documents_failed"] += 1
                
                # Check limit
                if args.limit and collected_count >= args.limit:
                    break
            
            # Submit batch job if we collected requests
            if collected_count > 0:
                self.logger.info(f"\n📦 Collected {collected_count} LLM requests for batch processing")
                self._handle_batch_submission(args)
            else:
                self.logger.warning("⚠️ No LLM requests were collected")
            
            # Final summary
            elapsed = datetime.now() - start_time
            self.logger.info(f"\n🎉 Batch Collection Complete!")
            self.logger.info(f"📦 LLM requests collected: {collected_count}")
            self.logger.info(f"❌ Documents failed: {self.stats['documents_failed']}")
            self.logger.info(f"⏭️ Documents skipped: {self.stats['documents_skipped']}")
            self.logger.info(f"⏱️ Collection time: {elapsed}")
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️ Collection interrupted by user")
            self.logger.info(f"💾 Partial collection: {collected_count} requests collected")
        except Exception as e:
            self.logger.error(f"❌ Collection error: {e}")
            raise

    def _handle_batch_submission(self, args: argparse.Namespace) -> None:
        """Handle batch job submission and provide user feedback"""
        batch_info = self.document_processor.get_batch_requests_info()
        
        if batch_info["total_requests"] == 0:
            self.logger.warning("⚠️ No batch requests collected - nothing to submit")
            return
        
        self.logger.info(f"\n🚀 Submitting batch job for {batch_info['total_requests']} documents...")
        
        try:
            batch_job_id = self.document_processor.submit_batch_classification()
            
            if batch_job_id:
                self.logger.success(f"✅ Batch job submitted successfully: {batch_job_id}")
                
                # Save batch job info to discovery file if available
                if hasattr(self, 'persistence') and self.persistence:
                    try:
                        # Add batch job tracking to discovery metadata
                        batch_job_info = {
                            "job_id": batch_job_id,
                            "status": "submitted",
                            "document_count": batch_info["total_requests"],
                            "submitted_at": datetime.now().isoformat(),
                            "mode": "batch_only" if args.batch_only else "batch_processing"
                        }
                        
                        # This would require extending DiscoveryPersistence to track processing batch jobs
                        # For now, just log the information
                        self.logger.info(f"📝 Batch job info: {batch_job_info}")
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ Could not save batch job info: {e}")
                
                self.logger.info(f"\n📊 Next Steps:")
                self.logger.info(f"   1. Monitor job: python batch_results_checker.py --job-id {batch_job_id}")
                self.logger.info(f"   2. Check status: python -c \"from src.pipeline.processing_batch_manager import ProcessingBatchManager; import os; print(ProcessingBatchManager(os.getenv('OPENAI_API_KEY')).check_batch_status('{batch_job_id}'))\"")
                if not args.batch_only:
                    self.logger.info(f"   3. Update processed documents: python batch_processing_updater.py --job-id {batch_job_id} --discovery-file {args.input}")
                
                # Clear batch collections after successful submission
                self.document_processor.clear_batch_collections()
                
            else:
                self.logger.error("❌ Failed to submit batch job")
                
        except Exception as e:
            self.logger.error(f"❌ Error submitting batch job: {e}")

    def _interactive_batch_prompt(self, num_documents: int, args: argparse.Namespace) -> bool:
        """Interactive prompt for batch processing with cost estimation"""
        
        print(f"\n🎯 Batch Processing Mode")
        print(f"📊 Documents to process: {num_documents}")
        
        # Get cost estimation if batch manager is available
        if hasattr(self.document_processor, 'batch_manager') and self.document_processor.batch_manager:
            try:
                cost_estimate = self.document_processor.batch_manager.estimate_batch_cost(num_documents)
                
                print(f"\n💰 Cost Estimation:")
                print(f"   Batch API (50% savings): ${cost_estimate['batch_cost']:.2f}")
                print(f"   Immediate API:          ${cost_estimate['immediate_cost']:.2f}")
                print(f"   Total savings:          ${cost_estimate['savings']:.2f} ({cost_estimate['savings_percentage']:.1f}%)")
                print(f"   Estimated tokens:        {cost_estimate['estimated_input_tokens']:,} input + {cost_estimate['estimated_output_tokens']:,} output")
                
            except Exception as e:
                print(f"⚠️ Could not estimate costs: {e}")
        
        print(f"\n⏱️ Processing Timeline:")
        if args.batch_only:
            print(f"   📦 Collect requests: Immediate")
            print(f"   🚀 Submit batch job: Immediate") 
            print(f"   📊 LLM results: Available within 24 hours")
            print(f"   🔄 Apply results: Manual (use batch_processing_updater.py)")
        else:
            print(f"   📄 Process documents: Immediate (searchable right away)")
            print(f"   📦 Collect LLM requests: During processing")
            print(f"   🚀 Submit batch job: After processing")
            print(f"   📊 Enhanced metadata: Available within 24 hours")
        
        print(f"\n🎯 Processing Mode: {'Batch Collection Only' if args.batch_only else 'Batch Processing (Documents + LLM requests)'}")
        
        while True:
            response = input(f"\nProceed with batch processing? [Y/n]: ").strip().lower()
            if response in ['', 'y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("Please enter 'y' for yes or 'n' for no.")

    def _extract_file_info(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract file information from discovery data.
        
        Args:
            doc_data: Document data dictionary from discovery JSON.
            
        Returns:
            Dictionary with file-related fields for DocumentMetadata.
        """
            file_info = doc_data.get("file_info", {})
            source_meta = doc_data.get("source_metadata", {})
            
        return {
            "path": file_info.get("path", ""),
            "name": file_info.get("name", ""),
            "size": file_info.get("size", 0),
            "size_mb": file_info.get("size_mb", 0.0),
            "file_type": file_info.get("file_type", ""),
            "modified_time": file_info.get("modified_time", ""),
            "full_path": source_meta.get("source_path", file_info.get("path", "")),
            "dropbox_id": source_meta.get("source_id", ""),
            "content_hash": file_info.get("content_hash"),
            "is_downloadable": True,
        }
    
    def _extract_business_metadata(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract business metadata from discovery data.
        
        Args:
            doc_data: Document data dictionary from discovery JSON.
            
        Returns:
            Dictionary with business-related fields for DocumentMetadata.
        """
        business_meta = doc_data.get("business_metadata", {})
            deal_meta = doc_data.get("deal_metadata", {})
            
        return {
            "deal_creation_date": business_meta.get("deal_creation_date") or deal_meta.get("deal_creation_date"),
            "week_number": business_meta.get("week_number"),
            "week_date": business_meta.get("week_date"),
            "vendor": business_meta.get("vendor"),
            "client": business_meta.get("client"),
            "deal_number": business_meta.get("deal_number"),
            "deal_name": business_meta.get("deal_name"),
            "path_components": business_meta.get("path_components", []),
        }
    
    def _extract_deal_metadata(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract deal metadata from discovery data.
        
        Args:
            doc_data: Document data dictionary from discovery JSON.
            
        Returns:
            Dictionary with deal-related fields for DocumentMetadata.
        """
        deal_meta = doc_data.get("deal_metadata", {})
        
        return {
            "deal_id": deal_meta.get("deal_id"),
            "salesforce_deal_id": deal_meta.get("salesforce_deal_id"),
            "deal_subject": deal_meta.get("deal_subject"),
            "deal_status": deal_meta.get("deal_status"),
            "deal_reason": deal_meta.get("deal_reason"),
            "deal_start_date": deal_meta.get("deal_start_date"),
            "negotiated_by": deal_meta.get("negotiated_by"),
            "proposed_amount": deal_meta.get("proposed_amount"),
            "final_amount": deal_meta.get("final_amount"),
            "savings_1yr": deal_meta.get("savings_1yr"),
            "savings_3yr": deal_meta.get("savings_3yr"),
            "savings_target": deal_meta.get("savings_target"),
            "savings_percentage": deal_meta.get("savings_percentage"),
            "client_id": deal_meta.get("client_id"),
            "client_name": deal_meta.get("client_name"),
            "salesforce_client_id": deal_meta.get("salesforce_client_id"),
            "vendor_id": deal_meta.get("vendor_id"),
            "vendor_name": deal_meta.get("vendor_name"),
            "salesforce_vendor_id": deal_meta.get("salesforce_vendor_id"),
            "contract_term": deal_meta.get("contract_term"),
            "contract_start": deal_meta.get("contract_start"),
            "contract_end": deal_meta.get("contract_end"),
            "effort_level": deal_meta.get("effort_level"),
            "has_fmv_report": deal_meta.get("has_fmv_report"),
            "deal_origin": deal_meta.get("deal_origin"),
            "salesforce_content_version_id": deal_meta.get("salesforce_content_version_id"),
            "mapping_status": deal_meta.get("mapping_status"),
            "mapping_method": deal_meta.get("mapping_method"),
            "mapping_reason": deal_meta.get("mapping_reason"),
            "report_type": deal_meta.get("report_type"),
            "project_type": deal_meta.get("project_type"),
            "competition": deal_meta.get("competition"),
            "npi_analyst": deal_meta.get("npi_analyst"),
            "dual_multi_sourcing": deal_meta.get("dual_multi_sourcing"),
            "time_pressure": deal_meta.get("time_pressure"),
            "advisor_network_used": deal_meta.get("advisor_network_used"),
        }
    
    def _extract_llm_classification(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract LLM classification data from discovery data.
        
        Args:
            doc_data: Document data dictionary from discovery JSON.
            
        Returns:
            Dictionary with LLM classification fields for DocumentMetadata.
        """
        llm_classification = doc_data.get("llm_classification", {})
        
        return {
            "document_type": llm_classification.get("document_type"),
            "document_type_confidence": llm_classification.get("confidence", 0.0),
            "classification_reasoning": llm_classification.get("reasoning"),
            "classification_method": llm_classification.get("classification_method"),
            "alternative_document_types": llm_classification.get("alternative_types", []),
            "classification_tokens_used": llm_classification.get("tokens_used", 0),
        }
    
    def _convert_to_document_metadata(self, doc_data: Dict[str, Any]) -> Optional[DocumentMetadata]:
        """Convert discovery JSON data to DocumentMetadata object.
        
        Extracts file info, business metadata, deal metadata, and LLM classification
        from the discovery JSON structure and constructs a DocumentMetadata instance.
        
        Args:
            doc_data: Document data dictionary from discovery JSON.
            
        Returns:
            DocumentMetadata object or None if conversion fails.
            
        Note:
            Errors are logged and None is returned rather than raising exceptions.
        """
        try:
            file_info_dict = self._extract_file_info(doc_data)
            business_meta_dict = self._extract_business_metadata(doc_data)
            deal_meta_dict = self._extract_deal_metadata(doc_data)
            llm_class_dict = self._extract_llm_classification(doc_data)
            
            # Combine all dictionaries and create DocumentMetadata
            doc_metadata = DocumentMetadata(**file_info_dict, **business_meta_dict, **deal_meta_dict, **llm_class_dict)
            
            return doc_metadata
            
        except Exception as e:
            self.logger.error(f"❌ Error converting document data: {e}")
            return None
    
    def _display_final_summary(self, elapsed) -> None:
        """Display final processing summary.
        
        Args:
            elapsed: Total elapsed time for processing.
        """
        self.logger.success(f"\n🎉 Processing Complete!")
        self.logger.info(f"📊 Documents processed: {self.stats['documents_processed']}")
        self.logger.info(f"❌ Documents failed: {self.stats['documents_failed']}")
        self.logger.info(f"⏭️ Documents skipped: {self.stats['documents_skipped']}")
        self.logger.info(f"🧩 Total chunks created: {self.stats['total_chunks_created']}")
        self.logger.info(f"⏱️ Total processing time: {self.stats['total_processing_time']:.2f}s")
        self.logger.info(f"⏱️ Wall clock time: {elapsed}")
        
        if self.stats['documents_processed'] > 0:
            avg_time = self.stats['total_processing_time'] / self.stats['documents_processed']
            avg_chunks = self.stats['total_chunks_created'] / self.stats['documents_processed']
            self.logger.info(f"📈 Average: {avg_time:.2f}s per document, {avg_chunks:.1f} chunks per document")


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser.
    
    Returns:
        Configured ArgumentParser instance with all processing options.
    """
    parser = argparse.ArgumentParser(
        description="Process documents from discovery JSON files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all unprocessed documents
  python process_discovered_documents.py --input discovery_results.json

  # Process only specific document types
  python process_discovered_documents.py --input discovery_results.json --filter-type "IDD,FMV"

  # Process documents from specific vendor
  python process_discovered_documents.py --input discovery_results.json --filter-vendor "Microsoft"

  # Reprocess all documents (including already processed)
  python process_discovered_documents.py --input discovery_results.json --reprocess

  # Resume interrupted processing
  python process_discovered_documents.py --input discovery_results.json --resume

  # Limit processing for testing
  python process_discovered_documents.py --input discovery_results.json --limit 10
        """
    )
    
    # Input file
    parser.add_argument("--input", type=str, required=True,
                       help="Discovery JSON file to process")
    
    # Processing options
    parser.add_argument("--namespace", type=str, default=DEFAULT_NAMESPACE,
                       help=f"Pinecone namespace for storage (default: {DEFAULT_NAMESPACE})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                       help=f"Processing batch size for progress updates (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--reprocess", action="store_true",
                       help="Reprocess already processed documents")
    parser.add_argument("--resume", action="store_true",
                       help="Resume from last processed document")
    
    # Batch processing options (v3)
    parser.add_argument(
        "--use-batch",
        action="store_true",
        help="Use batch API for LLM classification (50%% cost savings)",
    )
    parser.add_argument("--batch-only", action="store_true",
                       help="Only collect LLM requests without processing documents")
    parser.add_argument("--interactive", action="store_true",
                       help="Interactive mode with cost estimation and confirmation")
    
    # Filtering options
    parser.add_argument("--filter-type", type=str,
                       help="Filter by document types (comma-separated). NOTE: only works if discovery JSON already contains llm_classification.document_type.")
    parser.add_argument("--filter-file-type", type=str,
                       help="Filter by file types (comma-separated, e.g., '.pdf,.docx')")
    parser.add_argument("--exclude-file-type", type=str, default=EXCLUDED_FILE_TYPES_DEFAULT,
                       help=f"Exclude file types (comma-separated). Default: '{EXCLUDED_FILE_TYPES_DEFAULT}'")
    parser.add_argument("--filter-vendor", type=str,
                       help="Filter by vendor names (comma-separated)")
    parser.add_argument("--filter-client", type=str,
                       help="Filter by client names (comma-separated)")
    parser.add_argument("--modified-after", type=str,
                       help="Only include files with modified_time on/after this date/time (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--modified-before", type=str,
                       help="Only include files with modified_time on/before this date/time (YYYY-MM-DD or ISO 8601)")
    parser.add_argument("--deal-created-after", type=str,
                       help="Only include documents with deal_creation_date on/after this date (YYYY-MM-DD). "
                            "Recommended: Use this instead of --modified-after for reliable date filtering.")
    parser.add_argument("--deal-created-before", type=str,
                       help="Only include documents with deal_creation_date on/before this date (YYYY-MM-DD)")
    parser.add_argument("--min-size-kb", type=float,
                       help="Minimum file size in KB (filters out tiny files)")
    parser.add_argument("--max-size-mb", type=float,
                       help="Maximum file size in MB")

    # Salesforce (organized) config (only used when discovery source_type == 'salesforce')
    parser.add_argument("--salesforce-files-dir", type=str,
                       help="Path to organized Salesforce files directory (optional; can also set SALESFORCE_FILES_DIR)")
    parser.add_argument("--file-mapping-csv", type=str,
                       help="CSV mapping files→deals (optional; can also set SALESFORCE_FILE_MAPPING_CSV)")
    parser.add_argument("--deal-metadata-csv", type=str,
                       help="Deal__c CSV path (optional; can also set SALESFORCE_DEAL_METADATA_CSV)")
    parser.add_argument("--vendor-mapping-csv", type=str,
                       help="Vendor mapping CSV (optional; can also set SALESFORCE_VENDOR_MAPPING_CSV)")
    parser.add_argument("--client-mapping-csv", type=str,
                       help="Client mapping CSV (optional; can also set SALESFORCE_CLIENT_MAPPING_CSV)")
    
    # Limiting options
    parser.add_argument("--limit", type=int,
                       help="Maximum number of documents to process")
    
    parser.add_argument(
        "--chunking-strategy",
        type=str,
        choices=['business_aware', 'semantic'],
        default='business_aware',
        help="The chunking strategy to use: 'business_aware' (default) or 'semantic' (LangChain)."
    )
    
    # Parser backend selection
    parser.add_argument(
        "--parser-backend",
        type=str,
        choices=["pdfplumber", "docling", "mistral"],
        default="mistral",
        help=(
            "PDF parser backend: "
            "'mistral' (default, Mistral OCR API - best for production) or "
            "'docling' (Granite Docling - best for structured tables) or "
            "'pdfplumber' (fast local baseline)."
        ),
    )
    
    # Parallel processing
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=1,
        help=(
            "Number of parallel worker processes (default: 1 = serial). "
            "Recommended: 4-6 for Docling, 6-8 for PDFPlumber. "
            "Each worker gets its own parser instance for process safety."
        ),
    )
    
    # Docling OCR mode and quality thresholds (optional tuning)
    parser.add_argument(
        "--docling-ocr-mode",
        type=str,
        choices=["auto", "on", "off"],
        default="on",
        help=(
            "Docling OCR behavior mode (default: on). "
            "'on' always uses OCR with TableFormer ACCURATE (quality-first approach). "
            "'off' disables OCR. "
            "'auto' is deprecated and treated as 'on' for backward compatibility."
        ),
    )
    parser.add_argument(
        "--docling-timeout-seconds",
        type=int,
        default=240,
        help="Total timeout cap for Docling PDF conversion in seconds (default: 240). "
             "In AUTO mode, timeout is split between pass1 and pass2.",
    )
    parser.add_argument(
        "--docling-min-text-chars",
        type=int,
        default=800,
        help="Minimum character count for pass1 success in AUTO mode (default: 800).",
    )
    parser.add_argument(
        "--docling-min-word-count",
        type=int,
        default=150,
        help="Minimum word count for pass1 success in AUTO mode (default: 150).",
    )
    parser.add_argument(
        "--docling-alnum-threshold",
        type=float,
        default=0.5,
        help="Minimum alphanumeric ratio for pass1 success in AUTO mode (default: 0.5).",
    )
    
    # PII Redaction options
    parser.add_argument(
        "--enable-redaction",
        action="store_true",
        default=False,
        help="Enable PII redaction stage (removes client names, people names, emails, phones, addresses before chunking).",
    )
    parser.add_argument(
        "--client-redaction-csv",
        type=str,
        default=None,
        help="Path to CSV file with client registry (columns: salesforce_client_id, client_name, industry_label, aliases). Required if --enable-redaction.",
    )
    parser.add_argument(
        "--redaction-model",
        type=str,
        default=DEFAULT_REDACTION_MODEL,
        help=f"OpenAI model for PERSON entity detection (default: {DEFAULT_REDACTION_MODEL}).",
    )
    
    return parser


def main() -> None:
    """Main entry point for document processing script."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Use parallel processing if workers > 1
    if args.workers > 1:
        from src.pipeline.parallel_processor import run_parallel_processing
        
        # Build docling_kwargs if using docling parser
        docling_kwargs = None
        if args.parser_backend == "docling":
            docling_kwargs = {
                "ocr_mode": args.docling_ocr_mode,
                "timeout_seconds": args.docling_timeout_seconds,
                "min_text_chars": args.docling_min_text_chars,
                "min_word_count": args.docling_min_word_count,
                "alnum_threshold": args.docling_alnum_threshold,
            }
        
        print(f"🚀 Starting PARALLEL processing with {args.workers} workers")
        run_parallel_processing(
            discovery_file=args.input,
            workers=args.workers,
            namespace=args.namespace,
            parser_backend=args.parser_backend,
            resume=args.resume,
            limit=args.limit,
            # Selection filters (keep parity with serial path)
            filter_file_type=args.filter_file_type,
            exclude_file_type=args.exclude_file_type,
            modified_after=args.modified_after,
            modified_before=args.modified_before,
            deal_created_after=args.deal_created_after,
            deal_created_before=args.deal_created_before,
            min_size_kb=args.min_size_kb,
            max_size_mb=args.max_size_mb,
            docling_kwargs=docling_kwargs,
            client_redaction_csv=args.client_redaction_csv,
            redaction_model=args.redaction_model,
            enable_redaction=args.enable_redaction,
        )
    else:
        # Serial processing (existing behavior)
        processor = DiscoveredDocumentProcessor()
        processor.run(args)


if __name__ == "__main__":
    main() 