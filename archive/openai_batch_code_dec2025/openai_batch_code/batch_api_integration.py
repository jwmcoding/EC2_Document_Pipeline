#!/usr/bin/env python3
"""
Document Discovery Tool with Batch API Support - Enhanced with cost-efficient batch processing

This version adds OpenAI Batch API support for document classification, reducing costs by 50%
while maintaining all existing functionality for immediate classification.

New Features:
- Batch API support for cost-efficient classification
- Dual-mode operation: immediate vs batch classification
- Progress tracking for batch operations
- Cost estimation and comparison
"""

import os
import sys
import argparse
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

# Import components
from src.config.colored_logging import ColoredLogger, setup_colored_logging
from src.config.settings import Settings
from src.connectors.file_source_interface import FileSourceInterface
from src.connectors.dropbox_client import DropboxClient, DocumentMetadata
from src.connectors.local_filesystem_client import LocalFilesystemClient
from src.utils.discovery_persistence import DiscoveryPersistence


class BatchAPIManager:
    """Manages OpenAI Batch API operations for document classification"""
    
    def __init__(self, openai_api_key: str):
        from openai import OpenAI
        self.client = OpenAI(api_key=openai_api_key)
        self.logger = ColoredLogger("batch_api")
        
    def create_classification_batch(self, documents: List[Dict], batch_id: str) -> str:
        """Create a batch classification job and return the batch job ID"""
        
        # Create JSONL file for batch processing
        jsonl_path = f"batch_classification_{batch_id}.jsonl"
        
        with open(jsonl_path, 'w') as f:
            for idx, doc in enumerate(documents):
                # Create classification prompt
                file_name = doc['file_info']['name']
                file_path = doc['file_info']['path']
                
                classification_request = {
                    "custom_id": f"doc_{batch_id}_{idx}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-4.1-mini",
                        "temperature": 0.1,
                        "response_format": {"type": "json_object"},
                        "messages": [
                            {
                                "role": "system",
                                "content": """You are an expert document classifier for business deal documents. 
                                Classify the document type based on the filename and path.
                                
                                Common document types:
                                - IDD (Implementation and Design Document)
                                - FMV (Fair Market Value)
                                - Contract
                                - Invoice
                                - Technical Specification
                                - Requirements Document
                                - Other
                                
                                Return JSON with:
                                {
                                    "document_type": "type_name",
                                    "confidence": 0.0-1.0,
                                    "reasoning": "explanation",
                                    "alternative_types": [{"type": "alt_type", "confidence": 0.0-1.0}]
                                }"""
                            },
                            {
                                "role": "user",
                                "content": f"File: {file_name}\nPath: {file_path}\n\nClassify this document."
                            }
                        ]
                    }
                }
                
                f.write(json.dumps(classification_request) + '\n')
        
        # Upload file to OpenAI
        self.logger.info(f"📤 Uploading batch file: {jsonl_path}")
        with open(jsonl_path, 'rb') as f:
            batch_file = self.client.files.create(file=f, purpose="batch")
        
        # Create batch job
        batch_job = self.client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        
        self.logger.success(f"✅ Batch job created: {batch_job.id}")
        self.logger.info(f"📊 Documents queued: {len(documents)}")
        
        # Clean up local file
        os.remove(jsonl_path)
        
        return batch_job.id
    
    def check_batch_status(self, batch_job_id: str) -> Dict[str, Any]:
        """Check the status of a batch job"""
        batch_job = self.client.batches.retrieve(batch_job_id)
        return {
            "id": batch_job.id,
            "status": batch_job.status,
            "created_at": batch_job.created_at,
            "completed_at": batch_job.completed_at,
            "failed_at": batch_job.failed_at,
            "request_counts": batch_job.request_counts.__dict__ if batch_job.request_counts else None
        }
    
    def retrieve_batch_results(self, batch_job_id: str) -> List[Dict[str, Any]]:
        """Retrieve and parse batch job results"""
        batch_job = self.client.batches.retrieve(batch_job_id)
        
        if batch_job.status != "completed":
            raise ValueError(f"Batch job not completed. Status: {batch_job.status}")
        
        # Download results
        result_file_id = batch_job.output_file_id
        result_content = self.client.files.content(result_file_id).content
        
        # Parse JSONL results
        results = []
        for line in result_content.decode('utf-8').strip().split('\n'):
            if line:
                result = json.loads(line)
                results.append(result)
        
        return results
    
    def estimate_batch_cost(self, num_documents: int, avg_tokens_per_request: int = 500) -> Dict[str, float]:
        """Estimate costs for batch vs immediate classification"""
        
        # Estimate tokens (input + output)
        input_tokens = num_documents * avg_tokens_per_request
        output_tokens = num_documents * 100  # Estimated JSON response tokens
        
        # Batch API costs (50% discount)
        batch_input_cost = (input_tokens / 1_000_000) * 0.20  # $0.20 per 1M tokens
        batch_output_cost = (output_tokens / 1_000_000) * 0.80  # $0.80 per 1M tokens
        batch_total = batch_input_cost + batch_output_cost
        
        # Immediate API costs
        immediate_input_cost = (input_tokens / 1_000_000) * 0.40  # $0.40 per 1M tokens
        immediate_output_cost = (output_tokens / 1_000_000) * 1.60  # $1.60 per 1M tokens
        immediate_total = immediate_input_cost + immediate_output_cost
        
        return {
            "batch_cost": batch_total,
            "immediate_cost": immediate_total,
            "savings": immediate_total - batch_total,
            "savings_percentage": ((immediate_total - batch_total) / immediate_total) * 100,
            "num_documents": num_documents
        }


class DocumentDiscoveryWithBatch:
    """Enhanced discovery orchestrator with Batch API support"""
    
    def __init__(self):
        # Setup colored logging first
        setup_colored_logging()
        self.logger = ColoredLogger("document_discovery")
        
        # Add file handler for comprehensive logging
        import logging
        import os
        os.makedirs("logs", exist_ok=True)
        file_handler = logging.FileHandler(f"logs/discovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logging.getLogger().addHandler(file_handler)
        
        self.settings = Settings()
        self.source_client: Optional[FileSourceInterface] = None
        self.persistence: Optional[DiscoveryPersistence] = None
        self.batch_manager: Optional[BatchAPIManager] = None
    
    def run(self, args: argparse.Namespace):
        """Run discovery based on arguments"""
        # Handle interactive mode
        if not args.source:
            args = self._interactive_prompt(args)
        
        # Initialize batch manager if classification is enabled
        if args.classify and self.settings.OPENAI_API_KEY:
            self.batch_manager = BatchAPIManager(self.settings.OPENAI_API_KEY)
        
        # Initialize persistence
        self.persistence = DiscoveryPersistence(args.output)
        
        # Check for resume
        if args.resume:
            if not Path(args.output).exists():
                self.logger.error("❌ No existing discovery to resume. Start fresh discovery.")
                return
            
            summary = self.persistence.get_discovery_summary()
            self.logger.info(f"📂 Resuming discovery from: {summary['source_path']}")
            self.logger.info(f"📊 Already discovered: {summary['total_documents']} documents")
            
            # Set source from saved metadata
            args.source = summary['source_type']
            if args.source == 'local':
                args.path = summary['source_path']
            else:
                args.folder = summary['source_path']
        
        # Initialize source client
        self._initialize_source(args)
        
        # Validate connection
        try:
            self.source_client.validate_connection()
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to source: {e}")
            return
        
        # Run discovery
        self._discover_documents(args)
    
    def _interactive_prompt(self, args: argparse.Namespace) -> argparse.Namespace:
        """Interactive prompts for missing arguments with batch API options"""
        print("\n=== Document Discovery Tool with Batch API Support ===\n")
        
        # Source selection
        print("Select document source:")
        print("1. Dropbox")
        print("2. Local filesystem")
        choice = input("> ").strip()
        
        if choice == "1":
            args.source = "dropbox"
            default_folder = "/NPI Data Ownership/2024 Deal Docs"
            folder = input(f"\nEnter Dropbox folder path [{default_folder}]: ").strip()
            args.folder = folder or default_folder
        elif choice == "2":
            args.source = "local"
            path = input("\nEnter local path to documents: ").strip()
            if not path:
                self.logger.error("❌ Path is required for local source")
                sys.exit(1)
            args.path = path
        else:
            self.logger.error("❌ Invalid choice")
            sys.exit(1)
        
        # LLM classification options
        print("\nDocument Classification Options:")
        print("1. No classification")
        print("2. Immediate classification (higher cost)")
        print("3. Batch classification (50% cost savings, 24hr processing)")
        
        classify_choice = input("Select option [3]: ").strip() or "3"
        
        if classify_choice == "1":
            args.classify = False
            args.use_batch = False
        elif classify_choice == "2":
            args.classify = True
            args.use_batch = False
        elif classify_choice == "3":
            args.classify = True
            args.use_batch = True
        else:
            self.logger.error("❌ Invalid choice")
            sys.exit(1)
        
        # Batch size
        batch_input = input(f"\nDiscovery batch size [{args.batch_size}]: ").strip()
        if batch_input:
            args.batch_size = int(batch_input)
        
        # Output file
        output_input = input(f"\nOutput file [{args.output}]: ").strip()
        if output_input:
            args.output = output_input
        
        return args
    
    def _initialize_source(self, args: argparse.Namespace):
        """Initialize the appropriate source client"""
        if args.source == "dropbox":
            if not self.settings.DROPBOX_ACCESS_TOKEN:
                self.logger.error("❌ DROPBOX_ACCESS_TOKEN not found in environment")
                sys.exit(1)
            
            # Only pass OpenAI key for immediate classification
            openai_key = self.settings.OPENAI_API_KEY if (args.classify and not args.use_batch) else None
            self.source_client = DropboxClient(
                self.settings.DROPBOX_ACCESS_TOKEN,
                openai_api_key=openai_key
            )
            
            # Set source metadata
            self.persistence.set_discovery_metadata(
                source_type="dropbox",
                source_path=args.folder,
                llm_enabled=args.classify,
                batch_mode=args.use_batch if args.classify else False
            )
            
        elif args.source == "local":
            if not args.path:
                self.logger.error("❌ Path is required for local source")
                sys.exit(1)
            
            # Only pass OpenAI key for immediate classification
            openai_key = self.settings.OPENAI_API_KEY if (args.classify and not args.use_batch) else None
            self.source_client = LocalFilesystemClient(
                args.path,
                openai_api_key=openai_key
            )
            
            # Set source metadata
            self.persistence.set_discovery_metadata(
                source_type="local",
                source_path=args.path,
                llm_enabled=args.classify,
                batch_mode=args.use_batch if args.classify else False
            )
        else:
            self.logger.error(f"❌ Unknown source: {args.source}")
            sys.exit(1)
    
    def _discover_documents(self, args: argparse.Namespace):
        """Run document discovery with optional batch processing"""
        start_time = datetime.now()
        
        # Get folder path based on source
        if args.source == "dropbox":
            folder_path = args.folder
        else:
            folder_path = ""  # Local filesystem uses base path
        
        # Check for resume
        progress = self.persistence.load_progress()
        last_path = progress.get("last_processed_path")
        
        if args.resume and last_path:
            self.logger.info(f"🔄 Resuming from: {last_path}")
        
        try:
            batch = []
            batch_num = progress.get("current_batch", 0) + 1
            total_discovered = progress.get("documents_discovered", 0)
            resumed = False if args.resume and last_path else True
            
            # Get document iterator (without immediate classification if using batch)
            classify_immediate = args.classify and not args.use_batch
            
            if hasattr(self.source_client, 'list_documents_as_metadata'):
                document_iterator = self.source_client.list_documents_as_metadata(
                    folder_path,
                    classify_with_llm=classify_immediate,
                    file_types=None
                )
            else:
                document_iterator = self.source_client.list_documents(folder_path)
            
            # Show cost estimation for batch processing
            if args.classify and args.use_batch:
                # Estimate document count (rough estimate)
                estimated_docs = args.max_docs or 1000
                cost_estimate = self.batch_manager.estimate_batch_cost(estimated_docs)
                
                self.logger.info(f"💰 Cost Estimation for {estimated_docs} documents:")
                self.logger.info(f"   Batch API: ${cost_estimate['batch_cost']:.2f}")
                self.logger.info(f"   Immediate API: ${cost_estimate['immediate_cost']:.2f}")
                self.logger.info(f"   Savings: ${cost_estimate['savings']:.2f} ({cost_estimate['savings_percentage']:.1f}%)")
            
            self.logger.info(f"🔍 Starting discovery in batch mode (batch size: {args.batch_size})")
            
            # Collect documents for batch processing
            documents_for_batch = []
            
            for doc in document_iterator:
                # Skip until we reach resume point
                if not resumed and last_path:
                    if doc.path == last_path:
                        resumed = True
                        self.logger.info(f"✅ Found resume point, continuing discovery...")
                    continue
                
                # Convert to discovery format
                discovery_doc = self._convert_to_discovery_format(doc)
                
                batch.append(discovery_doc)
                total_discovered += 1
                
                # Store for batch classification if enabled
                if args.classify and args.use_batch:
                    documents_for_batch.append(discovery_doc)
                
                # Real-time progress every 10 documents
                if total_discovered % 10 == 0:
                    elapsed = datetime.now() - start_time
                    rate = total_discovered / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                    self.logger.info(f"📈 Processed {total_discovered} documents | Rate: {rate:.1f} docs/sec")
                
                # Save batch when full
                if len(batch) >= args.batch_size:
                    self.persistence.add_batch(batch, batch_num)
                    self.logger.progress(f"💾 Saved batch {batch_num}: {len(batch)} documents (Total: {total_discovered})")
                    
                    # Save progress
                    self.persistence.save_progress(doc.path)
                    
                    batch = []
                    batch_num += 1
                
                # Check max documents limit
                if args.max_docs and total_discovered >= args.max_docs:
                    self.logger.warning(f"⚠️ Reached max documents limit: {args.max_docs}")
                    break
            
            # Save final batch
            if batch:
                self.persistence.add_batch(batch, batch_num)
                self.logger.progress(f"💾 Saved final batch {batch_num}: {len(batch)} documents")
            
            # Submit batch classification job if enabled
            if args.classify and args.use_batch and documents_for_batch:
                self.logger.info(f"🚀 Submitting batch classification job for {len(documents_for_batch)} documents")
                
                batch_job_id = self.batch_manager.create_classification_batch(
                    documents_for_batch, 
                    datetime.now().strftime('%Y%m%d_%H%M%S')
                )
                
                # Save batch job info
                self.persistence.save_batch_job(batch_job_id, len(documents_for_batch))
                
                self.logger.success(f"✅ Batch classification job submitted: {batch_job_id}")
                self.logger.info(f"⏰ Results will be available within 24 hours")
                self.logger.info(f"💡 Use: python check_batch_classification.py --job-id {batch_job_id}")
            
            # Mark discovery complete
            self.persistence.mark_discovery_complete()
            
            # Final summary
            elapsed = datetime.now() - start_time
            summary = self.persistence.get_discovery_summary()
            
            self.logger.success(f"\n🎉 Discovery Complete!")
            self.logger.info(f"📊 Total documents: {summary['total_documents']}")
            
            if args.classify:
                if args.use_batch:
                    self.logger.info(f"🔄 Batch classification job queued")
                else:
                    if summary.get('statistics'):
                        stats = summary['statistics']
                        self.logger.info(f"📊 Classified documents: {stats['classified_documents']} ({stats['classification_rate']:.1%})")
                        self.logger.info(f"📊 Document types: {json.dumps(stats['document_types'], indent=2)}")
            
            self.logger.info(f"⏱️ Time elapsed: {elapsed}")
            self.logger.info(f"💾 Results saved to: {args.output}")
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️ Discovery interrupted by user")
            self.persistence.flush_buffer()
            self.logger.info(f"💾 Progress saved. Use --resume to continue.")
        except Exception as e:
            self.logger.error(f"❌ Discovery error: {e}")
            self.persistence.flush_buffer()
            raise
    
    def _convert_to_discovery_format(self, doc: Any) -> Dict[str, Any]:
        """Convert document metadata to discovery JSON format"""
        # Handle DocumentMetadata from Dropbox/Local clients
        if hasattr(doc, '__dict__'):
            doc_dict = doc.__dict__.copy()
            
            # Build discovery format
            discovery_doc = {
                "source_metadata": {
                    "source_type": self.persistence.data["discovery_metadata"]["source_type"],
                    "source_id": doc_dict.get('dropbox_id', doc_dict.get('source_id', '')),
                    "source_path": doc_dict.get('full_path', doc_dict.get('path', ''))
                },
                "file_info": {
                    "path": doc_dict.get('path', ''),
                    "name": doc_dict.get('name', ''),
                    "size": doc_dict.get('size', 0),
                    "size_mb": doc_dict.get('size_mb', 0.0),
                    "file_type": doc_dict.get('file_type', ''),
                    "modified_time": doc_dict.get('modified_time', ''),
                    "content_hash": doc_dict.get('content_hash')
                },
                "business_metadata": {
                    "year": doc_dict.get('year'),
                    "week_number": doc_dict.get('week_number'),
                    "week_date": doc_dict.get('week_date'),
                    "vendor": doc_dict.get('vendor'),
                    "client": doc_dict.get('client'),
                    "deal_number": doc_dict.get('deal_number'),
                    "deal_name": doc_dict.get('deal_name'),
                    "extraction_confidence": doc_dict.get('extraction_confidence', 0.0),
                    "path_components": doc_dict.get('path_components', [])
                }
            }
            
            # Add LLM classification if present (immediate classification)
            if doc_dict.get('document_type'):
                discovery_doc["llm_classification"] = {
                    "document_type": doc_dict.get('document_type'),
                    "confidence": doc_dict.get('document_type_confidence', 0.0),
                    "reasoning": doc_dict.get('classification_reasoning'),
                    "classification_method": doc_dict.get('classification_method', 'gpt-4.1-mini'),
                    "alternative_types": doc_dict.get('alternative_document_types', []),
                    "tokens_used": doc_dict.get('classification_tokens_used', 0),
                    "classification_timestamp": datetime.now().isoformat(),
                    "batch_processed": False
                }
            
            return discovery_doc
        else:
            # Handle basic FileMetadata
            return {
                "source_metadata": {
                    "source_type": doc.source_type,
                    "source_id": doc.source_id,
                    "source_path": doc.full_source_path
                },
                "file_info": {
                    "path": doc.path,
                    "name": doc.name,
                    "size": doc.size,
                    "size_mb": doc.size_mb,
                    "file_type": doc.file_type,
                    "modified_time": doc.modified_time,
                    "content_hash": doc.content_hash
                },
                "business_metadata": {},
                "processing_status": {
                    "processed": False
                }
            }


def create_argument_parser():
    """Create command line argument parser with batch API options"""
    parser = argparse.ArgumentParser(
        description="Discover documents with optional Batch API classification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (prompts for all options including Batch API)
  python discover_documents.py

  # Discover with immediate classification
  python discover_documents.py --source dropbox --folder "/2024 Deal Docs" --classify

  # Discover with batch classification (50% cost savings)
  python discover_documents.py --source dropbox --folder "/2024 Deal Docs" --classify --use-batch

  # Local filesystem with batch classification
  python discover_documents.py --source local --path /Users/docs --classify --use-batch

  # Resume interrupted discovery
  python discover_documents.py --resume
        """
    )
    
    # Source selection
    parser.add_argument("--source", choices=["dropbox", "local"],
                       help="Document source (dropbox or local)")
    parser.add_argument("--folder", type=str,
                       help="Dropbox folder path (for dropbox source)")
    parser.add_argument("--path", type=str,
                       help="Local filesystem path (for local source)")
    
    # Classification options
    parser.add_argument("--classify", action="store_true",
                       help="Enable LLM classification")
    parser.add_argument("--use-batch", action="store_true",
                       help="Use Batch API for classification (50% cost savings)")
    
    # Discovery options
    parser.add_argument("--batch-size", type=int, default=100,
                       help="Number of documents per batch (default: 100)")
    parser.add_argument("--max-docs", type=int,
                       help="Maximum documents to discover (for testing)")
    
    # Output options
    parser.add_argument("--output", type=str, default="discovery_results.json",
                       help="Output JSON file (default: discovery_results.json)")
    
    # Resume option
    parser.add_argument("--resume", action="store_true",
                       help="Resume from previous discovery")
    
    return parser


def main():
    """Main entry point"""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Run discovery with batch API support
    discovery = DocumentDiscoveryWithBatch()
    discovery.run(args)


if __name__ == "__main__":
    main()
