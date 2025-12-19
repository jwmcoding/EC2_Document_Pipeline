#!/usr/bin/env python3
"""
Document Discovery Tool - Fast Enumeration with Basic Classification

This tool performs Phase 1: Discovery with Basic Classification:
- Recursive file system scanning (Dropbox or local filesystem)
- Business metadata extraction from file paths (vendor, client, deal numbers)
- Basic file type classification (.pdf, .docx, .xlsx, etc.)
- Document categorization based on path structure
- Fast enumeration without content analysis
- JSON output for Phase 2 processing

Phase 1 Basic Classification: File path analysis, business metadata, file types
Phase 2 Advanced Classification: LLM document type classification, content analysis
(handled by process_discovered_documents.py)
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
from src.connectors.salesforce_file_source import SalesforceFileSource
from src.connectors.raw_salesforce_export_connector import RawSalesforceExportConnector
from src.utils.discovery_persistence import DiscoveryPersistence

# Discovery defaults
DEFAULT_BATCH_SIZE: int = 100
MIN_FILE_SIZE_KB_DEFAULT: float = 10.0

__all__ = ["DocumentDiscovery", "create_argument_parser", "main"]

class DocumentDiscovery:
    """Document discovery and metadata extraction (no LLM classification)"""
    
    def __init__(self):
        """Initialize discovery with colored logging"""
        setup_colored_logging()
        self.logger = ColoredLogger("discovery")
        self.settings = Settings()
        self.source_client: Optional[FileSourceInterface] = None
        self.persistence: Optional[DiscoveryPersistence] = None
        
    def run(self, args: argparse.Namespace) -> None:
        """Run document discovery based on arguments.
        
        Args:
            args: Parsed command line arguments.
            
        Raises:
            ValueError: If required arguments are missing or invalid.
            RuntimeError: If discovery fails.
        """
        try:
            # Show summary mode - just display stats from existing discovery file
            if getattr(args, 'show_summary', False):
                if not Path(args.output).exists():
                    self.logger.error(f"❌ Discovery file not found: {args.output}")
                    return
                self.persistence = DiscoveryPersistence(args.output)
                detailed_summary = self.persistence.get_detailed_summary()
                self._display_detailed_summary(detailed_summary)
                return
            
            # Initialize persistence
            self.persistence = DiscoveryPersistence(args.output)
            
            # Resume logic
            if args.resume and Path(args.output).exists():
                self.logger.info("🔄 Resuming from existing discovery file")
                summary = self.persistence.get_discovery_summary()
                
                if summary.get('discovery_completed', False):
                    self.logger.success(f"✅ Discovery already complete: {args.output}")
                    detailed_summary = self.persistence.get_detailed_summary()
                    self._display_detailed_summary(detailed_summary)
                    return
                else:
                    self.logger.info("🔄 Resuming interrupted discovery")
            
            # Initialize source client
            self._initialize_source_client(args)
            
            # Start discovery
            self._run_discovery(args)
            
        except KeyboardInterrupt:
            self.logger.warning("\n⚠️ Discovery interrupted by user")
            if self.persistence:
                self.persistence.flush_buffer()
                self.logger.info("💾 Progress saved. Use --resume to continue.")
        except Exception as e:
            self.logger.error(f"❌ Discovery failed: {e}")
            raise
    
    def _initialize_source_client(self, args: argparse.Namespace) -> None:
        """Initialize the appropriate source client based on source type.
        
        Args:
            args: Parsed command line arguments containing source configuration.
            
        Raises:
            ValueError: If required arguments are missing for the selected source type.
        """
        if args.source == "dropbox":
            if not args.folder:
                raise ValueError("--folder is required for Dropbox source")
            if not self.settings.DROPBOX_ACCESS_TOKEN:
                raise ValueError("DROPBOX_ACCESS_TOKEN not found in environment")
            
            self.source_client = DropboxClient(self.settings.DROPBOX_ACCESS_TOKEN)
            self.logger.info("✅ Dropbox connection validated")
            
        elif args.source == "local":
            if not args.path:
                raise ValueError("--path is required for local source")
            if not Path(args.path).exists():
                raise ValueError(f"Local path does not exist: {args.path}")
            
            # 🛡️ SAFETY CHECK: Warn if processing organized Salesforce files with local source
            path_str = str(args.path).lower()
            if any(indicator in path_str for indicator in ['salesforce', 'organized_salesforce', 'sf-files']):
                self.logger.warning("⚠️ " + "="*60)
                self.logger.warning("⚠️ POTENTIAL MISCONFIGURATION DETECTED!")
                self.logger.warning("⚠️ You're using --source local for what appears to be Salesforce files.")
                self.logger.warning("⚠️ This will NOT populate vendor_id fields from Deal metadata.")
                self.logger.warning("⚠️ Consider using --source salesforce instead.")
                self.logger.warning("⚠️ See SALESFORCE_PROCESSING_WORKFLOW.md for guidance.")
                self.logger.warning("⚠️ " + "="*60)
            
            self.source_client = LocalFilesystemClient(args.path)
            self.logger.info(f"✅ Local filesystem connection validated")
            
        elif args.source == "salesforce":
            # Salesforce source requires organized files directory and mapping files
            if not args.salesforce_files_dir:
                raise ValueError("--salesforce-files-dir is required for Salesforce source")
            if not args.file_mapping_csv:
                raise ValueError("--file-mapping-csv is required for Salesforce source")
            if not args.deal_metadata_csv:
                raise ValueError("--deal-metadata-csv is required for Salesforce source")
            
            # Default: require deal association and minimum file size to skip tiny icons/logos
            # Handle override flags
            require_deal = not getattr(args, 'include_unmapped', False)  # --include-unmapped overrides
            min_size_kb = getattr(args, 'min_size_kb', MIN_FILE_SIZE_KB_DEFAULT) or MIN_FILE_SIZE_KB_DEFAULT  # Default 10KB to skip icons
            
            self.source_client = SalesforceFileSource(
                organized_files_dir=args.salesforce_files_dir,
                file_mapping_csv=args.file_mapping_csv,
                deal_metadata_csv=args.deal_metadata_csv,
                client_mapping_csv=getattr(args, 'client_mapping_csv', None),
                vendor_mapping_csv=getattr(args, 'vendor_mapping_csv', None),
                require_deal_association=require_deal,
                min_file_size_kb=min_size_kb
            )
            self.logger.info(f"✅ Salesforce file source initialized (require_deal={require_deal}, min_size={min_size_kb}KB)")
        
        elif args.source == "salesforce_raw":
            # Raw Salesforce export source requires export root and CSV files
            if not args.export_root_dir:
                raise ValueError("--export-root-dir is required for salesforce_raw source")
            if not args.content_versions_csv:
                raise ValueError("--content-versions-csv is required for salesforce_raw source")
            # content_documents_csv is now optional - will derive from ContentVersion if not provided
            if not args.content_document_links_csv:
                raise ValueError("--content-document-links-csv is required for salesforce_raw source")
            if not args.deal_metadata_csv:
                raise ValueError("--deal-metadata-csv is required for salesforce_raw source")
            
            # ⚠️ CRITICAL WARNING: Check for missing mapping CSVs
            if not args.vendor_mapping_csv:
                self.logger.warning("=" * 70)
                self.logger.warning("⚠️  WARNING: --vendor-mapping-csv not provided!")
                self.logger.warning("⚠️  vendor_name will be NULL for all documents.")
                self.logger.warning("⚠️  Use: --vendor-mapping-csv /path/to/SF-Vendor_mapping.csv")
                self.logger.warning("=" * 70)
            
            if not args.client_mapping_csv:
                self.logger.warning("=" * 70)
                self.logger.warning("⚠️  WARNING: --client-mapping-csv not provided!")
                self.logger.warning("⚠️  client_name will be NULL for all documents.")
                self.logger.warning("⚠️  Use: --client-mapping-csv /path/to/SF-Cust-Mapping.csv")
                self.logger.warning("=" * 70)
            
            self.source_client = RawSalesforceExportConnector(
                export_root_dir=args.export_root_dir,
                content_versions_csv=args.content_versions_csv,
                content_documents_csv=getattr(args, 'content_documents_csv', None),  # Optional
                content_document_links_csv=args.content_document_links_csv,
                deal_metadata_csv=args.deal_metadata_csv,
                client_mapping_csv=getattr(args, 'client_mapping_csv', None),
                vendor_mapping_csv=getattr(args, 'vendor_mapping_csv', None),
                deal_mapping_csv=getattr(args, 'deal_mapping_csv', None)
            )
            self.logger.info("✅ Raw Salesforce export connector initialized")
            
            # Print export statistics
            if hasattr(self.source_client, 'print_export_statistics'):
                self.source_client.print_export_statistics()
            
        else:
            raise ValueError(f"Unknown source: {args.source}")
    
    def _run_discovery(self, args: argparse.Namespace) -> None:
        """Run the discovery process.
        
        Scans the configured source, extracts metadata, and saves results
        to the discovery JSON file.
        
        Args:
            args: Parsed command line arguments.
        """
        start_time = datetime.now()
        
        # Get the folder path for discovery
        if args.source == "dropbox":
            folder_path = args.folder
        elif args.source == "salesforce_raw":
            # IMPORTANT: We still persist a source_path so Phase 2 can download files
            # deterministically (especially for parallel processing on EC2).
            folder_path = args.export_root_dir
        else:
            folder_path = args.path
        
        self.logger.info("🔍 Starting discovery in batch mode (batch size: {})".format(args.batch_size))
        
        # Initialize discovery metadata
        self.persistence.set_discovery_metadata(
            source_type=args.source,
            source_path=folder_path,
            llm_enabled=False,  # LLM classification happens in processing
            batch_mode=False
        )
        
        # Run discovery
        total_discovered = 0
        batch_count = 0
        
        self.logger.info(f"🔍 Scanning {args.source} {'directory' if args.source == 'local' else 'folder'}: {folder_path}")
        
        # Discover documents (with business metadata)
        if hasattr(self.source_client, 'list_documents_as_metadata'):
            # Check if connector supports deal filtering
            if hasattr(self.source_client, 'get_export_statistics'):
                # Raw Salesforce connector - supports filtering
                require_deal = getattr(args, 'require_deal_association', False)
                documents_generator = self.source_client.list_documents_as_metadata(
                    folder_path, 
                    require_deal_association=require_deal
                )
            else:
                # Other connectors
                documents_generator = self.source_client.list_documents_as_metadata(folder_path)
        else:
            documents_generator = self.source_client.list_documents(folder_path)
        current_batch = []
        
        for doc_metadata in documents_generator:
            # Convert DocumentMetadata to dictionary format
            doc_dict = self._convert_metadata_to_dict(doc_metadata)
            current_batch.append(doc_dict)
            total_discovered += 1
            
            # Progress update every 10 documents
            if total_discovered % 10 == 0:
                elapsed = datetime.now() - start_time
                rate = total_discovered / elapsed.total_seconds()
                self.logger.info(f"📈 Processed {total_discovered} documents | Rate: {rate:.1f} docs/sec")
            
            # Check if we hit max documents limit
            if args.max_docs and total_discovered >= args.max_docs:
                self.logger.warning(f"⚠️ Reached max documents limit: {args.max_docs}")
                break
            
            # Process batch when full
            if len(current_batch) >= args.batch_size:
                batch_count += 1
                self.logger.info(f"💾 Saving batch {batch_count} with {len(current_batch)} documents")
                self.persistence.add_batch(current_batch, batch_count)
                current_batch = []
        
        # Save final batch if any documents remain
        if current_batch:
            batch_count += 1
            self.logger.info(f"💾 Saved final batch {batch_count}: {len(current_batch)} documents")
            self.persistence.add_batch(current_batch, batch_count)
        
        # Mark discovery as complete
        self.persistence.mark_discovery_complete()
        
        # Display final summary
        elapsed = datetime.now() - start_time
        self.logger.success("🎉 Phase 1: Basic Classification Complete!")
        self.logger.info(f"⏱️ Time elapsed: {elapsed}")
        
        # Display detailed summary
        detailed_summary = self.persistence.get_detailed_summary()
        self._display_detailed_summary(detailed_summary)
        self.logger.info(f"💾 Results saved to: {args.output}")
    
    def _convert_metadata_to_dict(self, doc_metadata: DocumentMetadata) -> Dict[str, Any]:
        """Convert DocumentMetadata object to dictionary format with Phase 1 Basic Classification"""
        
        # Detect source type more accurately
        source_type = "dropbox"
        if hasattr(self.source_client, 'export_root_dir'):
            source_type = "salesforce_raw"
        elif hasattr(self.source_client, 'base_path'):
            source_type = "local"
        elif hasattr(self.source_client, 'organized_files_dir'):
            source_type = "salesforce"
        
        return {
            "source_metadata": {
                "source_type": source_type,
                "source_id": getattr(doc_metadata, 'salesforce_content_version_id', None) or 
                           getattr(doc_metadata, 'dropbox_id', '') or 
                           f"local_{hash(doc_metadata.path)}",
                "source_path": doc_metadata.path
            },
            "file_info": {
                "path": doc_metadata.path,
                "name": doc_metadata.name,
                "size": doc_metadata.size,
                "size_mb": round(doc_metadata.size / (1024 * 1024), 2),
                "file_type": doc_metadata.file_type,
                "modified_time": doc_metadata.modified_time,
                "content_hash": doc_metadata.content_hash
            },
            "business_metadata": {
                "deal_creation_date": doc_metadata.deal_creation_date,
                "week_number": doc_metadata.week_number,
                "week_date": doc_metadata.week_date,
                "vendor": doc_metadata.vendor,
                "client": doc_metadata.client,
                "deal_number": doc_metadata.deal_number,
                "deal_name": doc_metadata.deal_name,
                "extraction_confidence": getattr(doc_metadata, 'extraction_confidence', 0.0),
                "path_components": doc_metadata.path_components or []
            },
            "deal_metadata": {
                # Core deal information
                "deal_id": getattr(doc_metadata, 'deal_id', None),
                "salesforce_deal_id": getattr(doc_metadata, 'salesforce_deal_id', None),
                "deal_subject": getattr(doc_metadata, 'deal_subject', None),
                "deal_status": getattr(doc_metadata, 'deal_status', None),
                "deal_reason": getattr(doc_metadata, 'deal_reason', None),
                "deal_start_date": getattr(doc_metadata, 'deal_start_date', None),
                "negotiated_by": getattr(doc_metadata, 'negotiated_by', None),
                
                # Financial metrics
                "proposed_amount": getattr(doc_metadata, 'proposed_amount', None),
                "final_amount": getattr(doc_metadata, 'final_amount', None),
                "savings_1yr": getattr(doc_metadata, 'savings_1yr', None),
                "savings_3yr": getattr(doc_metadata, 'savings_3yr', None),
                "savings_target": getattr(doc_metadata, 'savings_target', None),
                "savings_percentage": getattr(doc_metadata, 'savings_percentage', None),
                
                # Client/Vendor information
                "client_id": getattr(doc_metadata, 'client_id', None),
                "client_name": getattr(doc_metadata, 'client_name', None),
                "salesforce_client_id": getattr(doc_metadata, 'salesforce_client_id', None),
                "vendor_id": getattr(doc_metadata, 'vendor_id', None),
                "vendor_name": getattr(doc_metadata, 'vendor_name', None),
                "salesforce_vendor_id": getattr(doc_metadata, 'salesforce_vendor_id', None),
                
                # Contract information
                "contract_term": getattr(doc_metadata, 'contract_term', None),
                "contract_start": getattr(doc_metadata, 'contract_start', None),
                "contract_end": getattr(doc_metadata, 'contract_end', None),
                "effort_level": getattr(doc_metadata, 'effort_level', None),
                "has_fmv_report": getattr(doc_metadata, 'has_fmv_report', None),
                "deal_origin": getattr(doc_metadata, 'deal_origin', None),
                
                # ENHANCED FINANCIAL FIELDS (Missing from original!)
                "savings_achieved": getattr(doc_metadata, 'savings_achieved', None),
                "fixed_savings": getattr(doc_metadata, 'fixed_savings', None),
                "savings_target_full_term": getattr(doc_metadata, 'savings_target_full_term', None),
                "final_amount_full_term": getattr(doc_metadata, 'final_amount_full_term', None),
                
                # RICH NARRATIVE CONTENT (Missing from original!)
                "current_narrative": getattr(doc_metadata, 'current_narrative', None),
                "customer_comments": getattr(doc_metadata, 'customer_comments', None),
                "content_source": getattr(doc_metadata, 'content_source', None),
                
                # Deal classification fields (added December 2025)
                "report_type": getattr(doc_metadata, 'report_type', None),
                "description": getattr(doc_metadata, 'description', None),
                "project_type": getattr(doc_metadata, 'project_type', None),
                "competition": getattr(doc_metadata, 'competition', None),
                "npi_analyst": getattr(doc_metadata, 'npi_analyst', None),
                "dual_multi_sourcing": getattr(doc_metadata, 'dual_multi_sourcing', None),
                "time_pressure": getattr(doc_metadata, 'time_pressure', None),
                "advisor_network_used": getattr(doc_metadata, 'advisor_network_used', None),
                
                # Mapping status (for debugging)
                "mapping_status": getattr(doc_metadata, 'mapping_status', None),
                "mapping_method": getattr(doc_metadata, 'mapping_method', None),
                "mapping_reason": getattr(doc_metadata, 'mapping_reason', None)
            },
            # Phase 2 Advanced Classification: LLM document types (added during processing)
            "llm_classification": {
                "document_type": None,  # Will be classified in Phase 2: IDD, FMV, Contract, etc.
                "confidence": 0.0,
                "reasoning": None,
                "classification_method": "pending_phase_2_processing",
                "alternative_types": [],
                "tokens_used": 0
            },
            "processing_status": {
                "processed": False,
                "processing_date": None
            }
        }
    
    def _display_summary(self, summary: Dict[str, Any]) -> None:
        """Display discovery summary.
        
        Args:
            summary: Discovery summary dictionary.
        """
        self.logger.info(f"\n📊 Discovery Summary:")
        self.logger.info(f"📂 Source: {summary['source_type']} ({summary['source_path']})")
        self.logger.info(f"📄 Total documents: {summary['total_documents']}")
        self.logger.info(f"🏷️ LLM classification: Handled by processing pipeline")
        self.logger.info(f"💾 Output file: {summary.get('output_file', 'N/A')}")
    
    def _display_detailed_summary(self, summary: Dict[str, Any]) -> None:
        """Display comprehensive discovery summary with statistics.
        
        Shows date ranges, file type distribution, size statistics,
        and other detailed metrics.
        
        Args:
            summary: Discovery summary dictionary with detailed statistics.
        """
        print(f"\n{'='*70}")
        print(f"📊 DISCOVERY SUMMARY")
        print(f"{'='*70}")
        print(f"📂 Source: {summary['source_type']}")
        print(f"📁 Path: {summary['source_path']}")
        print(f"📄 Total documents: {summary['total_documents']:,}")
        print(f"💾 Output file: {summary.get('output_file', 'N/A')}")
        
        detailed = summary.get('detailed_statistics', {})
        
        # Date Range Analysis
        date_range = detailed.get('date_range', {})
        if date_range:
            print(f"\n📅 DATE RANGE ANALYSIS:")
            print(f"   Earliest year: {date_range.get('earliest_year', 'N/A')}")
            print(f"   Latest year: {date_range.get('latest_year', 'N/A')}")
            print(f"   Pre-2000 documents: {date_range.get('pre_2000_count', 0):,}")
            print(f"   Year 2000+: {date_range.get('2000_and_later_count', 0):,}")
        
        # Year Distribution
        year_dist = detailed.get('year_distribution', {})
        if year_dist:
            print(f"\n📆 DOCUMENTS BY YEAR:")
            # Sort years and show in columns
            sorted_years = sorted([y for y in year_dist.keys() if y != "no_date"])
            for year in sorted_years:
                count = year_dist[year]
                bar = "█" * min(50, int(count / max(year_dist.values()) * 50)) if max(year_dist.values()) > 0 else ""
                print(f"   {year}: {count:>6,} {bar}")
            if "no_date" in year_dist:
                print(f"   No date: {year_dist['no_date']:>6,}")
        
        # File Type Distribution
        file_dist = detailed.get('file_type_distribution', {})
        if file_dist:
            print(f"\n📁 FILE TYPES:")
            for file_type, count in sorted(file_dist.items(), key=lambda x: x[1], reverse=True)[:15]:
                pct = 100 * count / summary['total_documents'] if summary['total_documents'] > 0 else 0
                print(f"   {file_type:<10} {count:>8,} ({pct:>5.1f}%)")
            if len(file_dist) > 15:
                print(f"   ... and {len(file_dist) - 15} more file types")
        
        # Size Statistics
        size_stats = detailed.get('size_statistics', {})
        if size_stats:
            print(f"\n💾 SIZE STATISTICS:")
            print(f"   Total size: {size_stats.get('total_size_mb', 0):,.1f} MB ({size_stats.get('total_size_mb', 0)/1024:.1f} GB)")
            print(f"   Average size: {size_stats.get('avg_size_mb', 0):.2f} MB")
            print(f"   Largest file: {size_stats.get('max_size_mb', 0):.2f} MB")
            print(f"   Smallest file: {size_stats.get('min_size_mb', 0):.2f} MB")
            print(f"   Files > 10MB: {size_stats.get('files_over_10mb', 0):,}")
            print(f"   Files > 50MB: {size_stats.get('files_over_50mb', 0):,}")
        
        print(f"\n{'='*70}")


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser.
    
    Returns:
        Configured ArgumentParser instance with all discovery options.
    """
    parser = argparse.ArgumentParser(
        description="Phase 1: Discovery with Basic Classification (file enumeration + business metadata)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Phase 1: Basic Classification Examples:
  # Discover documents from Dropbox with basic classification
  python discover_documents.py --source dropbox --folder "/2024 Deal Docs"

  # Discover documents from local filesystem with business metadata  
  python discover_documents.py --source local --path "/Users/docs/2024 Deal Docs"

  # Discover Salesforce files with Deal metadata enrichment (organized files)
  python discover_documents.py --source salesforce \\
    --salesforce-files-dir "/Volumes/Jeff_2TB/organized_salesforce_v2" \\
    --file-mapping-csv "organized_files_to_deal_mapping.csv" \\
    --deal-metadata-csv "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/Deal__c.csv" \\
    --client-mapping-csv "client_mapping.csv" \\
    --vendor-mapping-csv "vendor_mapping.csv" \\
    --output "salesforce_discovery.json"

  # Discover from raw Salesforce export bundle (differential exports)
  python discover_documents.py --source salesforce_raw \\
    --export-root-dir "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771" \\
    --content-versions-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/content_versions.csv" \\
    --content-documents-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/content_documents.csv" \\
    --content-document-links-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/content_document_links.csv" \\
    --deal-metadata-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/deal__cs.csv" \\
    --client-mapping-csv "client_mapping.csv" \\
    --vendor-mapping-csv "vendor_mapping.csv" \\
    --deal-mapping-csv "organized_files_to_deal_mapping.csv" \\
    --output "raw_salesforce_discovery.json"

  # Limit discovery for testing
  python discover_documents.py --source local --path "/docs" --max-docs 100

  # Resume interrupted discovery
  python discover_documents.py --source local --path "/docs" --resume

Classification Phases:
  Phase 1 (Basic): File paths → business metadata + file types (THIS TOOL)
  Phase 2 (Advanced): Content → LLM document types + vectors (process_discovered_documents.py)
        """
    )
    
    # Source options
    parser.add_argument("--source", choices=["dropbox", "local", "salesforce", "salesforce_raw"], required=True,
                       help="Document source type")
    parser.add_argument("--folder", type=str,
                       help="Dropbox folder path (for dropbox source)")
    parser.add_argument("--path", type=str,
                       help="Local filesystem path (for local source)")
    
    # Salesforce source options (organized files)
    parser.add_argument("--salesforce-files-dir", type=str,
                       help="Path to organized Salesforce files directory (for salesforce source)")
    parser.add_argument("--file-mapping-csv", type=str,
                       help="Path to organized_files_to_deal_mapping.csv (for salesforce source)")
    parser.add_argument("--deal-metadata-csv", type=str,
                       help="Path to Deal__c.csv from Salesforce export (for salesforce/salesforce_raw source)")
    parser.add_argument("--client-mapping-csv", type=str,
                       help="Optional path to Client ID -> Name mapping CSV (for salesforce/salesforce_raw source)")
    parser.add_argument("--vendor-mapping-csv", type=str,
                       help="Optional path to Vendor ID -> Name mapping CSV (for salesforce/salesforce_raw source)")
    parser.add_argument("--deal-mapping-csv", type=str,
                       help="Optional path to organized_files_to_deal_mapping.csv for user-friendly deal numbers (for salesforce_raw source)")
    parser.add_argument("--require-deal-association", action="store_true", default=True,
                       help="Only process documents with valid deal associations. "
                            "Default: True for 'salesforce' source, False for 'salesforce_raw'.")
    parser.add_argument("--include-unmapped", action="store_true",
                       help="Include files without deal associations (overrides --require-deal-association)")
    
    # Raw Salesforce export options
    parser.add_argument("--export-root-dir", type=str,
                       help="Path to raw Salesforce export root directory (for salesforce_raw source)")
    parser.add_argument("--content-versions-csv", type=str,
                       help="Path to content_versions.csv (for salesforce_raw source)")
    parser.add_argument("--content-documents-csv", type=str,
                       help="Path to content_documents.csv (for salesforce_raw source)")
    parser.add_argument("--content-document-links-csv", type=str,
                       help="Path to content_document_links.csv (for salesforce_raw source)")
    
    # Discovery options
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                       help=f"Number of documents per batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--max-docs", type=int,
                       help="Maximum documents to discover (for testing)")
    
    # Output options
    parser.add_argument("--output", type=str, default="discovery.json",
                       help="Output JSON file. Date auto-appended if not present (e.g., discovery.json → discovery_12_05_2025.json)")
    
    # Summary and resume options
    parser.add_argument("--show-summary", action="store_true",
                       help="Display detailed summary of an existing discovery file (does not run discovery)")
    parser.add_argument("--resume", action="store_true",
                       help="Resume from previous discovery")
    
    return parser


def main() -> None:
    """Main entry point for document discovery script."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Auto-append date to output filename if not already present
    from datetime import datetime
    import re
    
    # Check if output filename already has a date pattern (YYYY-MM-DD or similar)
    date_pattern = r'\d{4}[-_]\d{2}[-_]\d{2}|\d{2}[-_]\d{2}[-_]\d{4}'
    if not re.search(date_pattern, args.output):
        # Add today's date before .json extension
        today = datetime.now().strftime("%m_%d_%Y")
        if args.output.endswith('.json'):
            args.output = args.output[:-5] + f"_{today}.json"
        else:
            args.output = args.output + f"_{today}.json"
    
    # Run discovery
    discovery = DocumentDiscovery()
    discovery.run(args)


if __name__ == "__main__":
    main() 