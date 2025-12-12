#!/usr/bin/env python3
"""
FIXED: Populate Missing Vendor IDs in Pinecone SF-Files Namespace

This is the corrected version that fixes the infinite loop issue in the original script.
Uses proper Pinecone pagination to process each document exactly once.

Key Fixes:
1. Uses Pinecone list() method for systematic pagination
2. Processes each document exactly once (no duplicates)
3. Proper completion detection
4. Accurate progress tracking

Usage:
    python populate_vendor_ids_fixed.py --namespace SF-Files-2020-8-15-25
    python populate_vendor_ids_fixed.py --namespace SF-Files-2020-8-15-25 --dry-run
"""

import sys
import os
import csv
import argparse
from typing import Dict, List, Set
from collections import defaultdict
from datetime import datetime
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from dotenv import load_dotenv
load_dotenv()

from connectors.pinecone_client import PineconeDocumentClient
from config.settings import Settings
from config.colored_logging import setup_colored_logging, ColoredLogger

class VendorIDPopulatorFixed:
    """Fixed version of vendor ID populator with proper pagination"""
    
    def __init__(self, namespace: str, dry_run: bool = False):
        setup_colored_logging()
        self.logger = ColoredLogger("VendorIDPopulatorFixed")
        
        self.namespace = namespace
        self.dry_run = dry_run
        
        # Initialize Pinecone client
        settings = Settings()
        self.pinecone_client = PineconeDocumentClient(
            settings.PINECONE_API_KEY, 
            settings.PINECONE_INDEX_NAME
        )
        
        # Data storage
        self.deal_to_vendor_mapping = {}
        self.vendor_name_mapping = {}
        
        self.logger.info(f"✅ 🔧 VendorIDPopulatorFixed initialized for namespace: {namespace}")
        if dry_run:
            self.logger.info("🔍 DRY RUN MODE - No actual updates will be made")
    
    def _load_deal_metadata(self):
        """Load deal metadata and vendor mappings"""
        try:
            # Load Deal metadata from the organized files mapping (check local first)
            local_deal_csv = "organized_files_to_deal_mapping_enhanced.csv"
            external_deal_csv = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/organized_files_to_deal_mapping_enhanced.csv"
            
            if os.path.exists(local_deal_csv):
                deal_csv = local_deal_csv
            elif os.path.exists(external_deal_csv):
                deal_csv = external_deal_csv
            else:
                # Fallback to regular mapping
                deal_csv = "organized_files_to_deal_mapping.csv"
            
            self.logger.info(f"ℹ️  📋 Loading deal metadata from {deal_csv}")
            
            # Load the deal mapping data (for file-to-deal relationships)
            df_mapping = pd.read_csv(deal_csv)
            
            # Load Deal metadata CSV with vendor information
            deal_metadata_csv = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/Deal__c.csv"
            self.logger.info(f"ℹ️  📋 Loading Deal metadata from {deal_metadata_csv}")
            
            try:
                df_deals = pd.read_csv(deal_metadata_csv, encoding='utf-8-sig', on_bad_lines='skip')
            except UnicodeDecodeError:
                try:
                    df_deals = pd.read_csv(deal_metadata_csv, encoding='latin1', on_bad_lines='skip')
                except:
                    df_deals = pd.read_csv(deal_metadata_csv, encoding='cp1252', on_bad_lines='skip')
            
            # Create mapping from deal_id to vendor information
            for _, row in df_deals.iterrows():
                deal_id = row.get('Id', '')
                if deal_id and str(deal_id) != 'nan':
                    vendor_id = row.get('Primary_Deal_Vendor__c', '')
                    self.deal_to_vendor_mapping[deal_id] = {
                        'vendor_id': vendor_id,
                        'deal_name': row.get('Name', ''),
                        'deal_subject': row.get('Subject__c', ''),
                        'client_name': row.get('Client_Name__c', '')
                    }
            
            self.logger.info(f"✅ Loaded {len(self.deal_to_vendor_mapping):,} deal-to-vendor mappings")
            
            # Load vendor name mappings
            vendor_csv = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Vendor_mapping.csv"
            self.logger.info(f"ℹ️  📋 Loading vendor names from {vendor_csv}")
            
            df_vendors = pd.read_csv(vendor_csv, encoding='utf-8-sig')
            
            for _, row in df_vendors.iterrows():
                vendor_id = row.get('Account ID', '')
                vendor_name = row.get('Account Name', '')
                if vendor_id and vendor_name:
                    self.vendor_name_mapping[str(vendor_id)] = str(vendor_name)
            
            self.logger.info(f"✅ Loaded {len(self.vendor_name_mapping):,} vendor name mappings")
            
        except Exception as e:
            self.logger.error(f"❌ ❌ Failed to load deal metadata: {e}")
            raise
    
    def analyze_enhancement_potential(self, sample_size: int = 1000) -> Dict:
        """Analyze how many documents need vendor enhancement"""
        try:
            self.logger.info(f"🔍 Analyzing enhancement potential (sample: {sample_size})")
            
            # Use query method for analysis (simpler approach)
            results = self.pinecone_client.index.query(
                namespace=self.namespace,
                vector=[0.1] * 1024,  # Use slightly different vector to avoid cache
                top_k=sample_size,
                include_metadata=True
            )
            
            if not results.matches:
                return {"error": "No documents found in namespace"}
            
            # Process matches directly
            vector_data = {match.id: match for match in results.matches}
            
            stats = {
                'total_sampled': len(results.matches),
                'missing_vendor_id': 0,
                'has_deal_id': 0,
                'mappable_via_deal': 0,
                'already_has_vendor_id': 0
            }
            
            for match in results.matches:
                metadata = match.metadata or {}
                
                vendor_id = metadata.get('vendor_id', '')
                deal_id = metadata.get('deal_id', '')
                
                if deal_id and str(deal_id).strip():
                    stats['has_deal_id'] += 1
                
                # Check if vendor_id is missing (empty, None, or "None" string)
                vendor_id_clean = str(vendor_id).strip() if vendor_id else ''
                if not vendor_id_clean or vendor_id_clean.lower() in ['none', 'nan']:
                    stats['missing_vendor_id'] += 1
                    
                    # Check if we can map via deal
                    if deal_id and deal_id in self.deal_to_vendor_mapping:
                        deal_data = self.deal_to_vendor_mapping[deal_id]
                        raw_vendor_id = deal_data.get('vendor_id', '')
                        if not pd.isna(raw_vendor_id) and str(raw_vendor_id).strip():
                            stats['mappable_via_deal'] += 1
                else:
                    stats['already_has_vendor_id'] += 1
            
            # Calculate percentages
            for key in ['missing_vendor_id', 'has_deal_id', 'mappable_via_deal', 'already_has_vendor_id']:
                percentage = (stats[key] / stats['total_sampled']) * 100
                self.logger.info(f"  {key}: {stats[key]}/{stats['total_sampled']} ({percentage:.1f}%)")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ Analysis failed: {e}")
            return {"error": str(e)}
    
    def populate_vendor_ids_fixed(self, batch_size: int = 100):
        """Fixed version using Pinecone's proper list() method with implicit pagination"""
        import time
        from datetime import datetime, timedelta
        
        try:
            self.logger.info(f"🚀 Starting FIXED vendor ID population")
            self.logger.info(f"  Namespace: {self.namespace}")
            self.logger.info(f"  Batch size: {batch_size}")
            self.logger.info(f"  Dry run: {self.dry_run}")
            
            # Load metadata
            self._load_deal_metadata()
            
            # Progress tracking
            start_time = time.time()
            estimated_total_docs = 460000  # Based on our analysis
            
            # Statistics
            total_stats = {
                'documents_processed': 0,
                'vendor_ids_added': 0,
                'vendor_names_added': 0,
                'already_populated': 0,
                'no_deal_mapping': 0,
                'errors': 0
            }
            
            # Use Pinecone's proper list() method with implicit pagination
            batch_num = 0
            last_progress_time = start_time
            
            self.logger.info(f"📊 Estimated total documents: {estimated_total_docs:,}")
            self.logger.info(f"⏱️  Started at: {datetime.now().strftime('%H:%M:%S')}")
            
            # This automatically handles pagination and processes ALL vectors exactly once
            for vector_ids in self.pinecone_client.index.list(namespace=self.namespace, limit=batch_size):
                batch_num += 1
                batch_start_time = time.time()
                
                try:
                    # Ensure vector_ids is a list
                    if isinstance(vector_ids, str):
                        vector_ids = [vector_ids]
                    elif not isinstance(vector_ids, list):
                        vector_ids = list(vector_ids)
                    
                    if not vector_ids:
                        self.logger.info("✅ No more documents to process")
                        break
                    
                    # Fetch metadata for this batch of IDs
                    try:
                        fetch_result = self.pinecone_client.index.fetch(
                            ids=vector_ids,
                            namespace=self.namespace
                        )
                        
                        if not fetch_result.vectors:
                            self.logger.info("✅ No more documents to process")
                            break
                    except Exception as fetch_error:
                        self.logger.error(f"❌ Error fetching batch {batch_num}: {fetch_error}")
                        continue
                    
                    # Convert to list of matches for processing
                    matches = []
                    for vector_id, vector_data in fetch_result.vectors.items():
                        # Create a match-like object for compatibility
                        class MockMatch:
                            def __init__(self, id, metadata):
                                self.id = id
                                self.metadata = metadata
                        
                        matches.append(MockMatch(vector_id, vector_data.metadata))
                    
                    # Process this batch
                    batch_stats = self._process_vendor_batch_fixed(matches)
                    
                    # Update totals
                    for key in total_stats:
                        total_stats[key] += batch_stats[key]
                    
                    # Calculate progress metrics
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    batch_time = current_time - batch_start_time
                    
                    # Progress calculations
                    docs_processed = total_stats['documents_processed']
                    progress_percentage = (docs_processed / estimated_total_docs) * 100
                    
                    # Rate calculations
                    docs_per_second = docs_processed / elapsed_time if elapsed_time > 0 else 0
                    docs_per_minute = docs_per_second * 60
                    
                    # ETA calculation
                    remaining_docs = estimated_total_docs - docs_processed
                    eta_seconds = remaining_docs / docs_per_second if docs_per_second > 0 else 0
                    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                    
                    # Success rate
                    success_rate = (total_stats['vendor_ids_added'] / docs_processed * 100) if docs_processed > 0 else 0
                    
                    # Enhanced progress display (every 10 batches or every 30 seconds)
                    if batch_num % 10 == 0 or (current_time - last_progress_time) >= 30:
                        self.logger.info("📈 " + "="*80)
                        self.logger.info(f"📈 BATCH {batch_num:,} COMPLETE | {datetime.now().strftime('%H:%M:%S')}")
                        self.logger.info(f"📈 Progress: {docs_processed:,}/{estimated_total_docs:,} ({progress_percentage:.1f}%)")
                        self.logger.info(f"📈 Rate: {docs_per_minute:.0f} docs/min | Batch time: {batch_time:.1f}s")
                        self.logger.info(f"📈 Vendor IDs added: {total_stats['vendor_ids_added']:,} (Success: {success_rate:.1f}%)")
                        self.logger.info(f"📈 ETA: {eta_time.strftime('%H:%M:%S')} ({eta_seconds/3600:.1f}h remaining)")
                        self.logger.info("📈 " + "="*80)
                        last_progress_time = current_time
                    else:
                        # Quick progress update
                        self.logger.progress(f"📊 Batch {batch_num:,} | "
                                           f"{docs_processed:,} docs ({progress_percentage:.1f}%) | "
                                           f"Rate: {docs_per_minute:.0f}/min | "
                                           f"Added: {total_stats['vendor_ids_added']:,}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Batch {batch_num} failed: {e}")
                    total_stats['errors'] += 1
                    continue
            
            # Final summary with timing
            end_time = time.time()
            total_elapsed = end_time - start_time
            
            self.logger.info("✅ 🎉 VENDOR ID POPULATION COMPLETE")
            self.logger.info("✅ " + "=" * 80)
            self.logger.info(f"✅ Completed at: {datetime.now().strftime('%H:%M:%S on %Y-%m-%d')}")
            self.logger.info(f"✅ Total runtime: {total_elapsed/3600:.1f} hours ({total_elapsed/60:.1f} minutes)")
            
            # Performance metrics
            final_docs_processed = total_stats['documents_processed']
            if final_docs_processed > 0 and total_elapsed > 0:
                final_rate = final_docs_processed / total_elapsed
                self.logger.info(f"✅ Average rate: {final_rate*60:.0f} docs/minute ({final_rate:.1f} docs/second)")
            
            self.logger.info("✅ " + "-" * 80)
            
            # Detailed statistics
            for key, value in total_stats.items():
                percentage = ""
                if key in ['vendor_ids_added', 'vendor_names_added', 'already_populated'] and final_docs_processed > 0:
                    pct = (value / final_docs_processed) * 100
                    percentage = f" ({pct:.1f}%)"
                self.logger.info(f"✅ {key.replace('_', ' ').title()}: {value:,}{percentage}")
            
            # Coverage improvement estimate
            if total_stats['vendor_ids_added'] > 0:
                estimated_new_coverage = ((total_stats['vendor_ids_added'] + (estimated_total_docs * 0.287)) / estimated_total_docs) * 100
                self.logger.info("✅ " + "-" * 80)
                self.logger.info(f"✅ Estimated coverage improvement: 28.7% → {estimated_new_coverage:.1f}%")
                self.logger.info(f"✅ Estimated total enhanced documents: {total_stats['vendor_ids_added']:,}")
            
            self.logger.info("✅ " + "=" * 80)
            
        except Exception as e:
            self.logger.error(f"❌ ❌ Error during vendor ID population: {e}")
            raise
    
    def _process_vendor_batch_fixed(self, matches: List) -> Dict:
        """Process a batch of matches for vendor enhancement"""
        batch_stats = {
            'documents_processed': 0,
            'vendor_ids_added': 0,
            'vendor_names_added': 0,
            'already_populated': 0,
            'no_deal_mapping': 0,
            'errors': 0
        }
        
        for match in matches:
            try:
                metadata = match.metadata or {}
                batch_stats['documents_processed'] += 1
                
                # Check if vendor_id is missing but deal_id exists
                vendor_id = metadata.get('vendor_id', '')
                deal_id = metadata.get('deal_id', '')
                
                # Check if vendor_id is properly populated (not empty, None, or "None" string)
                vendor_id_clean = str(vendor_id).strip() if vendor_id else ''
                if vendor_id_clean and vendor_id_clean.lower() not in ['none', 'nan', '']:
                    batch_stats['already_populated'] += 1
                    continue
                
                if not deal_id or not str(deal_id).strip():
                    batch_stats['no_deal_mapping'] += 1
                    continue
                
                if deal_id not in self.deal_to_vendor_mapping:
                    batch_stats['no_deal_mapping'] += 1
                    continue
                
                # Get vendor info from deal mapping
                deal_data = self.deal_to_vendor_mapping[deal_id]
                raw_vendor_id = deal_data.get('vendor_id', '')
                
                # Handle different data types (string, float, NaN)
                if pd.isna(raw_vendor_id) or raw_vendor_id == '':
                    new_vendor_id = ''
                else:
                    new_vendor_id = str(raw_vendor_id).strip()
                
                if not new_vendor_id:
                    batch_stats['no_deal_mapping'] += 1
                    continue
                
                # Prepare updates
                updates = {'vendor_id': new_vendor_id}
                batch_stats['vendor_ids_added'] += 1
                
                # Also add vendor_name if available
                vendor_name = self.vendor_name_mapping.get(new_vendor_id)
                if vendor_name:
                    updates['vendor_name'] = vendor_name
                    batch_stats['vendor_names_added'] += 1
                
                # Apply updates
                if not self.dry_run:
                    self.pinecone_client.index.update(
                        id=match.id,
                        set_metadata=updates,
                        namespace=self.namespace
                    )
                
            except Exception as e:
                self.logger.error(f"❌ Error processing {match.id}: {e}")
                batch_stats['errors'] += 1
                continue
        
        return batch_stats

def main():
    parser = argparse.ArgumentParser(description="Fixed Vendor ID Population for Pinecone")
    parser.add_argument("--namespace", required=True, help="Pinecone namespace to process")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--dry-run", action="store_true", help="Run analysis only, no updates")
    parser.add_argument("--analyze-only", action="store_true", help="Only run analysis")
    
    args = parser.parse_args()
    
    try:
        populator = VendorIDPopulatorFixed(args.namespace, args.dry_run)
        
        if args.analyze_only:
            populator._load_deal_metadata()
            stats = populator.analyze_enhancement_potential(1000)
            print(f"\n📊 Analysis complete: {stats}")
        else:
            populator.populate_vendor_ids_fixed(args.batch_size)
            
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
