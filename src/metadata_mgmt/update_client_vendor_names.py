#!/usr/bin/env python3
"""
Pinecone Metadata Enhancement Script

Safely updates existing Pinecone vectors with client and vendor names
using Salesforce mapping CSV files. Designed to run concurrently with
ongoing upsert operations using Pinecone's thread-safe update() method.

Usage:
    python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25 --batch-size 100
    python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25 --dry-run
"""

import sys
import os
import csv
import time
import argparse
from typing import Dict, List, Optional, Set
from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables
load_dotenv()

from connectors.pinecone_client import PineconeDocumentClient
from config.settings import Settings
from config.colored_logging import setup_colored_logging, ColoredLogger

class MetadataEnhancer:
    """Safely enhance Pinecone metadata with client/vendor names from CSV mappings"""
    
    def __init__(self, namespace: str, batch_size: int = 100, dry_run: bool = False):
        self.namespace = namespace
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        # Initialize logging
        setup_colored_logging()
        self.logger = ColoredLogger("metadata_enhancer")
        
        # Initialize Pinecone client
        settings = Settings()
        self.pinecone_client = PineconeDocumentClient(
            api_key=settings.PINECONE_API_KEY,
            index_name=settings.PINECONE_INDEX_NAME
        )
        
        # Load CSV mappings
        self.client_mapping = self._load_client_mapping()
        self.vendor_mapping = self._load_vendor_mapping()
        
        # Statistics tracking
        self.stats = {
            'total_processed': 0,
            'client_names_added': 0,
            'vendor_names_added': 0,
            'no_mapping_found': 0,
            'errors': 0,
            'batches_completed': 0
        }
        
        self.logger.success(f"🚀 MetadataEnhancer initialized for namespace: {namespace}")
        self.logger.info(f"📊 Loaded {len(self.client_mapping)} client mappings")
        self.logger.info(f"📊 Loaded {len(self.vendor_mapping)} vendor mappings")
        
    def _load_client_mapping(self) -> Dict[str, str]:
        """Load client ID to name mapping from CSV"""
        mapping = {}
        csv_path = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Cust-Mapping.csv"
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:  # Handle BOM
                reader = csv.DictReader(f)
                for row in reader:
                    # Use both Account ID formats
                    account_id = row['Account ID'].strip()
                    account_id_18 = row['18 Digit ID'].strip()
                    account_name = row['Account Name'].strip()
                    
                    if account_id and account_name:
                        mapping[account_id] = account_name
                    if account_id_18 and account_name:
                        mapping[account_id_18] = account_name
                        
            self.logger.success(f"✅ Loaded {len(mapping)} client mappings from {csv_path}")
            return mapping
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load client mapping: {e}")
            return {}
    
    def _load_vendor_mapping(self) -> Dict[str, str]:
        """Load vendor ID to name mapping from CSV"""
        mapping = {}
        csv_path = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Vendor_mapping.csv"
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:  # Handle BOM
                reader = csv.DictReader(f)
                for row in reader:
                    # Use both Account ID formats
                    account_id = row['Account ID'].strip()
                    account_id_18 = row['18 Digit ID'].strip()
                    account_name = row['Account Name'].strip()
                    
                    if account_id and account_name:
                        mapping[account_id] = account_name
                    if account_id_18 and account_name:
                        mapping[account_id_18] = account_name
                        
            self.logger.success(f"✅ Loaded {len(mapping)} vendor mappings from {csv_path}")
            return mapping
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load vendor mapping: {e}")
            return {}
    
    def _get_sample_documents(self, limit: int = 1000) -> List[Dict]:
        """Get sample documents to analyze ID patterns and estimate scope"""
        try:
            self.logger.info(f"🔍 Sampling {limit} documents from {self.namespace} namespace...")
            
            # Use zero vector query to get random sample
            results = self.pinecone_client.index.query(
                namespace=self.namespace,
                vector=[0.0] * 1024,
                top_k=limit,
                include_metadata=True
            )
            
            sample_docs = []
            for match in results.matches:
                metadata = match.metadata or {}
                sample_docs.append({
                    'id': match.id,
                    'metadata': metadata
                })
                
            self.logger.success(f"✅ Retrieved {len(sample_docs)} sample documents")
            return sample_docs
            
        except Exception as e:
            self.logger.error(f"❌ Error sampling documents: {e}")
            return []
    
    def _analyze_enhancement_potential(self, sample_docs: List[Dict]) -> Dict:
        """Analyze what percentage of documents can be enhanced"""
        analysis = {
            'total_sampled': len(sample_docs),
            'has_client_id': 0,
            'has_vendor_id': 0,
            'client_mappable': 0,
            'vendor_mappable': 0,
            'already_has_client_name': 0,
            'already_has_vendor_name': 0
        }
        
        for doc in sample_docs:
            metadata = doc['metadata']
            
            # Check for client ID and mapping potential
            client_id = metadata.get('client_id')
            if client_id:
                analysis['has_client_id'] += 1
                if client_id in self.client_mapping:
                    analysis['client_mappable'] += 1
            
            # Check for vendor ID and mapping potential  
            vendor_id = metadata.get('vendor_id')
            if vendor_id:
                analysis['has_vendor_id'] += 1
                if vendor_id in self.vendor_mapping:
                    analysis['vendor_mappable'] += 1
            
            # Check if names already exist
            if metadata.get('client_name'):
                analysis['already_has_client_name'] += 1
            if metadata.get('vendor_name'):
                analysis['already_has_vendor_name'] += 1
        
        return analysis
    
    def _print_analysis_report(self, analysis: Dict):
        """Print enhancement potential analysis"""
        total = analysis['total_sampled']
        
        self.logger.info("📊 ENHANCEMENT POTENTIAL ANALYSIS")
        self.logger.info("=" * 50)
        self.logger.info(f"📋 Total Documents Sampled: {total:,}")
        self.logger.info("")
        
        # Client analysis
        client_pct = (analysis['has_client_id'] / total * 100) if total > 0 else 0
        client_mappable_pct = (analysis['client_mappable'] / total * 100) if total > 0 else 0
        client_existing_pct = (analysis['already_has_client_name'] / total * 100) if total > 0 else 0
        
        self.logger.info(f"👥 CLIENT ENHANCEMENT:")
        self.logger.info(f"   Documents with client_id: {analysis['has_client_id']:,} ({client_pct:.1f}%)")
        self.logger.info(f"   Mappable to client names: {analysis['client_mappable']:,} ({client_mappable_pct:.1f}%)")
        self.logger.info(f"   Already have client_name: {analysis['already_has_client_name']:,} ({client_existing_pct:.1f}%)")
        self.logger.info("")
        
        # Vendor analysis
        vendor_pct = (analysis['has_vendor_id'] / total * 100) if total > 0 else 0
        vendor_mappable_pct = (analysis['vendor_mappable'] / total * 100) if total > 0 else 0
        vendor_existing_pct = (analysis['already_has_vendor_name'] / total * 100) if total > 0 else 0
        
        self.logger.info(f"🏢 VENDOR ENHANCEMENT:")
        self.logger.info(f"   Documents with vendor_id: {analysis['has_vendor_id']:,} ({vendor_pct:.1f}%)")
        self.logger.info(f"   Mappable to vendor names: {analysis['vendor_mappable']:,} ({vendor_mappable_pct:.1f}%)")
        self.logger.info(f"   Already have vendor_name: {analysis['already_has_vendor_name']:,} ({vendor_existing_pct:.1f}%)")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8)
    )
    def _update_document_metadata(self, doc_id: str, metadata_updates: Dict) -> bool:
        """Safely update a single document's metadata with retry logic"""
        if self.dry_run:
            self.logger.info(f"🔍 DRY RUN: Would update {doc_id} with {metadata_updates}")
            return True
            
        try:
            # Use Pinecone's update method for metadata-only changes
            self.pinecone_client.index.update(
                id=doc_id,
                set_metadata=metadata_updates,
                namespace=self.namespace
            )
            return True
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to update {doc_id}: {e}")
            raise
    
    def _process_document_batch(self, docs: List[Dict]) -> Dict:
        """Process a batch of documents for metadata enhancement"""
        batch_stats = {
            'processed': 0,
            'client_names_added': 0,
            'vendor_names_added': 0,
            'no_updates_needed': 0,
            'errors': 0
        }
        
        for doc in docs:
            doc_id = doc['id']
            metadata = doc['metadata']
            
            # Prepare metadata updates
            updates = {}
            
            # Add client name if client_id exists and name not already present
            client_id = metadata.get('client_id')
            if client_id and not metadata.get('client_name'):
                client_name = self.client_mapping.get(client_id)
                if client_name:
                    updates['client_name'] = client_name
                    batch_stats['client_names_added'] += 1
            
            # Add vendor name if vendor_id exists and name not already present
            vendor_id = metadata.get('vendor_id')
            if vendor_id and not metadata.get('vendor_name'):
                vendor_name = self.vendor_mapping.get(vendor_id)
                if vendor_name:
                    updates['vendor_name'] = vendor_name
                    batch_stats['vendor_names_added'] += 1
            
            # Update document if we have enhancements
            if updates:
                try:
                    success = self._update_document_metadata(doc_id, updates)
                    if success:
                        batch_stats['processed'] += 1
                        if not self.dry_run:
                            self.logger.progress(f"✅ Enhanced {doc_id[:50]}... with {list(updates.keys())}")
                except Exception as e:
                    batch_stats['errors'] += 1
                    self.logger.error(f"❌ Failed to update {doc_id}: {e}")
            else:
                batch_stats['no_updates_needed'] += 1
        
        return batch_stats
    
    def _save_progress(self, processed_ids: Set[str], batch_num: int):
        """Save progress to resume file"""
        progress_file = f"metadata_enhancement_progress_{self.namespace.replace('-', '_')}.json"
        
        progress_data = {
            'namespace': self.namespace,
            'last_batch': batch_num,
            'processed_count': len(processed_ids),
            'processed_ids': list(processed_ids),
            'timestamp': datetime.now().isoformat(),
            'stats': self.stats
        }
        
        with open(progress_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
    
    def _load_progress(self) -> Set[str]:
        """Load progress from previous run"""
        progress_file = f"metadata_enhancement_progress_{self.namespace.replace('-', '_')}.json"
        
        if not Path(progress_file).exists():
            return set()
        
        try:
            with open(progress_file, 'r') as f:
                progress_data = json.load(f)
            
            processed_ids = set(progress_data.get('processed_ids', []))
            self.stats.update(progress_data.get('stats', {}))
            
            self.logger.info(f"📂 Resuming from previous run: {len(processed_ids):,} documents already processed")
            return processed_ids
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load progress file: {e}")
            return set()
    
    def enhance_metadata(self, limit: Optional[int] = None):
        """Main method to enhance metadata across the namespace"""
        start_time = datetime.now()
        processed_ids = self._load_progress()
        
        self.logger.info(f"🚀 Starting metadata enhancement for namespace: {self.namespace}")
        if self.dry_run:
            self.logger.warning("🔍 DRY RUN MODE - No actual updates will be made")
        
        # First, analyze enhancement potential with sample
        self.logger.info("🔍 Analyzing enhancement potential...")
        sample_docs = self._get_sample_documents(1000)
        if not sample_docs:
            self.logger.error("❌ Could not retrieve sample documents")
            return
        
        analysis = self._analyze_enhancement_potential(sample_docs)
        self._print_analysis_report(analysis)
        
        # Ask for confirmation if not dry run
        if not self.dry_run:
            estimated_updates = analysis['client_mappable'] + analysis['vendor_mappable']
            if estimated_updates == 0:
                self.logger.warning("⚠️ No documents appear to need enhancement based on sample")
                return
                
            response = input(f"\n🤔 Proceed with enhancement? Estimated {estimated_updates:,} updates from sample. (y/N): ")
            if response.lower() != 'y':
                self.logger.info("👋 Enhancement cancelled by user")
                return
        
        # Get all document IDs using list() method for serverless indexes
        self.logger.info("📋 Retrieving all document IDs from namespace...")
        try:
            all_doc_ids = []
            for ids_batch in self.pinecone_client.index.list(namespace=self.namespace):
                all_doc_ids.extend(ids_batch)
                if limit and len(all_doc_ids) >= limit:
                    all_doc_ids = all_doc_ids[:limit]
                    break
            
            # Filter out already processed documents
            remaining_ids = [doc_id for doc_id in all_doc_ids if doc_id not in processed_ids]
            
            self.logger.success(f"✅ Found {len(all_doc_ids):,} total documents")
            self.logger.info(f"📋 {len(remaining_ids):,} documents remaining to process")
            
        except Exception as e:
            self.logger.error(f"❌ Error retrieving document IDs: {e}")
            return
        
        if not remaining_ids:
            self.logger.success("🎉 All documents already processed!")
            return
        
        # Process documents in batches
        total_batches = (len(remaining_ids) + self.batch_size - 1) // self.batch_size
        self.logger.info(f"🔄 Processing {len(remaining_ids):,} documents in {total_batches} batches of {self.batch_size}")
        
        for batch_num in range(0, len(remaining_ids), self.batch_size):
            batch_ids = remaining_ids[batch_num:batch_num + self.batch_size]
            current_batch = (batch_num // self.batch_size) + 1
            
            self.logger.info(f"📦 Processing batch {current_batch}/{total_batches} ({len(batch_ids)} documents)")
            
            # Fetch metadata for batch
            try:
                fetch_response = self.pinecone_client.index.fetch(
                    ids=batch_ids,
                    namespace=self.namespace
                )
                
                # Convert to format expected by process_document_batch
                batch_docs = []
                vectors = fetch_response.vectors if hasattr(fetch_response, 'vectors') else fetch_response.get('vectors', {})
                for doc_id, vector_data in vectors.items():
                    metadata = vector_data.metadata if hasattr(vector_data, 'metadata') else vector_data.get('metadata', {})
                    batch_docs.append({
                        'id': doc_id,
                        'metadata': metadata
                    })
                
                # Process the batch
                batch_stats = self._process_document_batch(batch_docs)
                
                # Update overall stats
                self.stats['total_processed'] += batch_stats['processed']
                self.stats['client_names_added'] += batch_stats['client_names_added']
                self.stats['vendor_names_added'] += batch_stats['vendor_names_added']
                self.stats['errors'] += batch_stats['errors']
                self.stats['batches_completed'] += 1
                
                # Add processed IDs to tracking set
                processed_ids.update(batch_ids)
                
                # Progress reporting
                progress_pct = (current_batch / total_batches) * 100
                elapsed = datetime.now() - start_time
                rate = self.stats['total_processed'] / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                
                self.logger.progress(f"📈 Batch {current_batch}/{total_batches} ({progress_pct:.1f}%) | "
                                   f"Rate: {rate:.1f} docs/sec | "
                                   f"Enhanced: {batch_stats['client_names_added'] + batch_stats['vendor_names_added']}")
                
                # Save progress every 10 batches
                if current_batch % 10 == 0:
                    self._save_progress(processed_ids, current_batch)
                
                # Rate limiting - be respectful of concurrent upserts
                time.sleep(0.5)  # 500ms delay between batches
                
            except Exception as e:
                self.logger.error(f"❌ Error processing batch {current_batch}: {e}")
                self.stats['errors'] += len(batch_ids)
                continue
        
        # Final progress save
        self._save_progress(processed_ids, total_batches)
        
        # Final report
        self._print_final_report(start_time)
    
    def _print_final_report(self, start_time: datetime):
        """Print comprehensive final report"""
        elapsed = datetime.now() - start_time
        
        self.logger.success("🎉 METADATA ENHANCEMENT COMPLETE")
        self.logger.success("=" * 50)
        self.logger.success(f"⏱️ Total Time: {elapsed}")
        self.logger.success(f"📊 Documents Processed: {self.stats['total_processed']:,}")
        self.logger.success(f"👥 Client Names Added: {self.stats['client_names_added']:,}")
        self.logger.success(f"🏢 Vendor Names Added: {self.stats['vendor_names_added']:,}")
        self.logger.success(f"📦 Batches Completed: {self.stats['batches_completed']:,}")
        
        if self.stats['errors'] > 0:
            self.logger.warning(f"⚠️ Errors Encountered: {self.stats['errors']:,}")
        
        total_enhancements = self.stats['client_names_added'] + self.stats['vendor_names_added']
        rate = self.stats['total_processed'] / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        self.logger.success(f"✨ Total Enhancements: {total_enhancements:,}")
        self.logger.success(f"🚀 Processing Rate: {rate:.1f} documents/second")

def main():
    parser = argparse.ArgumentParser(description="Enhance Pinecone metadata with client/vendor names")
    parser.add_argument("--namespace", required=True, help="Pinecone namespace to enhance")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    parser.add_argument("--limit", type=int, help="Limit number of documents to process (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Analyze potential without making changes")
    
    args = parser.parse_args()
    
    enhancer = MetadataEnhancer(
        namespace=args.namespace,
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )
    
    enhancer.enhance_metadata(limit=args.limit)

if __name__ == "__main__":
    main()
