#!/usr/bin/env python3
"""
Pinecone Metadata Coverage Analyzer (Consolidated)

Unified script for analyzing metadata coverage in Pinecone namespaces with multiple modes:
- Comprehensive field analysis (all fields, client/vendor)
- Focused analysis (vendor-only or client-only)
- Log/JSON-based analysis (documents from processing logs)
- Quick vendor status check

Usage:
    # Comprehensive analysis (all fields)
    python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25
    
    # Quick vendor check
    python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --focus vendor --quick
    
    # Full namespace scan
    python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --full-scan --csv output.csv
    
    # Documents from logs/JSON
    python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --from-log logs/processing/*.log
    python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --from-json discovery.json
"""

import sys
import os
import csv
import argparse
import json
import re
import random
from typing import Dict, List, Optional, Tuple, Any
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from connectors.pinecone_client import PineconeDocumentClient
from config.settings import Settings
from config.colored_logging import setup_colored_logging, ColoredLogger

# Field list for log/JSON-based analysis
FIELDS: List[str] = [
    'document_path','file_name','file_type','file_size_mb','modified_time',
    'year','week_number','week_date','vendor','client','deal_number','deal_name',
    'deal_id','deal_subject','deal_status','deal_reason','deal_start_date','negotiated_by',
    'proposed_amount','final_amount','savings_1yr','savings_3yr','savings_target','savings_percentage',
    'savings_achieved','fixed_savings','savings_target_full_term','final_amount_full_term',
    'client_id','client_name','vendor_id','vendor_name',
    'contract_term','contract_start','contract_end','effort_level','has_fmv_report','deal_origin',
    'current_narrative','customer_comments','content_source',
    'document_type','document_type_confidence','classification_reasoning','classification_method','classification_tokens_used',
    'product_pricing_depth','commercial_terms_depth','proposed_term_start','proposed_term_end',
    'email_sender','email_recipients_to','email_subject','email_date','email_has_attachments','email_body_preview',
    'chunk_index','parser','extraction_confidence','has_parsing_errors','processing_method','namespace',
]

class MetadataCoverageAnalyzer:
    """Unified metadata coverage analyzer with multiple analysis modes"""
    
    def __init__(self, namespace: str):
        self.namespace = namespace
        
        # Initialize logging
        setup_colored_logging()
        self.logger = ColoredLogger("metadata_analyzer")
        
        # Initialize Pinecone client
        settings = Settings()
        self.pinecone_client = PineconeDocumentClient(
            api_key=settings.PINECONE_API_KEY,
            index_name=settings.PINECONE_INDEX_NAME
        )
        
        self.logger.success(f"🔍 MetadataCoverageAnalyzer initialized for namespace: {namespace}")
    
    def _load_mapping_coverage(self) -> Dict:
        """Load and analyze mapping file coverage"""
        mappings = {
            'clients': {},
            'vendors': {}
        }
        
        # Load client mapping
        try:
            client_csv = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Cust-Mapping.csv"
            with open(client_csv, 'r', encoding='utf-8-sig') as f:  # Handle BOM
                reader = csv.DictReader(f)
                for row in reader:
                    account_id = row['Account ID'].strip()
                    account_id_18 = row['18 Digit ID'].strip()
                    account_name = row['Account Name'].strip()
                    
                    if account_id and account_name:
                        mappings['clients'][account_id] = account_name
                    if account_id_18 and account_name:
                        mappings['clients'][account_id_18] = account_name
            
            self.logger.success(f"✅ Loaded {len(mappings['clients'])} client mappings")
        except Exception as e:
            self.logger.error(f"❌ Failed to load client mappings: {e}")
        
        # Load vendor mapping
        try:
            vendor_csv = "/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Vendor_mapping.csv"
            with open(vendor_csv, 'r', encoding='utf-8-sig') as f:  # Handle BOM
                reader = csv.DictReader(f)
                for row in reader:
                    account_id = row['Account ID'].strip()
                    account_id_18 = row['18 Digit ID'].strip()
                    account_name = row['Account Name'].strip()
                    
                    if account_id and account_name:
                        mappings['vendors'][account_id] = account_name
                    if account_id_18 and account_name:
                        mappings['vendors'][account_id_18] = account_name
            
            self.logger.success(f"✅ Loaded {len(mappings['vendors'])} vendor mappings")
        except Exception as e:
            self.logger.error(f"❌ Failed to load vendor mappings: {e}")
        
        return mappings
    
    def _get_enumeration_cache_path(self) -> str:
        """Get the path for the enumeration cache file"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        # Include namespace in filename for multi-namespace support
        safe_namespace = self.namespace.replace('/', '_').replace('-', '_')
        return os.path.join(cache_dir, f'enumeration_{safe_namespace}.json')
    
    def _get_progress_cache_path(self) -> str:
        """Get the path for the progress/checkpoint cache file"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        safe_namespace = self.namespace.replace('/', '_').replace('-', '_')
        return os.path.join(cache_dir, f'progress_{safe_namespace}.json')
    
    def _save_progress_checkpoint(self, completed_batches: set, failed_batches: list, total_batches: int) -> None:
        """Save progress checkpoint for resume capability"""
        cache_path = self._get_progress_cache_path()
        try:
            checkpoint_data = {
                'namespace': self.namespace,
                'last_updated': datetime.now().isoformat(),
                'total_batches': total_batches,
                'completed_batches': list(completed_batches),
                'failed_batches': failed_batches,
                'completion_percentage': (len(completed_batches) / total_batches * 100) if total_batches > 0 else 0
            }
            with open(cache_path, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to save progress checkpoint: {e}")
    
    def _load_progress_checkpoint(self) -> Optional[dict]:
        """Load progress checkpoint if available"""
        cache_path = self._get_progress_cache_path()
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r') as f:
                checkpoint_data = json.load(f)
            
            if checkpoint_data.get('namespace') != self.namespace:
                return None
            
            return checkpoint_data
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load progress checkpoint: {e}")
            return None
    
    def _clear_progress_checkpoint(self) -> None:
        """Clear progress checkpoint after successful completion"""
        cache_path = self._get_progress_cache_path()
        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to clear progress checkpoint: {e}")
    
    def _save_enumeration_cache(self, ids: List[str]) -> None:
        """Save enumerated IDs to cache file with metadata"""
        cache_path = self._get_enumeration_cache_path()
        try:
            cache_data = {
                'namespace': self.namespace,
                'enumerated_at': datetime.now().isoformat(),
                'total_ids': len(ids),
                'ids': ids
            }
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f)
            self.logger.success(f"💾 Saved enumeration cache: {cache_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to save enumeration cache: {e}")
    
    def _load_enumeration_cache(self) -> Optional[List[str]]:
        """Load enumerated IDs from cache if available and recent"""
        cache_path = self._get_enumeration_cache_path()
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)
            
            # Validate cache
            if cache_data.get('namespace') != self.namespace:
                self.logger.warning("⚠️ Cache namespace mismatch, re-enumerating...")
                return None
            
            # Check cache age (warn if older than 24 hours)
            cached_at = datetime.fromisoformat(cache_data['enumerated_at'])
            age_hours = (datetime.now() - cached_at).total_seconds() / 3600
            
            ids = cache_data.get('ids', [])
            self.logger.info(f"📂 Found enumeration cache: {len(ids):,} IDs")
            self.logger.info(f"   Cached at: {cached_at.strftime('%Y-%m-%d %H:%M:%S')} ({age_hours:.1f} hours ago)")
            
            if age_hours > 24:
                self.logger.warning(f"⚠️ Cache is {age_hours:.1f} hours old (>24h)")
                response = input("   Use cached enumeration anyway? (y/n): ").strip().lower()
                if response != 'y':
                    return None
            
            return ids
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load enumeration cache: {e}")
            return None
    
    def _iter_all_ids(self, limit: int = 100, use_cache: bool = True) -> List[str]:
        """Collect all vector ids in the namespace using Pinecone list() pagination.
        
        Args:
            limit: Page size for list() pagination
            use_cache: If True, try to load from cache before enumerating
        """
        # Try to load from cache first
        if use_cache:
            cached_ids = self._load_enumeration_cache()
            if cached_ids:
                self.logger.success(f"✅ Using cached enumeration: {len(cached_ids):,} IDs")
                return cached_ids
        
        # Enumerate from Pinecone
        ids: List[str] = []
        try:
            self.logger.info(f"📋 Enumerating document IDs (this may take a few minutes for large namespaces)...")
            page_count = 0
            for vector_ids in self.pinecone_client.index.list(namespace=self.namespace, limit=limit):
                page_count += 1
                if isinstance(vector_ids, str):
                    ids.append(vector_ids)
                else:
                    try:
                        for vid in vector_ids:
                            ids.append(vid)
                    except TypeError:
                        ids.append(vector_ids)  # type: ignore
                
                # Log progress every 100 pages
                if page_count % 100 == 0:
                    self.logger.info(f"   📄 Enumerated {len(ids):,} IDs so far...")
            
            self.logger.info(f"   ✅ Enumeration complete: {len(ids):,} total IDs")
            
            # Save to cache
            self._save_enumeration_cache(ids)
            
        except Exception as e:
            self.logger.error(f"❌ Error listing vector ids: {e}")
        
        return ids

    def _is_populated(self, value: Any) -> bool:
        """Check if a metadata field value is actually populated (not None, empty, or "None" string)"""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        s = str(value).strip()
        return s != '' and s.lower() not in ('none', 'null', 'nan', '')
    
    def _extract_document_paths_from_log(self, log_path: str) -> List[str]:
        """Extract document paths from processing log file"""
        patterns = [
            re.compile(r"Successfully processed\s+(?P<path>.+?):\s+\d+\s+chunks", re.UNICODE),
            re.compile(r"processed\s+(?P<path>.+?):\s+\d+\s+chunks", re.UNICODE),
            re.compile(r"processed\s+(?P<path>.+?):\s+\d+\s+chunks\b", re.UNICODE),
        ]
        paths: List[str] = []
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    for pat in patterns:
                        m = pat.search(line)
                        if m:
                            paths.append(m.group('path').strip())
                            break
        except Exception as e:
            self.logger.error(f"❌ Error reading log file {log_path}: {e}")
        
        # De-duplicate preserving order
        seen = set()
        unique = []
        for p in paths:
            if p not in seen:
                unique.append(p)
                seen.add(p)
        return unique
    
    def _extract_document_paths_from_json(self, json_path: str) -> List[str]:
        """Extract document paths from enhanced discovery JSON file"""
        paths: List[str] = []
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            docs = data.get('documents', [])
            for d in docs:
                p = d.get('path') or d.get('document_path') or ''
                if p:
                    # Normalize to relative under organized_salesforce_v2 if applicable
                    if p.startswith('/Volumes/Jeff_2TB/organized_salesforce_v2/'):
                        p = p.replace('/Volumes/Jeff_2TB/organized_salesforce_v2/', '')
                    paths.append(p)
        except Exception as e:
            self.logger.error(f"❌ Error reading JSON file {json_path}: {e}")
        
        # De-duplicate preserving order
        seen = set()
        unique = []
        for p in paths:
            if p not in seen:
                unique.append(p)
                seen.add(p)
        return unique
    
    def _autodetect_latest_log(self) -> Optional[str]:
        """Auto-detect the latest processing log file"""
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        cand_paths: List[str] = []
        today = datetime.now().strftime('%Y%m%d')
        
        for sub in ('processing', 'progress'):
            d = os.path.join(repo_root, 'logs', sub)
            if not os.path.isdir(d):
                continue
            todays = [os.path.join(d, n) for n in os.listdir(d) if n.endswith('.log') and today in n]
            cand_paths.extend(todays)
        
        # Fallback to any recent logs if none for today
        if not cand_paths:
            for sub in ('processing', 'progress'):
                d = os.path.join(repo_root, 'logs', sub)
                if not os.path.isdir(d):
                    continue
                cand_paths.extend([os.path.join(d, n) for n in os.listdir(d) if n.endswith('.log')])
        
        if not cand_paths:
            return None
        
        cand_paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return cand_paths[0]
    
    def _query_one_metadata_by_path(self, doc_path: str) -> Optional[Dict]:
        """Fetch one chunk's metadata for a given document path using multiple strategies"""
        # Strategy 1: Exact match (as-logged)
        try:
            res = self.pinecone_client.index.query(
                namespace=self.namespace,
                vector=[0.0] * 1024,
                top_k=1,
                include_metadata=True,
                filter={"document_path": {"$eq": doc_path}}
            )
            matches = getattr(res, 'matches', res.get('matches', [])) if isinstance(res, dict) else res.matches
            if matches:
                return matches[0].metadata if hasattr(matches[0], 'metadata') else matches[0].get('metadata', {})
        except Exception:
            pass

        # Strategy 2: Absolute path under organized_salesforce_v2
        try:
            abs_path = f"/Volumes/Jeff_2TB/organized_salesforce_v2/{doc_path}" if not doc_path.startswith('/Volumes/Jeff_2TB/') else doc_path
            res = self.pinecone_client.index.query(
                namespace=self.namespace,
                vector=[0.0] * 1024,
                top_k=1,
                include_metadata=True,
                filter={"document_path": {"$eq": abs_path}}
            )
            matches = getattr(res, 'matches', res.get('matches', [])) if isinstance(res, dict) else res.matches
            if matches:
                return matches[0].metadata if hasattr(matches[0], 'metadata') else matches[0].get('metadata', {})
        except Exception:
            pass

        # Strategy 3: Fallback: basename + file_type
        try:
            base = os.path.basename(doc_path)
            _, ext = os.path.splitext(base)
            filt: Dict[str, Any] = {"file_name": {"$eq": base}}
            if ext:
                filt = {"$and": [filt, {"file_type": {"$eq": ext}}]}
            res = self.pinecone_client.index.query(
                namespace=self.namespace,
                vector=[0.0] * 1024,
                top_k=1,
                include_metadata=True,
                filter=filt
            )
            matches = getattr(res, 'matches', res.get('matches', [])) if isinstance(res, dict) else res.matches
            if matches:
                return matches[0].metadata if hasattr(matches[0], 'metadata') else matches[0].get('metadata', {})
        except Exception:
            pass

        return None

    def _accumulate_field_stats(self, 
                                vectors_map: Dict,
                                field_stats: Dict,
                                client_analysis: Dict,
                                vendor_analysis: Dict) -> Tuple[int, int, int]:
        """Accumulate coverage stats from a fetched vectors map.

        Returns: (processed_count, enhanced_present, enhanced_missing)
        """
        processed = 0
        enhanced_present = 0
        enhanced_missing = 0
        # Key LLM-enhanced fields of interest (even if pruned in newer writes, legacy may exist)
        enhanced_fields = [
            'document_type',
            'document_type_confidence',
            'product_pricing_depth',
            'commercial_terms_depth',
            'proposed_term_start',
            'proposed_term_end',
            'content_summary',
            'key_topics',
            'vendor_products_mentioned',
            'pricing_indicators'
        ]

        vectors = getattr(vectors_map, 'vectors', None)
        if isinstance(vectors_map, dict) and 'vectors' in vectors_map:
            vectors = vectors_map['vectors']

        for _, v in (vectors.items() if isinstance(vectors, dict) else []):
            processed += 1
            metadata = getattr(v, 'metadata', {}) if hasattr(v, 'metadata') else v.get('metadata', {})

            # All fields coverage
            for field, value in metadata.items():
                if self._is_populated(value):
                    field_stats[field]['populated'] += 1
                    if len(field_stats[field]['samples']) < 3:
                        field_stats[field]['samples'].append(str(value)[:50])
                else:
                    field_stats[field]['empty'] += 1

            # Enhanced subset presence (for quick headline)
            if any(metadata.get(f) not in (None, "", "None") for f in enhanced_fields):
                enhanced_present += 1
            else:
                enhanced_missing += 1

            # Client/Vendor rollups
            client_id = metadata.get('client_id')
            client_name = metadata.get('client_name')
            if self._is_populated(client_id):
                client_analysis['has_id'] += 1
            if self._is_populated(client_name):
                client_analysis['has_name'] += 1

            vendor_id = metadata.get('vendor_id')
            vendor_name = metadata.get('vendor_name')
            if self._is_populated(vendor_id):
                vendor_analysis['has_id'] += 1
            if self._is_populated(vendor_name):
                vendor_analysis['has_name'] += 1

        return processed, enhanced_present, enhanced_missing

    
    def analyze_coverage(self, sample_size: int = 1000, focus: Optional[str] = None, show_all_fields: bool = False):
        """Comprehensive metadata coverage analysis using list()+fetch() pattern (FIXED)"""
        self.logger.info(f"🔍 METADATA COVERAGE ANALYSIS")
        self.logger.info(f"Namespace: {self.namespace}")
        self.logger.info(f"Sample Size: {sample_size:,}")
        if focus:
            self.logger.info(f"Focus: {focus}")
        if show_all_fields:
            self.logger.info("📋 Showing ALL fields (not just top 20)")
        self.logger.info("=" * 60)
        
        # Store flag for report printing
        self._show_all_fields = show_all_fields
        
        # Load mapping files
        mappings = self._load_mapping_coverage()
        
        # ✅ FIXED: Use list()+fetch() instead of query() for reliable sampling
        self.logger.info(f"📋 Enumerating namespace using list()...")
        use_cache = not getattr(self, '_no_cache', False)
        all_ids = self._iter_all_ids(limit=100, use_cache=use_cache)
        
        if not all_ids:
            self.logger.error("❌ No documents found in namespace")
            return
        
        # Random sample for analysis
        if len(all_ids) > sample_size:
            import random
            sample_ids = random.sample(all_ids, sample_size)
            self.logger.info(f"✅ Randomly sampled {sample_size:,} documents from {len(all_ids):,} total")
        else:
            sample_ids = all_ids
            self.logger.info(f"✅ Using all {len(sample_ids):,} documents (less than sample size)")
        
        # Fetch in batches
        self.logger.info(f"📥 Fetching {len(sample_ids):,} documents in batches...")
        matches = []
        batch_size = 100  # Respect Pinecone batch limits
        
        for i in range(0, len(sample_ids), batch_size):
            batch_ids = sample_ids[i:i+batch_size]
            try:
                fetched = self.pinecone_client.index.fetch(ids=batch_ids, namespace=self.namespace)
                vectors = getattr(fetched, 'vectors', None) or fetched.get('vectors', {})
                
                for vid, vector in vectors.items():
                    metadata = getattr(vector, 'metadata', {}) if hasattr(vector, 'metadata') else vector.get('metadata', {})
                    matches.append({
                        'id': vid,
                        'metadata': metadata
                    })
            except Exception as e:
                self.logger.warning(f"⚠️ Fetch failed for batch {i//batch_size + 1}: {e}")
                continue
            
            if (i + batch_size) % (batch_size * 10) == 0:
                self.logger.info(f"📈 Fetched {len(matches):,}/{len(sample_ids):,} documents...")
        
        if not matches:
            self.logger.error("❌ No documents retrieved")
            return
        
        self.logger.success(f"✅ Retrieved {len(matches):,} documents")
        
        # Analyze metadata fields
        field_stats = defaultdict(lambda: {'populated': 0, 'empty': 0, 'samples': []})
        client_analysis = {'has_id': 0, 'has_name': 0, 'mappable': 0, 'unmappable_ids': set()}
        vendor_analysis = {'has_id': 0, 'has_name': 0, 'mappable': 0, 'unmappable_ids': set()}
        
        for match in matches:
            metadata = match['metadata']
            
            # Analyze all metadata fields (or filtered by focus)
            for field, value in metadata.items():
                if focus == 'vendor' and 'vendor' not in field.lower():
                    continue
                if focus == 'client' and 'client' not in field.lower():
                    continue
                
                if self._is_populated(value):
                    field_stats[field]['populated'] += 1
                    if len(field_stats[field]['samples']) < 3:
                        field_stats[field]['samples'].append(str(value)[:50])
                else:
                    field_stats[field]['empty'] += 1
            
            # Client analysis
            client_id = metadata.get('client_id')
            client_name = metadata.get('client_name')
            
            if self._is_populated(client_id):
                client_analysis['has_id'] += 1
                if client_id in mappings['clients']:
                    client_analysis['mappable'] += 1
                else:
                    client_analysis['unmappable_ids'].add(client_id)
            
            if self._is_populated(client_name):
                client_analysis['has_name'] += 1
            
            # Vendor analysis
            vendor_id = metadata.get('vendor_id')
            vendor_name = metadata.get('vendor_name')
            
            if self._is_populated(vendor_id):
                vendor_analysis['has_id'] += 1
                if vendor_id in mappings['vendors']:
                    vendor_analysis['mappable'] += 1
                else:
                    vendor_analysis['unmappable_ids'].add(vendor_id)
            
            if self._is_populated(vendor_name):
                vendor_analysis['has_name'] += 1
        
        # Print comprehensive report
        self._print_coverage_report(field_stats, client_analysis, vendor_analysis, len(matches), focus=focus)

    def analyze_full_scan(self, batch_size: int = 50, max_ids: Optional[int] = None, list_page_limit: int = 100, csv_path: Optional[str] = None, focus: Optional[str] = None, show_all_fields: bool = False) -> None:
        """Full namespace coverage using list()+fetch() in batches for accuracy and efficiency.

        Args:
            batch_size: Number of ids to fetch per batch (keep small to avoid response limits)
            max_ids: Optional cap on total ids to scan (None = all)
            list_page_limit: Page size for index.list() pagination
        """
        self.logger.info(f"🔎 FULL METADATA COVERAGE SCAN")
        self.logger.info(f"Namespace: {self.namespace}")
        self.logger.info(f"Batch size: {batch_size}")
        if show_all_fields:
            self.logger.info("📋 Showing ALL fields (not just top 20)")
        
        # Store flag for report printing
        self._show_all_fields = show_all_fields
        use_cache = not getattr(self, '_no_cache', False)
        ids = self._iter_all_ids(limit=list_page_limit, use_cache=use_cache)
        if not ids:
            self.logger.error("❌ No ids enumerated from namespace")
            return
        total_ids = len(ids) if not max_ids or max_ids <= 0 else min(len(ids), max_ids)
        ids = ids[:total_ids]
        self.logger.success(f"✅ Enumerated {total_ids:,} ids for scanning")

        # Stats structures
        field_stats = defaultdict(lambda: {'populated': 0, 'empty': 0, 'samples': []})
        client_analysis = {'has_id': 0, 'has_name': 0, 'mappable': 0, 'unmappable_ids': set()}
        vendor_analysis = {'has_id': 0, 'has_name': 0, 'mappable': 0, 'unmappable_ids': set()}

        processed_total = 0
        enhanced_present_total = 0
        enhanced_missing_total = 0

        # Load mappings for full scan
        self.logger.info("📂 Loading mapping files...")
        mappings = self._load_mapping_coverage()
        self.logger.info("✅ Starting batch processing...")

        # Optional CSV export setup
        csv_file = None
        csv_writer = None
        if csv_path:
            try:
                csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(["field","populated","empty","percentage","sample_values"])
            except Exception as e:
                self.logger.warning(f"⚠️ Could not open CSV for writing: {e}")

        # Load progress checkpoint if available
        checkpoint = self._load_progress_checkpoint()
        completed_batches = set(checkpoint['completed_batches']) if checkpoint else set()
        failed_batches = checkpoint['failed_batches'] if checkpoint else []
        total_batches = (len(ids) + batch_size - 1) // batch_size
        
        if checkpoint:
            self.logger.info(f"📂 Found progress checkpoint: {len(completed_batches)}/{total_batches} batches completed")
            self.logger.info(f"   {len(failed_batches)} batches previously failed")
            response = input("   Resume from checkpoint? (y/n): ").strip().lower()
            if response != 'y':
                completed_batches = set()
                failed_batches = []
                self.logger.info("   Starting fresh scan...")
        
        checkpoint_interval = 100  # Save checkpoint every 100 batches
        
        for start in range(0, len(ids), batch_size):
            end = min(start + batch_size, len(ids))
            batch_ids = ids[start:end]
            batch_num = start // batch_size + 1
            
            # Skip if already completed
            if batch_num in completed_batches:
                continue
            
            try:
                fetched = self.pinecone_client.index.fetch(ids=batch_ids, namespace=self.namespace)
                proc, enh_p, enh_m = self._accumulate_field_stats(fetched, field_stats, client_analysis, vendor_analysis)
                processed_total += proc
                enhanced_present_total += enh_p
                enhanced_missing_total += enh_m
                
                # Update mapping analysis
                vectors = getattr(fetched, 'vectors', None) or fetched.get('vectors', {})
                for vid, vector in vectors.items():
                    metadata = getattr(vector, 'metadata', {}) if hasattr(vector, 'metadata') else vector.get('metadata', {})
                    client_id = metadata.get('client_id')
                    vendor_id = metadata.get('vendor_id')
                    
                    if self._is_populated(client_id):
                        if client_id in mappings['clients']:
                            client_analysis['mappable'] += 1
                        else:
                            client_analysis['unmappable_ids'].add(client_id)
                    
                    if self._is_populated(vendor_id):
                        if vendor_id in mappings['vendors']:
                            vendor_analysis['mappable'] += 1
                        else:
                            vendor_analysis['unmappable_ids'].add(vendor_id)
                
                # Mark batch as completed
                completed_batches.add(batch_num)
                
            except Exception as e:
                error_str = str(e)
                # Check if it's a 414 URI too large error
                if '414' in error_str or 'Request-URI Too Large' in error_str:
                    self.logger.warning(f"⚠️ Batch {batch_num} failed (414 URI too large) - splitting into smaller batches...")
                    # Retry with smaller sub-batches
                    sub_batch_size = batch_size // 2
                    for sub_start in range(start, end, sub_batch_size):
                        sub_end = min(sub_start + sub_batch_size, end)
                        sub_batch_ids = ids[sub_start:sub_end]
                        try:
                            fetched = self.pinecone_client.index.fetch(ids=sub_batch_ids, namespace=self.namespace)
                            proc, enh_p, enh_m = self._accumulate_field_stats(fetched, field_stats, client_analysis, vendor_analysis)
                            processed_total += proc
                            enhanced_present_total += enh_p
                            enhanced_missing_total += enh_m
                            
                            # Update mapping analysis
                            vectors = getattr(fetched, 'vectors', None) or fetched.get('vectors', {})
                            for vid, vector in vectors.items():
                                metadata = getattr(vector, 'metadata', {}) if hasattr(vector, 'metadata') else vector.get('metadata', {})
                                client_id = metadata.get('client_id')
                                vendor_id = metadata.get('vendor_id')
                                
                                if self._is_populated(client_id):
                                    if client_id in mappings['clients']:
                                        client_analysis['mappable'] += 1
                                    else:
                                        client_analysis['unmappable_ids'].add(client_id)
                                
                                if self._is_populated(vendor_id):
                                    if vendor_id in mappings['vendors']:
                                        vendor_analysis['mappable'] += 1
                                    else:
                                        vendor_analysis['unmappable_ids'].add(vendor_id)
                            
                            self.logger.info(f"   ✅ Sub-batch succeeded with size {sub_batch_size}")
                        except Exception as sub_e:
                            self.logger.warning(f"   ⚠️ Sub-batch also failed: {sub_e}")
                            failed_batches.append((batch_num, len(sub_batch_ids)))
                else:
                    self.logger.warning(f"⚠️ Fetch failed for batch {batch_num}: {e}")
                    failed_batches.append((batch_num, len(batch_ids)))
                continue

            # Save checkpoint periodically
            if batch_num % checkpoint_interval == 0:
                self._save_progress_checkpoint(completed_batches, failed_batches, total_batches)
            
            # Show progress more frequently (every 10 batches or every 5%)
            progress_interval = max(batch_size * 10, len(ids) // 20)  # Every 10 batches or every 5%
            if processed_total % progress_interval < batch_size or end == len(ids):
                pct = (processed_total / len(ids)) * 100 if len(ids) > 0 else 0
                self.logger.info(f"📈 Progress: {processed_total:,}/{len(ids):,} ({pct:.1f}%) | Batches: {len(completed_batches)}/{total_batches}")

        self.logger.success(f"✅ Completed full scan of {processed_total:,} vectors")
        
        # Report failed batches
        if failed_batches:
            total_failed_docs = sum(count for _, count in failed_batches)
            self.logger.warning(f"\n⚠️ {len(failed_batches)} batches failed ({total_failed_docs:,} documents)")
            self.logger.warning(f"   Coverage: {processed_total:,}/{len(ids):,} ({(processed_total/len(ids)*100):.1f}%)")
            self.logger.info(f"   Failed batch numbers: {[num for num, _ in failed_batches[:10]]}")
            if len(failed_batches) > 10:
                self.logger.info(f"   ... and {len(failed_batches) - 10} more")
            
            # Save final checkpoint with failed batches
            self._save_progress_checkpoint(completed_batches, failed_batches, total_batches)
            self.logger.info(f"\n💾 Progress saved. To retry only failed batches, run with --retry-failed")
        else:
            # Clear checkpoint on successful completion
            self._clear_progress_checkpoint()
        
        # Print report
        self._print_coverage_report(field_stats, client_analysis, vendor_analysis, processed_total, focus=focus)

        # Write CSV if requested
        if csv_writer:
            try:
                # Sort fields by populated desc
                sorted_fields = sorted(field_stats.items(), key=lambda x: x[1]['populated'], reverse=True)
                for field, stats in sorted_fields:
                    populated = stats['populated']
                    empty = stats['empty']
                    pct = (populated / processed_total * 100) if processed_total else 0.0
                    samples = "; ".join(stats['samples'][:3])
                    csv_writer.writerow([field, populated, empty, f"{pct:.2f}", samples])
                self.logger.success(f"💾 Wrote coverage CSV: {csv_path}")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed writing CSV: {e}")
            finally:
                try:
                    csv_file.close()  # type: ignore
                except Exception:
                    pass
    
    def analyze_from_logs_or_json(self, log_path: Optional[str] = None, json_paths: Optional[List[str]] = None, limit: Optional[int] = None, csv_path: Optional[str] = None):
        """Analyze metadata coverage for documents from logs or JSON files"""
        self.logger.info(f"🔍 LOG/JSON-BASED METADATA COVERAGE ANALYSIS")
        self.logger.info(f"Namespace: {self.namespace}")
        
        # Collect document paths
        doc_paths: List[str] = []
        
        if json_paths:
            for jp in json_paths:
                if os.path.exists(jp):
                    paths = self._extract_document_paths_from_json(jp)
                    doc_paths.extend(paths)
                    self.logger.info(f"✅ Extracted {len(paths)} paths from {jp}")
                else:
                    self.logger.warning(f"⚠️ JSON file not found: {jp}")
        elif log_path:
            if os.path.exists(log_path):
                paths = self._extract_document_paths_from_log(log_path)
                doc_paths.extend(paths)
                self.logger.info(f"✅ Extracted {len(paths)} paths from {log_path}")
            else:
                self.logger.error(f"❌ Log file not found: {log_path}")
                return
        else:
            # Auto-detect latest log
            log_path = self._autodetect_latest_log()
            if log_path:
                paths = self._extract_document_paths_from_log(log_path)
                doc_paths.extend(paths)
                self.logger.info(f"✅ Auto-detected and extracted {len(paths)} paths from {log_path}")
            else:
                self.logger.error("❌ No log file found and none specified")
                return
        
        if not doc_paths:
            self.logger.error("❌ No document paths extracted")
            return
        
        if limit and limit > 0:
            doc_paths = doc_paths[:limit]
            self.logger.info(f"📊 Limited to {len(doc_paths)} documents")
        
        # Fetch metadata for each document path
        self.logger.info(f"📥 Fetching metadata for {len(doc_paths):,} documents...")
        field_stats = defaultdict(lambda: {'populated': 0, 'empty': 0})
        seen_docs = 0
        
        for i, dp in enumerate(doc_paths):
            md = self._query_one_metadata_by_path(dp)
            if not md:
                continue
            
            seen_docs += 1
            for field in FIELDS:
                if self._is_populated(md.get(field)):
                    field_stats[field]['populated'] += 1
                else:
                    field_stats[field]['empty'] += 1
            
            if (i + 1) % 100 == 0:
                self.logger.info(f"📈 Processed {i + 1:,}/{len(doc_paths):,} documents...")
        
        self.logger.success(f"✅ Examined {seen_docs:,} documents")
        
        # Write CSV
        if csv_path:
            try:
                with open(csv_path, 'w', newline='') as f:
                    w = csv.writer(f)
                    w.writerow(['field', 'populated', 'total_docs', 'percent'])
                    total = max(seen_docs, 1)
                    for field in FIELDS:
                        p = field_stats[field]['populated']
                        pct = round(100.0 * p / total, 2)
                        w.writerow([field, p, total, pct])
                self.logger.success(f"💾 Wrote coverage CSV: {csv_path}")
            except Exception as e:
                self.logger.error(f"❌ Failed writing CSV: {e}")
        else:
            # Auto-generate CSV path
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            out_dir = os.path.join(repo_root, 'output')
            os.makedirs(out_dir, exist_ok=True)
            today = datetime.now().strftime('%Y%m%d')
            csv_path = os.path.join(out_dir, f'metadata_coverage_{self.namespace.replace("-", "_")}_{today}.csv')
            try:
                with open(csv_path, 'w', newline='') as f:
                    w = csv.writer(f)
                    w.writerow(['field', 'populated', 'total_docs', 'percent'])
                    total = max(seen_docs, 1)
                    for field in FIELDS:
                        p = field_stats[field]['populated']
                        pct = round(100.0 * p / total, 2)
                        w.writerow([field, p, total, pct])
                self.logger.success(f"💾 Wrote coverage CSV: {csv_path}")
            except Exception as e:
                self.logger.error(f"❌ Failed writing CSV: {e}")
        
        # Print summary
        self.logger.info(f"\n📊 SUMMARY")
        self.logger.info(f"  Documents parsed: {len(doc_paths):,}")
        self.logger.info(f"  Documents examined: {seen_docs:,}")
        self.logger.info(f"  Output CSV: {csv_path}")
    
    def analyze_vendor_quick(self, sample_size: int = 1000):
        """Quick vendor metadata status check (replaces check_vendor_metadata_status.py)"""
        print(f"🔍 Checking Vendor Metadata Status in {self.namespace}")
        print("=" * 60)
        
        try:
            # Get index stats first
            stats = self.pinecone_client.index.describe_index_stats()
            namespace_stats = stats.namespaces.get(self.namespace, {})
            total_vectors = namespace_stats.get('vector_count', 0)
            
            print(f"📊 Total vectors in namespace: {total_vectors:,}")
            
            # ✅ FIXED: Use list()+fetch() instead of query()
            print(f"\n🔍 Sampling {sample_size} documents using list()+fetch()...")
            all_ids = self._iter_all_ids(limit=100)
            
            if not all_ids:
                print("❌ No documents found!")
                return
            
            # Random sample
            if len(all_ids) > sample_size:
                import random
                sample_ids = random.sample(all_ids, sample_size)
            else:
                sample_ids = all_ids
            
            # Fetch in batches
            matches = []
            batch_size = 100
            for i in range(0, len(sample_ids), batch_size):
                batch_ids = sample_ids[i:i+batch_size]
                try:
                    fetched = self.pinecone_client.index.fetch(ids=batch_ids, namespace=self.namespace)
                    vectors = getattr(fetched, 'vectors', None) or fetched.get('vectors', {})
                    for vid, vector in vectors.items():
                        metadata = getattr(vector, 'metadata', {}) if hasattr(vector, 'metadata') else vector.get('metadata', {})
                        matches.append({'id': vid, 'metadata': metadata})
                except Exception as e:
                    print(f"⚠️ Fetch failed for batch {i//batch_size + 1}: {e}")
                    continue
            
            if not matches:
                print("❌ No documents found!")
                return
            
            print(f"✅ Found {len(matches)} documents to analyze")
            
            # Analyze vendor metadata
            stats = {
                'total_sampled': len(matches),
                'has_vendor_id': 0,
                'has_vendor_name': 0,
                'has_both': 0,
                'has_neither': 0,
                'vendor_id_is_none_string': 0,
                'vendor_name_is_none_string': 0
            }
            
            sample_records = []
            
            for match in matches:
                metadata = match['metadata']
                
                vendor_id = metadata.get('vendor_id', '')
                vendor_name = metadata.get('vendor_name', '')
                
                # Check if values are actually populated (not empty, None, or "None" string)
                vendor_id_str = str(vendor_id).strip() if vendor_id else ''
                vendor_name_str = str(vendor_name).strip() if vendor_name else ''
                
                has_real_vendor_id = vendor_id_str and vendor_id_str.lower() not in ['none', 'nan', '']
                has_real_vendor_name = vendor_name_str and vendor_name_str.lower() not in ['none', 'nan', '']
                
                if has_real_vendor_id:
                    stats['has_vendor_id'] += 1
                if has_real_vendor_name:
                    stats['has_vendor_name'] += 1
                if has_real_vendor_id and has_real_vendor_name:
                    stats['has_both'] += 1
                if not has_real_vendor_id and not has_real_vendor_name:
                    stats['has_neither'] += 1
                    
                # Check for "None" strings specifically
                if vendor_id_str.lower() == 'none':
                    stats['vendor_id_is_none_string'] += 1
                if vendor_name_str.lower() == 'none':
                    stats['vendor_name_is_none_string'] += 1
                
                # Collect some sample records for display
                if len(sample_records) < 10:
                    sample_records.append({
                        'id': match['id'],
                        'vendor_id': vendor_id_str[:30] if vendor_id_str else 'EMPTY',
                        'vendor_name': vendor_name_str[:30] if vendor_name_str else 'EMPTY',
                        'deal_id': metadata.get('deal_id', 'N/A'),
                        'document_path': metadata.get('document_path', 'N/A')[:50] if metadata.get('document_path') else 'N/A'
                    })
            
            # Print results
            print(f"\n📊 VENDOR METADATA ANALYSIS RESULTS")
            print("=" * 50)
            
            for key, value in stats.items():
                if key == 'total_sampled':
                    continue
                percentage = (value / stats['total_sampled']) * 100
                print(f"  {key.replace('_', ' ').title()}: {value:,}/{stats['total_sampled']:,} ({percentage:.1f}%)")
            
            # Show sample records
            print(f"\n📋 SAMPLE RECORDS:")
            print("-" * 50)
            for i, record in enumerate(sample_records, 1):
                print(f"  Record {i}:")
                print(f"    ID: {record['id']}")
                print(f"    Vendor ID: {record['vendor_id']}")
                print(f"    Vendor Name: {record['vendor_name']}")
                print(f"    Deal ID: {record['deal_id']}")
                print(f"    Path: {record['document_path']}")
                print()
            
            # Calculate estimated totals
            print(f"\n🎯 ESTIMATED TOTALS FOR ENTIRE NAMESPACE:")
            print("-" * 50)
            
            vendor_id_percentage = (stats['has_vendor_id'] / stats['total_sampled']) * 100
            vendor_name_percentage = (stats['has_vendor_name'] / stats['total_sampled']) * 100
            
            estimated_with_vendor_id = int((vendor_id_percentage / 100) * total_vectors)
            estimated_with_vendor_name = int((vendor_name_percentage / 100) * total_vectors)
            
            print(f"  Estimated documents with vendor_id: {estimated_with_vendor_id:,} ({vendor_id_percentage:.1f}%)")
            print(f"  Estimated documents with vendor_name: {estimated_with_vendor_name:,} ({vendor_name_percentage:.1f}%)")
            print(f"  Estimated missing vendor_id: {total_vectors - estimated_with_vendor_id:,}")
            print(f"  Estimated missing vendor_name: {total_vectors - estimated_with_vendor_name:,}")
            
            if vendor_id_percentage < 5 and vendor_name_percentage < 5:
                print(f"\n⚠️  CONCLUSION: Vendor data appears to be mostly missing!")
                print(f"     Consider running vendor population script.")
            else:
                print(f"\n✅ CONCLUSION: Vendor data IS populated")
        
        except Exception as e:
            print(f"❌ Error checking vendor status: {e}")
            import traceback
            traceback.print_exc()
    
    def _print_coverage_report(self, field_stats: Dict, client_analysis: Dict, vendor_analysis: Dict, total_docs: int, focus: Optional[str] = None):
        """Print comprehensive coverage analysis report"""
        
        # Filter fields by focus if specified
        if focus == 'vendor':
            # Show only vendor-related fields
            filtered_fields = {k: v for k, v in field_stats.items() if 'vendor' in k.lower()}
            field_stats = filtered_fields
        elif focus == 'client':
            # Show only client-related fields
            filtered_fields = {k: v for k, v in field_stats.items() if 'client' in k.lower()}
            field_stats = filtered_fields
        
        # Overall field coverage
        self.logger.info("\n📊 METADATA FIELD COVERAGE")
        self.logger.info("-" * 60)
        
        # Sort fields by population percentage
        sorted_fields = sorted(field_stats.items(), 
                             key=lambda x: x[1]['populated'], reverse=True)
        
        # Show all fields if requested, otherwise top 20
        display_count = len(sorted_fields) if getattr(self, '_show_all_fields', False) else (20 if focus is None else len(sorted_fields))
        total_fields_found = len(sorted_fields)
        
        if display_count < total_fields_found:
            self.logger.info(f"📊 Showing top {display_count} of {total_fields_found} fields found")
            self.logger.info(f"   Use --show-all-fields to see all {total_fields_found} fields")
        
        for field, stats in sorted_fields[:display_count]:
            populated = stats['populated']
            percentage = (populated / total_docs) * 100
            samples = ", ".join(stats['samples'][:2]) if stats.get('samples') else ""
            
            status = "✅" if percentage > 50 else "⚠️" if percentage > 10 else "❌"
            self.logger.info(f"{status} {field:<30} {populated:>6}/{total_docs} ({percentage:>5.1f}%) | {samples}")
        
        # Client/Vendor specific analysis (always show if not vendor-only focus)
        if focus != 'vendor':
            self.logger.info("\n👥 CLIENT NAME ANALYSIS")
            self.logger.info("-" * 40)
            client_id_pct = (client_analysis['has_id'] / total_docs) * 100 if total_docs > 0 else 0
            client_name_pct = (client_analysis['has_name'] / total_docs) * 100 if total_docs > 0 else 0
            client_mappable_pct = (client_analysis['mappable'] / total_docs) * 100 if total_docs > 0 else 0
            
            self.logger.info(f"📋 Documents with client_id: {client_analysis['has_id']:,} ({client_id_pct:.1f}%)")
            self.logger.info(f"📋 Documents with client_name: {client_analysis['has_name']:,} ({client_name_pct:.1f}%)")
            self.logger.info(f"🔗 Mappable client_ids: {client_analysis['mappable']:,} ({client_mappable_pct:.1f}%)")
            
            if client_analysis['unmappable_ids']:
                self.logger.warning(f"⚠️ Unmappable client IDs: {len(client_analysis['unmappable_ids'])}")
                sample_unmappable = list(client_analysis['unmappable_ids'])[:5]
                self.logger.warning(f"   Sample unmappable IDs: {sample_unmappable}")
        
        if focus != 'client':
            self.logger.info("\n🏢 VENDOR NAME ANALYSIS") 
            self.logger.info("-" * 40)
            vendor_id_pct = (vendor_analysis['has_id'] / total_docs) * 100 if total_docs > 0 else 0
            vendor_name_pct = (vendor_analysis['has_name'] / total_docs) * 100 if total_docs > 0 else 0
            vendor_mappable_pct = (vendor_analysis['mappable'] / total_docs) * 100 if total_docs > 0 else 0
            
            self.logger.info(f"📋 Documents with vendor_id: {vendor_analysis['has_id']:,} ({vendor_id_pct:.1f}%)")
            self.logger.info(f"📋 Documents with vendor_name: {vendor_analysis['has_name']:,} ({vendor_name_pct:.1f}%)")
            self.logger.info(f"🔗 Mappable vendor_ids: {vendor_analysis['mappable']:,} ({vendor_mappable_pct:.1f}%)")
            
            if vendor_analysis['unmappable_ids']:
                self.logger.warning(f"⚠️ Unmappable vendor IDs: {len(vendor_analysis['unmappable_ids'])}")
                sample_unmappable = list(vendor_analysis['unmappable_ids'])[:5]
                self.logger.warning(f"   Sample unmappable IDs: {sample_unmappable}")
        
        # Enhancement recommendations
        if focus is None:
            self.logger.info("\n💡 ENHANCEMENT RECOMMENDATIONS")
            self.logger.info("-" * 40)
            
            total_potential = client_analysis['mappable'] + vendor_analysis['mappable']
            if total_potential > 0:
                self.logger.success(f"🎯 {total_potential:,} documents can be enhanced with names")
                self.logger.info(f"   Run: python update_client_vendor_names.py --namespace {self.namespace}")
            else:
                self.logger.warning("⚠️ No documents appear to benefit from name enhancement")
                self.logger.info("   Check if client_id/vendor_id fields exist and mapping files are correct")

def main():
    parser = argparse.ArgumentParser(
        description="Unified Pinecone metadata coverage analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick vendor check
  python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --focus vendor --quick
  
  # Comprehensive analysis with sampling
  python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --sample-size 2000
  
  # Full namespace scan
  python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --full-scan --csv output.csv
  
  # Documents from processing logs
  python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --from-log logs/processing/*.log
  
  # Documents from JSON files
  python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --from-json discovery.json
        """
    )
    parser.add_argument("--namespace", required=True, help="Pinecone namespace to analyze")
    parser.add_argument("--focus", choices=['vendor', 'client'], help="Focus analysis on vendor or client fields only")
    parser.add_argument("--quick", action="store_true", help="Quick vendor status check (replaces check_vendor_metadata_status.py)")
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of documents to sample (default: 1000)")
    parser.add_argument("--full-scan", action="store_true", help="Scan entire namespace using list()+fetch()")
    parser.add_argument("--batch-size", type=int, default=100, help="Fetch batch size for full scan (default: 100, max recommended: 100)")
    parser.add_argument("--max-ids", type=int, default=0, help="Optional cap on total ids to scan (0 = all)")
    parser.add_argument("--csv", type=str, default="", help="Optional CSV path for field coverage export")
    parser.add_argument("--from-log", type=str, help="Path to processing log file with document paths")
    parser.add_argument("--from-json", nargs='*', help="One or more JSON files with document paths")
    parser.add_argument("--limit", type=int, help="Limit number of documents for log/JSON analysis")
    parser.add_argument("--show-all-fields", action="store_true", help="Show all metadata fields found (not just top 20)")
    parser.add_argument("--no-cache", action="store_true", help="Skip enumeration cache and re-enumerate from Pinecone")
    parser.add_argument("--clear-cache", action="store_true", help="Clear enumeration cache and exit")
    parser.add_argument("--retry-failed", action="store_true", help="Only process batches that failed in previous run")
    
    args = parser.parse_args()
    
    # Validate batch size against Pinecone limits
    # Based on Pinecone best practices:
    # - Vector fetch operations: max 1000 records per batch
    # - Text records: max 96 records per batch
    # - Total request size: must stay under 2MB
    # - URI length limit: can cause 414 errors with long document IDs
    # Recommended safe maximum: 100 for fetch operations with typical document IDs
    MAX_SAFE_BATCH_SIZE = 100
    if args.batch_size > MAX_SAFE_BATCH_SIZE:
        print(f"⚠️  WARNING: Batch size {args.batch_size} exceeds recommended maximum of {MAX_SAFE_BATCH_SIZE}")
        print(f"   Pinecone fetch() operations may fail with '414 Request-URI Too Large' errors")
        print(f"   when document IDs are long (common in production namespaces).")
        print(f"   Reducing batch size to {MAX_SAFE_BATCH_SIZE}...")
        args.batch_size = MAX_SAFE_BATCH_SIZE
    
    analyzer = MetadataCoverageAnalyzer(args.namespace)
    
    # Clear cache mode
    if args.clear_cache:
        cache_path = analyzer._get_enumeration_cache_path()
        if os.path.exists(cache_path):
            os.remove(cache_path)
            print(f"✅ Cleared enumeration cache: {cache_path}")
        else:
            print(f"ℹ️  No cache file found: {cache_path}")
        return
    
    # Quick vendor check mode
    if args.quick:
        analyzer.analyze_vendor_quick(sample_size=args.sample_size)
    
    # Log/JSON-based analysis mode
    elif args.from_log or args.from_json:
        csv_out = args.csv if args.csv else None
        analyzer.analyze_from_logs_or_json(
            log_path=args.from_log,
            json_paths=args.from_json,
            limit=args.limit,
            csv_path=csv_out
        )
    
    # Full scan mode
    elif args.full_scan:
        csv_out = args.csv if args.csv else None
        analyzer._no_cache = args.no_cache
        analyzer.analyze_full_scan(batch_size=args.batch_size, max_ids=args.max_ids, csv_path=csv_out, focus=args.focus, show_all_fields=args.show_all_fields)
    
    # Sample analysis mode (default)
    else:
        analyzer._no_cache = args.no_cache
        analyzer.analyze_coverage(args.sample_size, focus=args.focus, show_all_fields=args.show_all_fields)

if __name__ == "__main__":
    main()
