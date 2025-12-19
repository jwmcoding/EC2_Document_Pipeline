#!/usr/bin/env python3
"""
Compare documents between two Pinecone indexes/namespaces.

Reconstitutes full document text from chunks and compares parsing/extraction quality
for LLM usefulness, including embedding comparison when possible.
"""

import os
import sys
import argparse
import json
import math
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path

# Add src to path (script is in scripts/compare_pinecone_targets/, need to go up 2 levels)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(script_dir, '..', '..')
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
try:
    load_dotenv()
except PermissionError:
    # In some sandboxed environments (or when .env is protected), reading .env can fail.
    # That's OK as long as required env vars are already set in the shell.
    pass

from src.connectors.pinecone_client import PineconeDocumentClient, _sanitize_str
from src.config.settings import Settings
from pinecone import Pinecone


def sanitize_str(value: Any, default: str = "") -> str:
    """Wrapper for _sanitize_str from pinecone_client"""
    return _sanitize_str(value, default)


class DocumentResolver:
    """Resolve documents by deal_id/file_name/document_name with exact filter + scan fallback"""
    
    def __init__(self, pinecone_client: PineconeDocumentClient, namespace: str):
        self.client = pinecone_client
        self.namespace = namespace
        self.index = pinecone_client.index
        self._vector_dim: Optional[int] = None

    def _dummy_query_vector(self) -> List[float]:
        """Return a dummy vector of the correct dimension for filter-only Pinecone queries."""
        if self._vector_dim is None:
            try:
                stats = self.client.get_index_stats() or {}
                dim = stats.get("dimension")
                self._vector_dim = int(dim) if dim else 1024
            except Exception:
                self._vector_dim = 1024
        return [0.0] * int(self._vector_dim)
    
    def resolve_document(
        self,
        deal_id: Optional[str] = None,
        file_name: Optional[str] = None,
        document_name: Optional[str] = None,
        match_mode: str = "exact",
        scan_max_ids: int = 10000,
        scan_batch_size: int = 100
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Resolve a document by metadata criteria.
        
        Returns:
            (resolved_doc_info, candidate_list)
            resolved_doc_info: Dict with document key and metadata if unique match found
            candidate_list: List of all candidates if ambiguous
        """
        # Normalize inputs (document_name is alias for file_name)
        if document_name and not file_name:
            file_name = document_name
        
        # Try exact filter first
        if match_mode == "exact" or match_mode == "auto":
            filter_dict = {}
            if deal_id:
                filter_dict["deal_id"] = {"$eq": sanitize_str(deal_id)}
            if file_name:
                filter_dict["file_name"] = {"$eq": sanitize_str(file_name)}
            
            if filter_dict:
                candidates = self._query_by_filter(filter_dict)
                if candidates:
                    return self._resolve_from_candidates(candidates, deal_id, file_name)
        
        # Fallback to scan if exact failed or match_mode is "scan"
        if match_mode == "scan" or (match_mode == "auto" and not filter_dict):
            candidates = self._scan_for_document(
                deal_id=deal_id,
                file_name=file_name,
                max_ids=scan_max_ids,
                batch_size=scan_batch_size
            )
            return self._resolve_from_candidates(candidates, deal_id, file_name)
        
        return None, []
    
    def _query_by_filter(self, filter_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query Pinecone with exact filter"""
        try:
            # Use $and if multiple conditions
            if len(filter_dict) > 1:
                filter_condition = {"$and": [filter_dict]}
            else:
                filter_condition = filter_dict
            
            results = self.index.query(
                vector=self._dummy_query_vector(),  # Dummy vector for filter-only query
                top_k=200,  # Get enough to find all chunks for a document
                namespace=self.namespace,
                include_metadata=True,
                filter=filter_condition
            )
            
            candidates = []
            seen_keys = set()
            
            for match in results.matches:
                metadata = match.metadata if hasattr(match, 'metadata') else {}
                
                # Build document key
                doc_key = self._build_document_key(metadata)
                if doc_key in seen_keys:
                    continue
                seen_keys.add(doc_key)
                
                candidates.append({
                    'document_key': doc_key,
                    'file_name': sanitize_str(metadata.get('file_name', '')),
                    'deal_id': sanitize_str(metadata.get('deal_id', '')),
                    'salesforce_deal_id': sanitize_str(metadata.get('salesforce_deal_id', '')),
                    'vendor_name': sanitize_str(metadata.get('vendor_name', '')),
                    'client_name': sanitize_str(metadata.get('client_name', '')),
                    'metadata': metadata
                })
            
            return candidates
        except Exception as e:
            print(f"⚠️  Query by filter failed: {e}")
            return []
    
    def _scan_for_document(
        self,
        deal_id: Optional[str] = None,
        file_name: Optional[str] = None,
        max_ids: int = 10000,
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Scan namespace to find document (bounded enumeration)"""
        print(f"🔍 Scanning namespace (max {max_ids} IDs)...")
        
        candidates = []
        seen_keys = set()
        total_processed = 0
        
        try:
            for vector_ids_batch in self.index.list(
                namespace=self.namespace,
                limit=batch_size
            ):
                if not vector_ids_batch:
                    break
                
                if isinstance(vector_ids_batch, str):
                    batch_ids = [vector_ids_batch]
                else:
                    batch_ids = list(vector_ids_batch)
                
                if not batch_ids:
                    continue
                
                # Fetch metadata
                fetch_results = self.index.fetch(
                    ids=batch_ids,
                    namespace=self.namespace
                )
                
                vectors_map = getattr(fetch_results, 'vectors', None) or fetch_results.get('vectors', {})
                
                for vector_id in batch_ids:
                    vector_data = vectors_map.get(vector_id, {})
                    metadata = getattr(vector_data, 'metadata', None)
                    if metadata is None and isinstance(vector_data, dict):
                        metadata = vector_data.get('metadata', {})
                    metadata = metadata or {}
                    
                    # Check if matches criteria
                    matches = True
                    if deal_id:
                        if sanitize_str(metadata.get('deal_id', '')) != sanitize_str(deal_id):
                            matches = False
                    if file_name:
                        if sanitize_str(metadata.get('file_name', '')) != sanitize_str(file_name):
                            matches = False
                    
                    if matches:
                        doc_key = self._build_document_key(metadata)
                        if doc_key not in seen_keys:
                            seen_keys.add(doc_key)
                            candidates.append({
                                'document_key': doc_key,
                                'file_name': sanitize_str(metadata.get('file_name', '')),
                                'deal_id': sanitize_str(metadata.get('deal_id', '')),
                                'salesforce_deal_id': sanitize_str(metadata.get('salesforce_deal_id', '')),
                                'vendor_name': sanitize_str(metadata.get('vendor_name', '')),
                                'client_name': sanitize_str(metadata.get('client_name', '')),
                                'metadata': metadata
                            })
                
                total_processed += len(batch_ids)
                
                if total_processed % 1000 == 0:
                    print(f"  Processed {total_processed:,} IDs, found {len(candidates)} candidates...")
                
                if total_processed >= max_ids:
                    break
                
                if candidates and (deal_id or file_name):
                    # If we have specific criteria and found matches, we can stop early
                    break
        
        except Exception as e:
            print(f"⚠️  Scan failed: {e}")
        
        return candidates
    
    def _build_document_key(self, metadata: Dict[str, Any]) -> str:
        """Build document key from metadata (deal_id + file_name preferred)"""
        deal_id = sanitize_str(metadata.get('deal_id', ''))
        file_name = sanitize_str(metadata.get('file_name', ''))
        
        if deal_id and file_name:
            return f"{deal_id}::{file_name}"
        elif file_name:
            return f"::{file_name}"
        elif deal_id:
            return f"{deal_id}::"
        else:
            # Fallback to other identifiers
            salesforce_deal_id = sanitize_str(metadata.get('salesforce_deal_id', ''))
            if salesforce_deal_id:
                return f"sf_{salesforce_deal_id}"
            return "unknown"
    
    def _resolve_from_candidates(
        self,
        candidates: List[Dict[str, Any]],
        deal_id: Optional[str],
        file_name: Optional[str]
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """Resolve unique document from candidates"""
        if not candidates:
            return None, []
        
        if len(candidates) == 1:
            return candidates[0], candidates
        
        # Multiple candidates - return first one but also return all for user to pick
        # In practice, we'll use the first match but log ambiguity
        print(f"⚠️  Found {len(candidates)} candidate documents, using first match")
        return candidates[0], candidates


class ChunkFetcher:
    """Fetch all chunks for a resolved document"""
    
    def __init__(self, pinecone_client: PineconeDocumentClient, namespace: str):
        self.client = pinecone_client
        self.namespace = namespace
        self.index = pinecone_client.index
        self._vector_dim: Optional[int] = None

    def _dummy_query_vector(self) -> List[float]:
        """Return a dummy vector of the correct dimension for filter-only Pinecone queries."""
        if self._vector_dim is None:
            try:
                stats = self.client.get_index_stats() or {}
                dim = stats.get("dimension")
                self._vector_dim = int(dim) if dim else 1024
            except Exception:
                self._vector_dim = 1024
        return [0.0] * int(self._vector_dim)
    
    def fetch_all_chunks(
        self,
        document_key: str,
        deal_id: Optional[str] = None,
        salesforce_deal_id: Optional[str] = None,
        file_name: Optional[str] = None,
        query_top_k: int = 1000,
        ensure_all: bool = False,
        scan_max_ids: int = 250000,
        scan_batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch chunks for a document, sorted by chunk_index.

        Notes:
        - Filtered `query()` has a top_k cap; for very large docs this can truncate.
        - If ensure_all=True, we detect likely truncation/gaps and fall back to list()+fetch() scan.
        """
        chunks = []
        
        # Build filter from document key components
        filter_dict: Dict[str, Any] = {}
        deal_id_s = sanitize_str(deal_id) if deal_id else ""
        sf_id_s = sanitize_str(salesforce_deal_id) if salesforce_deal_id else ""
        file_s = sanitize_str(file_name) if file_name else ""

        if deal_id_s:
            filter_dict["deal_id"] = {"$eq": deal_id_s}

        # IMPORTANT: across namespaces, the Salesforce deal id may live in either:
        # - metadata.salesforce_deal_id (newer schema)
        # - metadata.deal_id (older schema)
        # So when salesforce_deal_id is provided, match either field.
        if sf_id_s:
            filter_dict["$or"] = [
                {"salesforce_deal_id": {"$eq": sf_id_s}},
                {"deal_id": {"$eq": sf_id_s}},
            ]

        if file_s:
            filter_dict["file_name"] = {"$eq": file_s}
        
        if filter_dict:
            # Use query with filter.
            # Build $and list explicitly so "$or" can coexist cleanly with other fields.
            and_terms: List[Dict[str, Any]] = []
            for k, v in filter_dict.items():
                and_terms.append({k: v} if k != "$or" else {"$or": v})
            filter_condition = {"$and": and_terms} if len(and_terms) > 1 else and_terms[0]
            
            # Query with filter (note: Pinecone query has top_k limit, no pagination)
            # We use a conservative default and fall back to scan if we suspect truncation.
            top_k = int(query_top_k) if query_top_k else 200
            # Guardrail: Pinecone list() limit is <= 100, but query top_k is typically larger.
            # Still, prevent pathological values.
            if top_k < 1:
                top_k = 200
            if top_k > 10000:
                top_k = 10000
            seen_ids = set()
            
            # Single query (Pinecone doesn't support query pagination)
            # If document has >200 chunks, consider using scan mode
            results = self.index.query(
                vector=self._dummy_query_vector(),
                top_k=top_k,
                namespace=self.namespace,
                include_metadata=True,
                filter=filter_condition
            )
            
            for match in results.matches:
                if match.id in seen_ids:
                    continue
                seen_ids.add(match.id)
                
                metadata = match.metadata if hasattr(match, 'metadata') else {}
                
                # Extract text (try metadata.text first, then top-level)
                text = metadata.get('text', '')
                if not text and hasattr(match, 'text'):
                    text = match.text
                
                chunk_data = {
                    'id': match.id,
                    'text': text,
                    'chunk_index': metadata.get('chunk_index', -1),
                    'metadata': metadata,
                    'vector': None  # Will fetch separately if needed
                }
                
                # Try to get vector if available in match (unlikely, but check)
                if hasattr(match, 'values') and match.values:
                    chunk_data['vector'] = match.values
                
                chunks.append(chunk_data)
            
            # Warn if we hit the limit
            if len(chunks) >= top_k:
                print(f"⚠️  Warning: Retrieved {len(chunks)} chunks (may be limited by top_k={top_k})")

            # If requested, try to ensure we got *all* chunks for this doc.
            if ensure_all:
                need_scan = False
                if len(chunks) >= top_k:
                    need_scan = True
                else:
                    # Gap detection on chunk_index (best-effort)
                    idxs = [c.get("chunk_index") for c in chunks if isinstance(c.get("chunk_index"), int)]
                    idxs = [i for i in idxs if i >= 0]
                    if idxs:
                        s = set(idxs)
                        # If there are gaps between min and max, we likely missed some chunks.
                        if len(s) != (max(s) - min(s) + 1):
                            need_scan = True
                    else:
                        # Missing chunk_index values — treat as suspicious
                        need_scan = True

                if need_scan:
                    print("🔍 Falling back to scan mode to ensure all chunks are retrieved...")
                    chunks = self._scan_fetch_all_chunks(
                        deal_id=deal_id,
                        salesforce_deal_id=salesforce_deal_id,
                        file_name=file_name,
                        max_ids=scan_max_ids,
                        batch_size=scan_batch_size,
                    )
        
        # Sort by chunk_index
        chunks.sort(key=lambda x: x.get('chunk_index', 0))
        
        # Fetch vectors separately if needed (Pinecone query doesn't return vectors by default)
        if chunks:
            chunk_ids = [c['id'] for c in chunks]
            try:
                # Fetch vectors in batches
                batch_size = 100
                all_vectors = {}
                for i in range(0, len(chunk_ids), batch_size):
                    batch_ids = chunk_ids[i:i+batch_size]
                    fetch_result = self.index.fetch(ids=batch_ids, namespace=self.namespace)
                    vectors_map = getattr(fetch_result, 'vectors', None) or fetch_result.get('vectors', {})
                    for vid, vdata in vectors_map.items():
                        if hasattr(vdata, 'values'):
                            all_vectors[vid] = vdata.values
                        elif isinstance(vdata, dict) and 'values' in vdata:
                            all_vectors[vid] = vdata['values']
                
                # Attach vectors to chunks
                for chunk in chunks:
                    if chunk['id'] in all_vectors:
                        chunk['vector'] = all_vectors[chunk['id']]
            except Exception as e:
                print(f"⚠️  Could not fetch vectors: {e}")
        
        return chunks

    def _scan_fetch_all_chunks(
        self,
        deal_id: Optional[str],
        salesforce_deal_id: Optional[str],
        file_name: Optional[str],
        max_ids: int,
        batch_size: int,
    ) -> List[Dict[str, Any]]:
        """
        Guaranteed chunk collection path: enumerate IDs with list() + fetch() and filter client-side.
        This avoids query top_k truncation.
        """
        # Pinecone list() limit constraints: keep in [1, 100]
        bs = int(batch_size) if batch_size else 100
        if bs < 1:
            bs = 1
        if bs > 100:
            bs = 100

        deal_id_s = sanitize_str(deal_id) if deal_id else ""
        sf_id_s = sanitize_str(salesforce_deal_id) if salesforce_deal_id else ""
        file_s = sanitize_str(file_name) if file_name else ""

        matched: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()
        processed = 0

        for vector_ids_batch in self.index.list(namespace=self.namespace, limit=bs):
            if not vector_ids_batch:
                break

            if isinstance(vector_ids_batch, str):
                batch_ids = [vector_ids_batch]
            else:
                batch_ids = list(vector_ids_batch)

            fetch_results = self.index.fetch(ids=batch_ids, namespace=self.namespace)
            vectors_map = getattr(fetch_results, "vectors", None) or fetch_results.get("vectors", {})

            for vid in batch_ids:
                vdata = vectors_map.get(vid, {})
                md = getattr(vdata, "metadata", None)
                if md is None and isinstance(vdata, dict):
                    md = vdata.get("metadata", {})
                md = md or {}

                # Apply matching
                if file_s and sanitize_str(md.get("file_name", "")) != file_s:
                    continue
                if sf_id_s:
                    md_sf = sanitize_str(md.get("salesforce_deal_id", ""))
                    md_deal = sanitize_str(md.get("deal_id", ""))
                    # schema-adaptive match: salesforce id might live in either field
                    if md_sf != sf_id_s and md_deal != sf_id_s:
                        continue
                if deal_id_s and sanitize_str(md.get("deal_id", "")) != deal_id_s:
                    continue

                if vid in seen_ids:
                    continue
                seen_ids.add(vid)

                text = md.get("text", "")
                matched.append(
                    {
                        "id": vid,
                        "text": text,
                        "chunk_index": md.get("chunk_index", -1),
                        "metadata": md,
                        "vector": None,
                    }
                )

            processed += len(batch_ids)
            if processed >= int(max_ids):
                print(f"⚠️  Scan reached max_ids={max_ids:,} (may still be incomplete)")
                break

        return matched


class TextReconstructor:
    """Reconstruct full document text from chunks"""
    
    @staticmethod
    def reconstruct_text(chunks: List[Dict[str, Any]]) -> str:
        """Reconstruct full text from chunks with section markers"""
        if not chunks:
            return ""
        
        parts = []
        prev_section = None
        
        for chunk in chunks:
            text = chunk.get('text', '')
            if not text:
                continue
            
            metadata = chunk.get('metadata', {})
            section_name = sanitize_str(metadata.get('section_name', ''))
            chunk_type = sanitize_str(metadata.get('chunk_type', ''))
            chunk_index = chunk.get('chunk_index', -1)
            
            # Add section marker if changed
            if section_name and section_name != prev_section:
                parts.append(f"\n\n=== {section_name} ===\n")
                prev_section = section_name
            
            # Add chunk text
            parts.append(text)
            
            # Add separator between chunks (unless it's a table)
            if chunk_type != "table" and not text.strip().startswith("==="):
                parts.append("\n")
        
        return "".join(parts).strip()


class TextDiagnostics:
    """Compute text quality diagnostics for LLM usefulness"""
    
    @staticmethod
    def compute_diagnostics(text: str, chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute comprehensive text diagnostics"""
        if not text:
            return {
                'total_length': 0,
                'chunk_count': len(chunks),
                'empty_chunks_pct': 100.0,
                'repeated_lines_ratio': 0.0,
                'non_ascii_density': 0.0,
                'table_markers_count': 0,
                'section_headers_count': 0,
                'ocr_artifact_score': 0.0
            }
        
        # Basic stats
        total_length = len(text)
        chunk_count = len(chunks)
        empty_chunks = sum(1 for c in chunks if not c.get('text', '').strip())
        empty_chunks_pct = (empty_chunks / chunk_count * 100) if chunk_count > 0 else 0.0
        
        # Repeated lines (duplicate detection)
        lines = text.split('\n')
        unique_lines = set(lines)
        repeated_lines_ratio = 1.0 - (len(unique_lines) / len(lines)) if lines else 0.0
        
        # Non-ASCII density (OCR artifacts indicator)
        non_ascii_chars = sum(1 for c in text if ord(c) > 127)
        non_ascii_density = (non_ascii_chars / total_length * 100) if total_length > 0 else 0.0
        
        # Table markers (from semantic_chunker conventions)
        table_markers = len(re.findall(r'=== .+ ===', text))
        
        # Section headers
        section_headers = len(re.findall(r'^=== .+ ===$', text, re.MULTILINE))
        
        # OCR artifact heuristics
        # - Broken hyphenation (word- \n word)
        broken_hyphens = len(re.findall(r'\w+-\s*\n\s*\w+', text))
        # - Excessive whitespace
        excessive_whitespace = len(re.findall(r' {3,}', text))
        # - Mixed case artifacts (e.g., "ThIs Is OcR")
        mixed_case_lines = sum(1 for line in lines[:100] if _is_mixed_case_artifact(line))
        
        ocr_artifact_score = min(100.0, (
            (broken_hyphens / max(len(lines), 1) * 50) +
            (excessive_whitespace / max(len(lines), 1) * 30) +
            (mixed_case_lines / min(len(lines), 100) * 20)
        ))
        
        return {
            'total_length': total_length,
            'chunk_count': chunk_count,
            'empty_chunks_pct': round(empty_chunks_pct, 2),
            'repeated_lines_ratio': round(repeated_lines_ratio, 4),
            'non_ascii_density': round(non_ascii_density, 2),
            'table_markers_count': table_markers,
            'section_headers_count': section_headers,
            'ocr_artifact_score': round(ocr_artifact_score, 2),
            'broken_hyphens_count': broken_hyphens,
            'excessive_whitespace_count': excessive_whitespace
        }


def _is_mixed_case_artifact(line: str) -> bool:
    """Detect mixed case OCR artifacts (e.g., "ThIs Is OcR")"""
    if len(line) < 10:
        return False
    words = line.split()[:5]  # Check first 5 words
    if len(words) < 2:
        return False
    
    mixed_count = 0
    for word in words:
        if len(word) > 2:
            # Check for alternating case pattern
            has_upper = any(c.isupper() for c in word)
            has_lower = any(c.islower() for c in word)
            if has_upper and has_lower:
                # Check if it's not just proper noun capitalization
                if not word[0].isupper() or sum(1 for c in word[1:] if c.isupper()) > len(word) * 0.3:
                    mixed_count += 1
    
    return mixed_count >= 2


class EmbeddingComparator:
    """Compare embeddings between two document sets"""
    
    @staticmethod
    def compare_embeddings(
        left_chunks: List[Dict[str, Any]],
        right_chunks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Compare embeddings: compute centroid similarity if dimensions match"""
        
        # Extract vectors
        left_vectors = [c.get('vector') for c in left_chunks if c.get('vector')]
        right_vectors = [c.get('vector') for c in right_chunks if c.get('vector')]
        
        if not left_vectors or not right_vectors:
            return {
                'comparable': False,
                'reason': 'Missing vectors in one or both document sets'
            }
        
        # Check dimensions
        left_dim = len(left_vectors[0]) if left_vectors else 0
        right_dim = len(right_vectors[0]) if right_vectors else 0
        
        if left_dim != right_dim:
            return {
                'comparable': False,
                'reason': f'Dimension mismatch: {left_dim} vs {right_dim}',
                'left_dimension': left_dim,
                'right_dimension': right_dim
            }
        
        def dot(a: List[float], b: List[float]) -> float:
            return sum(x * y for x, y in zip(a, b))

        def norm(v: List[float]) -> float:
            return math.sqrt(sum(x * x for x in v))

        def mean_vector(vectors: List[List[float]]) -> List[float]:
            n = len(vectors)
            dim = len(vectors[0])
            acc = [0.0] * dim
            counted = 0
            for v in vectors:
                if len(v) != dim:
                    continue
                counted += 1
                for i, val in enumerate(v):
                    acc[i] += float(val)
            denom = max(counted, 1)
            return [x / denom for x in acc]

        # Compute centroids
        left_centroid = mean_vector(left_vectors)
        right_centroid = mean_vector(right_vectors)

        # Cosine similarity (centroid)
        norm_a = norm(left_centroid)
        norm_b = norm(right_centroid)
        centroid_similarity = 0.0
        if norm_a != 0.0 and norm_b != 0.0:
            centroid_similarity = dot(left_centroid, right_centroid) / (norm_a * norm_b)

        # Vector statistics
        left_norms = [norm(v) for v in left_vectors]
        right_norms = [norm(v) for v in right_vectors]
        
        # Check for duplicate vectors
        left_unique = len(set(tuple(v) for v in left_vectors))
        right_unique = len(set(tuple(v) for v in right_vectors))
        
        return {
            'comparable': True,
            'centroid_similarity': round(float(centroid_similarity), 4),
            'left_vector_count': len(left_vectors),
            'right_vector_count': len(right_vectors),
            'left_avg_norm': round(float(sum(left_norms) / max(len(left_norms), 1)), 4),
            'right_avg_norm': round(float(sum(right_norms) / max(len(right_norms), 1)), 4),
            'left_unique_vectors': left_unique,
            'right_unique_vectors': right_unique,
            'left_duplicate_ratio': round(1.0 - (left_unique / len(left_vectors)), 4) if left_vectors else 0.0,
            'right_duplicate_ratio': round(1.0 - (right_unique / len(right_vectors)), 4) if right_vectors else 0.0
        }


def generate_markdown_report(
    comparison_data: Dict[str, Any],
    output_dir: Path,
    save_reconstructed: bool = True
) -> str:
    """Generate human-readable Markdown report"""
    
    left_info = comparison_data.get('left', {})
    right_info = comparison_data.get('right', {})
    comparison = comparison_data.get('comparison', {})
    
    lines = []
    lines.append("# Pinecone Document Comparison Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().isoformat()}")
    lines.append("")
    
    # Resolution summary
    lines.append("## Document Resolution")
    lines.append("")
    lines.append("### Left Target")
    lines.append(f"- **Index:** {comparison_data.get('left_index')}")
    lines.append(f"- **Namespace:** {comparison_data.get('left_namespace')}")
    if left_info.get('resolved_doc'):
        doc = left_info['resolved_doc']
        lines.append(f"- **Document Key:** `{doc.get('document_key', 'N/A')}`")
        lines.append(f"- **File Name:** {doc.get('file_name', 'N/A')}")
        lines.append(f"- **Deal ID:** {doc.get('deal_id', 'N/A')}")
    else:
        lines.append("- **Status:** ❌ Not found")
    lines.append("")
    
    lines.append("### Right Target")
    lines.append(f"- **Index:** {comparison_data.get('right_index')}")
    lines.append(f"- **Namespace:** {comparison_data.get('right_namespace')}")
    if right_info.get('resolved_doc'):
        doc = right_info['resolved_doc']
        lines.append(f"- **Document Key:** `{doc.get('document_key', 'N/A')}`")
        lines.append(f"- **File Name:** {doc.get('file_name', 'N/A')}")
        lines.append(f"- **Deal ID:** {doc.get('deal_id', 'N/A')}")
    else:
        lines.append("- **Status:** ❌ Not found")
    lines.append("")
    
    # Text diagnostics comparison
    lines.append("## Text Quality Diagnostics")
    lines.append("")
    
    left_diag = left_info.get('diagnostics', {})
    right_diag = right_info.get('diagnostics', {})
    
    lines.append("| Metric | Left | Right | Difference |")
    lines.append("|--------|------|-------|------------|")
    
    metrics = [
        ('Total Length (chars)', 'total_length'),
        ('Chunk Count', 'chunk_count'),
        ('Empty Chunks %', 'empty_chunks_pct'),
        ('Repeated Lines Ratio', 'repeated_lines_ratio'),
        ('Non-ASCII Density %', 'non_ascii_density'),
        ('Table Markers', 'table_markers_count'),
        ('Section Headers', 'section_headers_count'),
        ('OCR Artifact Score', 'ocr_artifact_score')
    ]
    
    for label, key in metrics:
        left_val = left_diag.get(key, 0)
        right_val = right_diag.get(key, 0)
        diff = right_val - left_val
        diff_str = f"+{diff}" if diff >= 0 else str(diff)
        lines.append(f"| {label} | {left_val} | {right_val} | {diff_str} |")
    
    lines.append("")
    
    # Embedding comparison
    lines.append("## Embedding Comparison")
    lines.append("")
    
    emb_comp = comparison.get('embedding_comparison', {})
    if emb_comp.get('comparable'):
        lines.append("✅ **Embeddings are comparable**")
        lines.append("")
        lines.append(f"- **Centroid Similarity:** {emb_comp.get('centroid_similarity', 'N/A')}")
        lines.append(f"- **Left Vector Count:** {emb_comp.get('left_vector_count', 0)}")
        lines.append(f"- **Right Vector Count:** {emb_comp.get('right_vector_count', 0)}")
        lines.append(f"- **Left Avg Norm:** {emb_comp.get('left_avg_norm', 'N/A')}")
        lines.append(f"- **Right Avg Norm:** {emb_comp.get('right_avg_norm', 'N/A')}")
        lines.append(f"- **Left Duplicate Ratio:** {emb_comp.get('left_duplicate_ratio', 0)}")
        lines.append(f"- **Right Duplicate Ratio:** {emb_comp.get('right_duplicate_ratio', 0)}")
    else:
        lines.append("❌ **Embeddings not comparable**")
        lines.append(f"- **Reason:** {emb_comp.get('reason', 'Unknown')}")
    
    lines.append("")
    
    # Excerpts
    lines.append("## Text Excerpts")
    lines.append("")
    
    left_text = left_info.get('reconstructed_text', '')
    right_text = right_info.get('reconstructed_text', '')
    
    # Head excerpt
    lines.append("### Head (first 500 chars)")
    lines.append("")
    lines.append("**Left:**")
    lines.append("```")
    lines.append(left_text[:500] + ("..." if len(left_text) > 500 else ""))
    lines.append("```")
    lines.append("")
    lines.append("**Right:**")
    lines.append("```")
    lines.append(right_text[:500] + ("..." if len(right_text) > 500 else ""))
    lines.append("```")
    lines.append("")
    
    # Middle excerpt
    if len(left_text) > 1000 or len(right_text) > 1000:
        lines.append("### Middle (chars 1000-1500)")
        lines.append("")
        lines.append("**Left:**")
        lines.append("```")
        lines.append(left_text[1000:1500] + ("..." if len(left_text) > 1500 else ""))
        lines.append("```")
        lines.append("")
        lines.append("**Right:**")
        lines.append("```")
        lines.append(right_text[1000:1500] + ("..." if len(right_text) > 1500 else ""))
        lines.append("```")
        lines.append("")
    
    # Tail excerpt
    if len(left_text) > 500 or len(right_text) > 500:
        lines.append("### Tail (last 500 chars)")
        lines.append("")
        lines.append("**Left:**")
        lines.append("```")
        lines.append(left_text[-500:] if len(left_text) > 500 else left_text)
        lines.append("```")
        lines.append("")
        lines.append("**Right:**")
        lines.append("```")
        lines.append(right_text[-500:] if len(right_text) > 500 else right_text)
        lines.append("```")
        lines.append("")
    
    # Chunk IDs for traceability
    lines.append("## Chunk IDs (for traceability)")
    lines.append("")
    lines.append("### Left Chunks")
    left_chunks = left_info.get('chunks', [])
    if left_chunks:
        lines.append("| Chunk Index | Chunk ID | Text Length |")
        lines.append("|-------------|----------|-------------|")
        for chunk in left_chunks[:20]:  # Show first 20
            lines.append(f"| {chunk.get('chunk_index', 'N/A')} | `{chunk.get('id', 'N/A')}` | {len(chunk.get('text', ''))} |")
        if len(left_chunks) > 20:
            lines.append(f"| ... | ({len(left_chunks) - 20} more chunks) | |")
    else:
        lines.append("No chunks found.")
    lines.append("")
    
    lines.append("### Right Chunks")
    right_chunks = right_info.get('chunks', [])
    if right_chunks:
        lines.append("| Chunk Index | Chunk ID | Text Length |")
        lines.append("|-------------|----------|-------------|")
        for chunk in right_chunks[:20]:  # Show first 20
            lines.append(f"| {chunk.get('chunk_index', 'N/A')} | `{chunk.get('id', 'N/A')}` | {len(chunk.get('text', ''))} |")
        if len(right_chunks) > 20:
            lines.append(f"| ... | ({len(right_chunks) - 20} more chunks) | |")
    else:
        lines.append("No chunks found.")
    lines.append("")
    
    # Save reconstructed text files if requested
    if save_reconstructed:
        left_file = output_dir / "left_reconstructed.txt"
        right_file = output_dir / "right_reconstructed.txt"
        
        left_file.write_text(left_text, encoding='utf-8')
        right_file.write_text(right_text, encoding='utf-8')
        
        lines.append("## Reconstructed Text Files")
        lines.append("")
        lines.append(f"- **Left:** `{left_file.name}`")
        lines.append(f"- **Right:** `{right_file.name}`")
        lines.append("")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Compare documents between two Pinecone indexes/namespaces",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare by deal_id and file_name
  python scripts/compare_pinecone_targets.py \\
    --left-index npi-deal-data --left-namespace test-namespace \\
    --right-index npi-deal-data --right-namespace production-namespace \\
    --deal-id "58773" --file-name "contract.pdf"

  # Compare by file_name only (scan mode)
  python scripts/compare_pinecone_targets.py \\
    --left-index index1 --left-namespace ns1 \\
    --right-index index2 --right-namespace ns2 \\
    --file-name "document.pdf" --match-mode scan
        """
    )
    
    parser.add_argument('--left-index', type=str, required=True, help='Left Pinecone index name')
    parser.add_argument('--left-namespace', type=str, required=True, help='Left Pinecone namespace')
    parser.add_argument('--right-index', type=str, required=True, help='Right Pinecone index name')
    parser.add_argument('--right-namespace', type=str, required=True, help='Right Pinecone namespace')
    
    parser.add_argument('--deal-id', type=str, help='Deal ID to search for')
    parser.add_argument('--file-name', type=str, help='File name to search for')
    parser.add_argument('--document-name', type=str, help='Alias for --file-name')
    
    parser.add_argument('--match-mode', type=str, choices=['exact', 'scan', 'auto'], default='auto',
                       help='Match mode: exact (filter only), scan (enumerate), auto (try exact then scan)')
    parser.add_argument('--scan-max-ids', type=int, default=10000,
                       help='Maximum IDs to scan in scan mode (default: 10000)')
    parser.add_argument('--scan-batch-size', type=int, default=100,
                       help='Batch size for scanning (default: 100)')
    
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Output directory (default: output/compare_<timestamp>)')
    parser.add_argument('--no-reconstructed-files', action='store_true',
                       help='Do not save reconstructed text files')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not (args.deal_id or args.file_name or args.document_name):
        parser.error("Must provide at least one of: --deal-id, --file-name, or --document-name")
    
    # Initialize Pinecone
    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        print("❌ PINECONE_API_KEY not found in environment")
        sys.exit(1)
    
    # Create output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("output") / f"compare_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📊 Comparing documents between:")
    print(f"   Left:  {args.left_index} / {args.left_namespace}")
    print(f"   Right: {args.right_index} / {args.right_namespace}")
    print(f"📁 Output directory: {output_dir}")
    print()
    
    # Initialize clients
    left_client = PineconeDocumentClient(api_key=api_key, index_name=args.left_index)
    right_client = PineconeDocumentClient(api_key=api_key, index_name=args.right_index)
    
    # Resolve documents
    print("🔍 Resolving documents...")
    left_resolver = DocumentResolver(left_client, args.left_namespace)
    right_resolver = DocumentResolver(right_client, args.right_namespace)
    
    left_doc, left_candidates = left_resolver.resolve_document(
        deal_id=args.deal_id,
        file_name=args.file_name or args.document_name,
        match_mode=args.match_mode,
        scan_max_ids=args.scan_max_ids,
        scan_batch_size=args.scan_batch_size
    )
    
    right_doc, right_candidates = right_resolver.resolve_document(
        deal_id=args.deal_id,
        file_name=args.file_name or args.document_name,
        match_mode=args.match_mode,
        scan_max_ids=args.scan_max_ids,
        scan_batch_size=args.scan_batch_size
    )
    
    if not left_doc:
        print("❌ Could not resolve document in left target")
        sys.exit(1)
    
    if not right_doc:
        print("❌ Could not resolve document in right target")
        sys.exit(1)
    
    print(f"✅ Left document resolved: {left_doc.get('document_key')}")
    print(f"✅ Right document resolved: {right_doc.get('document_key')}")
    print()
    
    # Fetch chunks
    print("📥 Fetching chunks...")
    left_fetcher = ChunkFetcher(left_client, args.left_namespace)
    right_fetcher = ChunkFetcher(right_client, args.right_namespace)
    
    left_chunks = left_fetcher.fetch_all_chunks(
        document_key=left_doc['document_key'],
        deal_id=left_doc.get('deal_id'),
        file_name=left_doc.get('file_name')
    )
    
    right_chunks = right_fetcher.fetch_all_chunks(
        document_key=right_doc['document_key'],
        deal_id=right_doc.get('deal_id'),
        file_name=right_doc.get('file_name')
    )
    
    print(f"✅ Left: {len(left_chunks)} chunks")
    print(f"✅ Right: {len(right_chunks)} chunks")
    print()
    
    # Reconstruct text
    print("🔧 Reconstructing text...")
    left_text = TextReconstructor.reconstruct_text(left_chunks)
    right_text = TextReconstructor.reconstruct_text(right_chunks)
    
    print(f"✅ Left: {len(left_text):,} characters")
    print(f"✅ Right: {len(right_text):,} characters")
    print()
    
    # Compute diagnostics
    print("📊 Computing diagnostics...")
    left_diagnostics = TextDiagnostics.compute_diagnostics(left_text, left_chunks)
    right_diagnostics = TextDiagnostics.compute_diagnostics(right_text, right_chunks)
    
    # Compare embeddings
    print("🔍 Comparing embeddings...")
    embedding_comparison = EmbeddingComparator.compare_embeddings(left_chunks, right_chunks)
    
    # Build comparison data
    comparison_data = {
        'left_index': args.left_index,
        'left_namespace': args.left_namespace,
        'right_index': args.right_index,
        'right_namespace': args.right_namespace,
        'search_criteria': {
            'deal_id': args.deal_id,
            'file_name': args.file_name or args.document_name
        },
        'left': {
            'resolved_doc': left_doc,
            'chunks': [
                {
                    'id': c['id'],
                    'chunk_index': c.get('chunk_index', -1),
                    'text_length': len(c.get('text', ''))
                }
                for c in left_chunks
            ],
            'reconstructed_text': left_text,
            'diagnostics': left_diagnostics
        },
        'right': {
            'resolved_doc': right_doc,
            'chunks': [
                {
                    'id': c['id'],
                    'chunk_index': c.get('chunk_index', -1),
                    'text_length': len(c.get('text', ''))
                }
                for c in right_chunks
            ],
            'reconstructed_text': right_text,
            'diagnostics': right_diagnostics
        },
        'comparison': {
            'embedding_comparison': embedding_comparison
        }
    }
    
    # Write JSON
    json_file = output_dir / "comparison.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(comparison_data, f, indent=2, ensure_ascii=False)
    print(f"✅ JSON written: {json_file}")
    
    # Write Markdown
    md_file = output_dir / "comparison.md"
    md_content = generate_markdown_report(
        comparison_data,
        output_dir,
        save_reconstructed=not args.no_reconstructed_files
    )
    md_file.write_text(md_content, encoding='utf-8')
    print(f"✅ Markdown written: {md_file}")
    
    print()
    print("🎉 Comparison complete!")
    print(f"📁 Results in: {output_dir}")


if __name__ == "__main__":
    main()

