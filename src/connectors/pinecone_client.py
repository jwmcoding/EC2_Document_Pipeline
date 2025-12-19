"""
Enhanced Pinecone Client for Business Document Processing Pipeline
Adapted from proven working Quick_Check hybrid search implementation
"""

import os
import uuid
import logging
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Tuple, Generator
from pinecone import Pinecone
from pinecone.exceptions import PineconeApiException
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
import math


def _sanitize_str(value: Any, default: str = "") -> str:
    """
    Sanitize value to a clean string for Pinecone metadata.
    
    Converts None, NaN, 'None', 'nan', 'NaN' to empty string.
    This prevents polluting Pinecone metadata with string representations
    of null values that break filtering and look bad in search results.
    """
    if value is None:
        return default
    # Handle float NaN (math.isnan only works on floats)
    if isinstance(value, float) and math.isnan(value):
        return default
    # Convert to string and check for null-like strings
    str_val = str(value).strip()
    if str_val.lower() in ('none', 'nan', 'null', ''):
        return default
    return str_val


def _sanitize_numeric(value: Any, default: float = 0.0) -> float:
    """
    Sanitize numeric value for Pinecone metadata.
    
    Converts None, NaN, pandas NaN to default value.
    Pinecone cannot store NaN values in metadata.
    
    Args:
        value: Value to sanitize (can be float, int, str, pandas.NaN, None)
        default: Default value to return if value is invalid (default: 0.0)
        
    Returns:
        Valid float value safe for Pinecone metadata
    """
    if value is None:
        return default
    
    # Handle pandas NaN (try/except to avoid requiring pandas import)
    try:
        import pandas as pd
        if pd.isna(value):
            return default
    except (ImportError, TypeError):
        pass
    
    # Handle Python float NaN
    if isinstance(value, float) and math.isnan(value):
        return default
    
    # Try to convert to float
    try:
        result = float(value)
        # Double-check for NaN after conversion
        if math.isnan(result):
            return default
        return result
    except (ValueError, TypeError):
        return default


def _parse_date_to_unix_ts(value: Any) -> Optional[int]:
    """
    Parse common Salesforce/ISO date formats into a Unix timestamp (seconds).

    Pinecone metadata range filtering ($gt/$gte/$lt/$lte) only supports numeric
    fields. We therefore store both:
      - string date fields for display/debug, AND
      - numeric *_ts fields for reliable range filters.
    """
    if value is None:
        return None

    # Handle pandas/numpy NaN before any conversions
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except (ImportError, TypeError):
        pass
    
    # Handle Python float NaN
    if isinstance(value, float) and math.isnan(value):
        return None

    # Allow already-numeric timestamps (or numeric strings).
    if isinstance(value, (int, float)):
        ts_int = int(value)
        return ts_int if ts_int > 0 else None

    if not isinstance(value, str):
        value = str(value)

    s = value.strip()
    if not s or s.lower() in ("none", "nan", "null"):
        return None

    # Normalize a few common ISO variants so datetime.fromisoformat can handle them.
    # - Strip fractional seconds (Python stdlib ISO parser is picky in some forms)
    # - Convert trailing "Z" to "+00:00"
    # - Convert offsets like "+0000" to "+00:00"
    normalized = s
    normalized = re.sub(r"(\.\d+)(Z|[+-]\d{2}:?\d{2})$", r"\2", normalized)  # drop fractional seconds
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    normalized = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", normalized)  # +0000 -> +00:00

    # Try ISO first (covers YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, with/without tz).
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass

    # Try common Salesforce-ish / CSV-ish formats (naive -> assume UTC).
    for fmt in (
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            continue

    return None


@dataclass
class DocumentSearchResult:
    """Enhanced search result for business documents with reranking scores"""
    id: str
    text: str
    pinecone_score: float
    rerank_score: Optional[float] = None
    
    # File metadata
    document_path: str = ""
    file_name: str = ""
    file_type: str = ""
    file_size_mb: float = 0.0
    modified_time: str = ""
    
    # Business metadata
    year: str = ""
    week_number: int = 0
    week_date: str = ""
    vendor: str = ""
    client: str = ""
    deal_number: str = ""
    deal_name: str = ""
    
    # Salesforce Deal Metadata - CRITICAL FOR SEARCH
    deal_id: str = ""
    deal_subject: str = ""
    deal_status: str = ""
    deal_reason: str = ""
    deal_start_date: str = ""
    negotiated_by: str = ""
    
    # Financial Metrics (ENHANCED WITH MISSING FIELDS!)
    proposed_amount: float = 0.0
    final_amount: float = 0.0
    savings_1yr: float = 0.0
    savings_3yr: float = 0.0
    savings_target: float = 0.0
    savings_percentage: float = 0.0
    
    # MISSING CRITICAL SAVINGS FIELDS (From EDA Analysis!)
    savings_achieved: str = ""  # 90.9% populated - actual outcomes like "N - Time constraint"
    fixed_savings: float = 0.0  # 92.3% populated - actual savings amounts
    savings_target_full_term: float = 0.0  # Full contract term target
    final_amount_full_term: float = 0.0  # Full contract term final amount
    
    # Client/Vendor Information
    client_id: str = ""
    client_name: str = ""
    vendor_id: str = ""
    vendor_name: str = ""
    
    # Contract Information
    contract_term: str = ""
    contract_start: str = ""
    contract_end: str = ""
    effort_level: str = ""
    has_fmv_report: bool = False
    deal_origin: str = ""
    
    # Rich Narrative Content (CRITICAL MISSING FIELDS!)
    current_narrative: str = ""  # Analyst insights (88% populated)
    customer_comments: str = ""  # Customer communication advice (40% populated)  
    content_source: str = ""     # "document_file", "deal_narrative", "customer_comments"
    
    # Processing metadata
    chunk_index: int = 0
    parser: str = ""
    extraction_confidence: float = 0.0
    
    # Full metadata dict for compatibility
    metadata: Optional[Dict[str, Any]] = None


def setup_logger():
    """Configure logging with timestamp and formatting"""
    logger = logging.getLogger('PineconeDocumentClient')
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


class PineconeDocumentClient:
    """Enhanced Pinecone client for business document processing with proven hybrid search"""
    
    def __init__(self, api_key: str, index_name: str = "business-documents", environment: str = "us-east-1"):
        """Initialize Pinecone client using proven working pattern from Quick_Check"""
        self.pc = Pinecone(api_key=api_key)
        self.index_name = index_name
        self.environment = environment
        self.index = self.pc.Index(index_name)
        self.logger = setup_logger()
        
        self.logger.info(f"Initialized PineconeDocumentClient with index: {index_name}")
    
    def create_index_if_not_exists(self, dimension: int = 3072, metric: str = "cosine") -> bool:
        """Create Pinecone index if it doesn't exist"""
        try:
            # Check if index exists
            existing_indexes = [index.name for index in self.pc.list_indexes()]
            
            if self.index_name not in existing_indexes:
                from pinecone import ServerlessSpec
                
                self.pc.create_index(
                    name=self.index_name,
                    dimension=dimension,
                    metric=metric,
                    spec=ServerlessSpec(
                        cloud="aws",
                        region=self.environment
                    )
                )
                self.logger.info(f"Created index {self.index_name}")
                return True
            else:
                self.logger.info(f"Index {self.index_name} already exists")
                return True
                
        except Exception as e:
            self.logger.error(f"Error creating index: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _generate_embeddings(self, texts, input_type: str = None) -> Dict:
        """Generate dense and sparse embeddings using Pinecone's inference API with batch size limits.

        Args:
            texts: A single query string or a list of passage strings.
            input_type: Optional override. Use "query" for searches and "passage" for document writes.
                        If not provided, will auto-detect: string → "query", list → "passage".
        """
        try:
            # Convert single string to list for consistent processing
            if isinstance(texts, str):
                texts = [texts]
                inferred_input_type = "query"
            else:
                inferred_input_type = "passage"

            effective_input_type = input_type or inferred_input_type
                
            # Define conservative batch size limits and request size guard (tunable via env)
            DENSE_MODEL_BATCH_SIZE = int(os.getenv("EMBED_DENSE_BATCH", "24"))
            SPARSE_MODEL_BATCH_SIZE = int(os.getenv("EMBED_SPARSE_BATCH", "24"))
            # Max request body size guard in bytes (approx). Keep well under provider limits.
            MAX_REQUEST_BYTES = int(os.getenv("EMBED_MAX_REQUEST_BYTES", str(800_000)))
            
            dense_embeddings = []
            sparse_embeddings = []
            
            # Helper to partition by size as well as count
            def _yield_sized_batches(items: list, max_count: int) -> Generator[List[str], None, None]:
                start = 0
                while start < len(items):
                    end = min(start + max_count, len(items))
                    # shrink if estimated size too large
                    while end > start:
                        est_bytes = sum(len(s) for s in items[start:end])
                        if est_bytes <= MAX_REQUEST_BYTES:
                            break
                        end -= 1
                    if end == start:
                        # single very long item; hard cap by truncating input text defensively
                        single = items[start][: max(1000, int(MAX_REQUEST_BYTES * 0.5))]
                        yield [single]
                        start += 1
                    else:
                        yield items[start:end]
                        start = end

            # Process dense embeddings in batches with size guard and adaptive retry
            batch_index = 0
            for batch_texts in _yield_sized_batches(texts, DENSE_MODEL_BATCH_SIZE):
                batch_index += 1
                self.logger.debug(
                    f"Processing dense embedding batch {batch_index}: {len(batch_texts)} texts (total: {len(texts)})"
                )
                try:
                    dense_response = self.pc.inference.embed(
                        model="multilingual-e5-large",
                        inputs=batch_texts,
                        parameters={"input_type": effective_input_type}
                    )
                except Exception as e:
                    # Adaptive fallback for 413-size errors: halve batch and retry once
                    err_str = str(e).lower()
                    if "request entity too large" in err_str or "length limit" in err_str or "413" in err_str:
                        if len(batch_texts) > 1:
                            mid = len(batch_texts) // 2
                            for sub in (batch_texts[:mid], batch_texts[mid:]):
                                dense_response_sub = self.pc.inference.embed(
                                    model="multilingual-e5-large",
                                    inputs=sub,
                                    parameters={"input_type": effective_input_type}
                                )
                                dense_embeddings.extend([item['values'] for item in dense_response_sub])
                            continue
                    raise
                batch_dense_embeddings = [item['values'] for item in dense_response]
                dense_embeddings.extend(batch_dense_embeddings)
            
            # Process sparse embeddings in batches with same guards
            batch_index = 0
            for batch_texts in _yield_sized_batches(texts, SPARSE_MODEL_BATCH_SIZE):
                batch_index += 1
                self.logger.debug(
                    f"Processing sparse embedding batch {batch_index}: {len(batch_texts)} texts (total: {len(texts)})"
                )
                try:
                    sparse_response = self.pc.inference.embed(
                        model="pinecone-sparse-english-v0",
                        inputs=batch_texts,
                        parameters={"input_type": effective_input_type}
                    )
                except Exception as e:
                    err_str = str(e).lower()
                    if "request entity too large" in err_str or "length limit" in err_str or "413" in err_str:
                        if len(batch_texts) > 1:
                            mid = len(batch_texts) // 2
                            for sub in (batch_texts[:mid], batch_texts[mid:]):
                                sparse_response_sub = self.pc.inference.embed(
                                    model="pinecone-sparse-english-v0",
                                    inputs=sub,
                                    parameters={"input_type": effective_input_type}
                                )
                                for item in sparse_response_sub:
                                    sparse_indices = item.get('sparse_indices', [])
                                    sparse_values = item.get('sparse_values', [])
                                    if not sparse_indices or not sparse_values:
                                        sparse_vector = {'indices': [0], 'values': [0.01]}
                                    else:
                                        sparse_vector = {'indices': sparse_indices, 'values': sparse_values}
                                    sparse_embeddings.append(sparse_vector)
                            continue
                    raise
                
                # Extract embeddings from this batch
                batch_sparse_embeddings = []
                for item_idx, item in enumerate(sparse_response):
                    sparse_indices = item.get('sparse_indices', [])
                    sparse_values = item.get('sparse_values', [])
                    
                    # Validate sparse vector is not empty
                    if not sparse_indices or not sparse_values or len(sparse_indices) == 0 or len(sparse_values) == 0:
                        # Create fallback sparse vector for empty content
                        self.logger.warning(f"⚠️  Empty sparse vector detected for item {item_idx + 1}, using fallback sparse vector")
                        sparse_vector = {
                            'indices': [0],  # Use index 0 as fallback
                            'values': [0.01]  # Minimal value to satisfy Pinecone requirement
                        }
                    else:
                        sparse_vector = {
                            'indices': sparse_indices,
                            'values': sparse_values
                        }
                    
                    batch_sparse_embeddings.append(sparse_vector)
                sparse_embeddings.extend(batch_sparse_embeddings)
            
            self.logger.info(f"✅ Successfully generated embeddings for {len(texts)} texts "
                           f"(dense batches: {(len(texts) + DENSE_MODEL_BATCH_SIZE - 1) // DENSE_MODEL_BATCH_SIZE}, "
                           f"sparse batches: {(len(texts) + SPARSE_MODEL_BATCH_SIZE - 1) // SPARSE_MODEL_BATCH_SIZE})")
            
            return {
                'dense_embeddings': dense_embeddings,
                'sparse_embeddings': sparse_embeddings
            }
            
        except Exception as e:
            self.logger.error(f"Error generating embeddings: {str(e)}")
            raise

    def _truncate_enhanced_fields(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Safely truncate enhanced metadata fields to stay under limits."""
        truncated = dict(metadata)

        # Coerce None to empty string for known string fields to satisfy Pinecone constraints
        string_fields = {
            'classification_reasoning', 'vendor_name', 'client_name', 'deal_subject', 'deal_reason'
        }
        for key in list(truncated.keys()):
            if key in string_fields and truncated.get(key) is None:
                truncated[key] = ""
        # Mirror truncation logic used in upsert path
        if 'classification_reasoning' in truncated and truncated['classification_reasoning']:
            truncated['classification_reasoning'] = str(truncated['classification_reasoning'])[:1000]
        if 'vendor_name' in truncated and truncated['vendor_name']:
            truncated['vendor_name'] = str(truncated['vendor_name'])[:100]
        if 'client_name' in truncated and truncated['client_name']:
            truncated['client_name'] = str(truncated['client_name'])[:100]
        if 'deal_subject' in truncated and truncated['deal_subject']:
            truncated['deal_subject'] = str(truncated['deal_subject'])[:300]
        if 'deal_reason' in truncated and truncated['deal_reason']:
            truncated['deal_reason'] = str(truncated['deal_reason'])[:200]
        return truncated

    def update_enhanced_metadata_for_document(
        self,
        document_path: str,
        updates: Dict[str, Any],
        namespace: str = "documents",
        top_k: int = 200
    ) -> int:
        """Update existing vectors' metadata for a given document_path using set_metadata.

        This is used by the LLM enrichment flow to add pruned enhanced fields without re-embedding.

        Args:
            document_path: The unique document path used during upsert (stored as metadata.document_path)
            updates: Dict of metadata fields to set (e.g., document_type, depths, term dates, etc.)
            namespace: Pinecone namespace
            top_k: Maximum chunks to retrieve for the document

        Returns:
            Number of chunks successfully updated.
        """
        try:
            # Fetch all chunks for this document via filter-only query (bounded by reasonable top_k)
            results = self.index.query(
                namespace=namespace,
                top_k=top_k,
                vector=[0.0] * 1024,  # filter-only query
                include_metadata=True,
                filter={"document_path": {"$eq": document_path}}
            )

            matches = results.get('matches', []) if isinstance(results, dict) else getattr(results, 'matches', [])
            if not matches:
                self.logger.info(f"No chunks found for document_path='{document_path}' in namespace='{namespace}'")
                return 0

            updated = 0
            safe_updates = self._truncate_enhanced_fields(updates)

            for match in matches:
                try:
                    # Merge with existing metadata minimally on the server side using set_metadata
                    self.index.update(
                        id=getattr(match, 'id', match.get('id')),
                        namespace=namespace,
                        set_metadata=safe_updates
                    )
                    updated += 1
                except Exception as e:
                    self.logger.warning(f"Failed to update chunk metadata for id={getattr(match, 'id', '<unknown>')}: {e}")
                    continue

            self.logger.info(f"✅ Updated enhanced metadata for {updated}/{len(matches)} chunks | document_path='{document_path}'")
            return updated
        except Exception as e:
            self.logger.error(f"Error updating enhanced metadata for '{document_path}': {e}")
            return 0
    
    def _try_rerank(self, query: str, documents: List[str], top_n: int) -> Optional[List[Dict]]:
        """Attempt reranking with Cohere's model using Pinecone inference"""
        try:
            rerank_results = self.pc.inference.rerank(
                model="cohere-rerank-3.5",
                query=query,
                documents=documents[:top_n],
                top_n=top_n,
                return_documents=True
            )
            return rerank_results.data
        except Exception as e:
            self.logger.warning(f"Reranking failed: {str(e)}. Continuing with vector scores.")
            return None
    
    def _is_retryable_error(self, exception) -> bool:
        """Check if a Pinecone exception is retryable (rate limiting)"""
        if not isinstance(exception, PineconeApiException):
            return False
        
        error_str = str(exception).lower()
        retryable_indicators = [
            "429",  # HTTP 429 Too Many Requests
            "rate limit",
            "quota",
            "too many requests", 
            "upsert size limit",
            "max upsert size",
            "throttled",
            "request size",  # For "Request size 3MB exceeds the maximum supported size of 2MB"
            "exceeds the maximum",
            "metadata size"  # For "Metadata size is 40990 bytes, which exceeds the limit"
        ]
        
        # Non-retryable errors that should fail fast
        non_retryable_indicators = [
            "sparse vector must contain at least one value",  # Data validation errors
            "invalid sparse vector",
            "malformed vector"
        ]
        
        # Check for non-retryable errors first
        if any(indicator in error_str for indicator in non_retryable_indicators):
            return False
        
        return any(indicator in error_str for indicator in retryable_indicators)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=lambda retry_state: PineconeDocumentClient._is_retryable_error(None, retry_state.outcome.exception()) if retry_state.outcome.failed else False,
        reraise=True
    )
    def _upsert_vectors_with_retry(self, vectors: List[Dict], namespace: str) -> None:
        """Retry wrapper for Pinecone upsert operations to handle rate limiting"""
        try:
            result = self.index.upsert(vectors=vectors, namespace=namespace)
            self.logger.debug(f"✅ Upsert successful: {len(vectors)} vectors to namespace '{namespace}'")
            return result
        except PineconeApiException as e:
            # Check if this is a rate limiting error that should be retried
            if self._is_retryable_error(e):
                self.logger.warning(f"⏳ Rate limited during upsert - retrying... Error: {e}")
                raise  # Will trigger retry
            else:
                self.logger.error(f"❌ Non-retryable Pinecone error: {e}")
                raise  # Will not retry (and won't be retried due to retry condition)
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during upsert: {e}")
            raise  # Will not retry

    def upsert_chunks(self, chunks: List[Dict[str, Any]], namespace: str = "documents") -> bool:
        """Upload document chunks to Pinecone with enhanced business metadata and request size batching"""
        try:
            vectors = []
            for chunk in chunks:
                vector_id = chunk.get("id", str(uuid.uuid4()))
                
                # Extract enhanced metadata
                metadata = chunk["metadata"]

                deal_creation_date_ts = _parse_date_to_unix_ts(metadata.get("deal_creation_date"))
                contract_start_ts = _parse_date_to_unix_ts(metadata.get("contract_start"))
                contract_end_ts = _parse_date_to_unix_ts(metadata.get("contract_end"))
                
                # Prepare metadata for Pinecone - SIMPLIFIED 22-FIELD SCHEMA (Dec 2025)
                # Based on METADATA_SIMPLIFICATION_PLAN.md analysis
                # Reduced from 53 → 26 → 24 → 22 fields
                # Removed: client_id/vendor_id (duplicates), parser_backend/processing_method (internal only)
                # Use _sanitize_str to convert None/NaN/'None'/'nan' to empty strings
                # This prevents polluting Pinecone metadata with null-like string values
                pinecone_metadata = {
                    # ===== CORE DOCUMENT (3 fields) =====
                    "file_name": _sanitize_str(metadata.get("name"))[:200],  # Truncate to 200 chars
                    "file_type": _sanitize_str(metadata.get("file_type")),
                    "deal_creation_date": _sanitize_str(metadata.get("deal_creation_date")),
                    
                    # ===== IDENTIFIERS (4 fields) =====
                    "deal_id": _sanitize_str(metadata.get("deal_id")),
                    "salesforce_deal_id": _sanitize_str(metadata.get("salesforce_deal_id")),
                    "salesforce_client_id": _sanitize_str(metadata.get("salesforce_client_id")),
                    "salesforce_vendor_id": _sanitize_str(metadata.get("salesforce_vendor_id")),
                    
                    # ===== FINANCIAL (6 fields) =====
                    "final_amount": _sanitize_numeric(metadata.get("final_amount")),
                    "savings_1yr": _sanitize_numeric(metadata.get("savings_1yr")),
                    "savings_3yr": _sanitize_numeric(metadata.get("savings_3yr")),
                    "savings_achieved": _sanitize_str(metadata.get("savings_achieved"))[:200],
                    "fixed_savings": _sanitize_numeric(metadata.get("fixed_savings")),
                    "savings_target_full_term": _sanitize_numeric(metadata.get("savings_target_full_term")),
                    
                    # ===== CONTRACT (3 fields) =====
                    "contract_term": _sanitize_str(metadata.get("contract_term"))[:100],
                    "contract_start": _sanitize_str(metadata.get("contract_start")),
                    "contract_end": _sanitize_str(metadata.get("contract_end")),
                    
                    # ===== PROCESSING (1 field) =====
                    "chunk_index": int(_sanitize_numeric(metadata.get("chunk_index"), default=0)),
                    
                    # ===== SEARCH (2 fields) =====
                    "client_name": _sanitize_str(metadata.get("client_name"))[:100],
                    "vendor_name": _sanitize_str(metadata.get("vendor_name"))[:100],
                    
                    # ===== QUALITY (2 fields) =====
                    "has_parsing_errors": bool(len(metadata.get("parsing_errors", [])) > 0),
                    "deal_status": _sanitize_str(metadata.get("deal_status")),
                    
                    # ===== EMAIL (1 field) =====
                    "email_has_attachments": bool(metadata.get("email_has_attachments", False)),
                    
                    # ===== DEAL CLASSIFICATION (7 fields) - NEW December 2025 =====
                    "report_type": _sanitize_str(metadata.get("report_type"))[:100],
                    "project_type": _sanitize_str(metadata.get("project_type"))[:50],
                    "competition": _sanitize_str(metadata.get("competition"))[:10],
                    "npi_analyst": _sanitize_str(metadata.get("npi_analyst"))[:50],
                    "dual_multi_sourcing": _sanitize_str(metadata.get("dual_multi_sourcing"))[:10],
                    "time_pressure": _sanitize_str(metadata.get("time_pressure"))[:20],
                    "advisor_network_used": _sanitize_str(metadata.get("advisor_network_used"))[:10],
                    
                    # ===== TEXT CONTENT (1 field) =====
                    "text": _sanitize_str(metadata.get("text"))[:37000],
                }
                # Add Unix timestamp variants for reliable Pinecone range filters.
                # (Pinecone range operators only support numeric values.)
                if deal_creation_date_ts is not None:
                    pinecone_metadata["deal_creation_date_ts"] = int(deal_creation_date_ts)
                if contract_start_ts is not None:
                    pinecone_metadata["contract_start_ts"] = int(contract_start_ts)
                if contract_end_ts is not None:
                    pinecone_metadata["contract_end_ts"] = int(contract_end_ts)
                # TOTAL: 30 base fields + 3 conditional timestamps = 33 max
                # See memory-bank/CURRENT_METADATA_SCHEMA.md for full documentation
                # REMOVED in Dec 5: client_id, vendor_id (duplicates), parser_backend, processing_method
                # ADDED in Dec 7: 7 deal classification fields
                # REMOVED Dec 14: description (long text not suitable for filtering)
                # REMOVED earlier: document_path, file_size_mb, modified_time, week_number, week_date, 
                #          vendor, client, deal_number, deal_name, deal_subject, deal_reason, 
                #          deal_start_date, negotiated_by, proposed_amount, savings_target, 
                #          savings_percentage, final_amount_full_term, effort_level, 
                #          has_fmv_report, deal_origin, current_narrative, customer_comments, 
                #          content_source, email_subject, email_body_preview, extraction_confidence
                
                # Handle both old format (embedding) and new format (dense_embedding + sparse_embedding)
                if "embedding" in chunk:
                    # Old format - single embedding
                    vector_data = {
                        "id": vector_id,
                        "values": chunk["embedding"],
                        "metadata": pinecone_metadata
                    }
                elif "dense_embedding" in chunk:
                    # New format - hybrid embeddings
                    vector_data = {
                        "id": vector_id,
                        "values": chunk["dense_embedding"],
                        "metadata": pinecone_metadata
                    }
                    # Add sparse values if available
                    if "sparse_embedding" in chunk:
                        vector_data["sparse_values"] = chunk["sparse_embedding"]
                else:
                    raise ValueError(f"Chunk {vector_id} missing embedding data")
                
                # NOTE: Text is NOT stored in Pinecone (not in metadata, not top-level)
                # Text must be retrieved from source documents using metadata fields
                # (e.g., document_path, file_name, chunk_index)
                
                vectors.append(vector_data)
            
            # Implement request size batching to stay under 2MB limit
            MAX_REQUEST_SIZE_MB = 1.5  # Conservative limit with buffer
            ESTIMATED_VECTOR_SIZE_KB = 50  # Rough estimate per vector (embedding + metadata)
            MAX_VECTORS_PER_BATCH = int((MAX_REQUEST_SIZE_MB * 1024) / ESTIMATED_VECTOR_SIZE_KB)  # ~30 vectors
            
            if len(vectors) <= MAX_VECTORS_PER_BATCH:
                # Small batch - upload directly with retry logic
                self._upsert_vectors_with_retry(vectors=vectors, namespace=namespace)
                self.logger.info(f"✅ Upserted {len(vectors)} chunks to namespace '{namespace}' in single batch")
            else:
                # Large batch - split into smaller batches
                total_batches = (len(vectors) + MAX_VECTORS_PER_BATCH - 1) // MAX_VECTORS_PER_BATCH
                self.logger.info(f"🔄 Splitting {len(vectors)} chunks into {total_batches} batches (max {MAX_VECTORS_PER_BATCH} per batch)")
                
                for batch_num in range(total_batches):
                    start_idx = batch_num * MAX_VECTORS_PER_BATCH
                    end_idx = min((batch_num + 1) * MAX_VECTORS_PER_BATCH, len(vectors))
                    batch_vectors = vectors[start_idx:end_idx]
                    
                    self.logger.debug(f"Upserting batch {batch_num + 1}/{total_batches}: {len(batch_vectors)} vectors")
                    self._upsert_vectors_with_retry(vectors=batch_vectors, namespace=namespace)
                
                self.logger.info(f"✅ Successfully upserted {len(vectors)} chunks to namespace '{namespace}' in {total_batches} batches")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error upserting chunks: {e}")
            return False
    

    def hybrid_search_documents(
        self,
        query: str,
        filter_metadata: Optional[Dict] = None,
        top_k: int = 100,
        namespaces: List[str] = ["documents"],
        alpha: float = 0.60,
        rerank: bool = True,
        rerank_top_n: int = 20,
        include_business_filters: bool = True
    ) -> List[DocumentSearchResult]:
        """
        Perform hybrid search with business document focus using proven Quick_Check pattern
        
        Args:
            query: Search query
            filter_metadata: Pinecone filter conditions
            top_k: Number of results from vector search
            namespaces: List of namespaces to search
            alpha: Weight for dense vs sparse (0.60 = 60% dense, 40% sparse)
            rerank: Whether to apply reranking
            rerank_top_n: Number of results to rerank
            include_business_filters: Whether to add business-specific filtering
        """
        try:
            # Generate embeddings using proven method
            embeddings = self._generate_embeddings(query)
            dense_values = embeddings['dense_embeddings'][0]
            sparse_values = embeddings['sparse_embeddings'][0]
            
            # Apply hybrid scoring weights
            dense_values = [v * alpha for v in dense_values]
            sparse_values = {
                'indices': sparse_values['indices'],
                'values': [v * (1 - alpha) for v in sparse_values['values']]
            }
            
            # Prepare filters
            # NOTE (Dec 2025): We no longer force "year" or "document_type" to exist.
            # The current schema stores time as deal_creation_date and we intentionally
            # removed document_type from metadata to save space/cost. Callers should
            # pass explicit filters via `filter_metadata` when needed.
            final_filter = filter_metadata or {}
            
            # Search across namespaces
            all_results = []
            for namespace in namespaces:
                try:
                    results = self.index.query(
                        namespace=namespace,
                        top_k=top_k,
                        vector=dense_values,
                        sparse_vector=sparse_values,
                        include_metadata=True,
                        filter=final_filter if final_filter else None
                    )
                    all_results.extend(results['matches'])
                except Exception as e:
                    self.logger.error(f"Error searching namespace {namespace}: {str(e)}")
                    continue
            
            # Convert to DocumentSearchResult objects
            search_results = []
            documents = []
            
            for match in all_results:
                metadata = match.metadata
                # Text is NOT stored in Pinecone (by design)
                # - Removed from metadata to prevent 40KB limit errors
                # - Not stored at top level (Pinecone doesn't support this)
                # - Not needed: embeddings contain semantic information for search
                # - Frontend does not display chunk text
                text = ''
                
                documents.append(text)
                search_results.append(DocumentSearchResult(
                    id=match.id,
                    text=text,
                    pinecone_score=match.score,
                    
                    # File metadata
                    document_path=metadata.get('document_path', ''),
                    file_name=metadata.get('file_name', ''),
                    file_type=metadata.get('file_type', ''),
                    file_size_mb=metadata.get('file_size_mb', 0.0),
                    modified_time=metadata.get('modified_time', ''),
                    
                    # Business metadata
                    year=metadata.get('year', ''),
                    week_number=metadata.get('week_number', 0),
                    week_date=metadata.get('week_date', ''),
                    vendor=metadata.get('vendor', ''),
                    client=metadata.get('client', ''),
                    deal_number=metadata.get('deal_number', ''),
                    deal_name=metadata.get('deal_name', ''),
                    
                    # Salesforce Deal Metadata - CRITICAL FIX
                    deal_id=metadata.get('deal_id', ''),
                    deal_subject=metadata.get('deal_subject', ''),
                    deal_status=metadata.get('deal_status', ''),
                    deal_reason=metadata.get('deal_reason', ''),
                    deal_start_date=metadata.get('deal_start_date', ''),
                    negotiated_by=metadata.get('negotiated_by', ''),
                    
                    # Financial Metrics (ENHANCED WITH MISSING FIELDS!)
                    proposed_amount=metadata.get('proposed_amount', 0.0),
                    final_amount=metadata.get('final_amount', 0.0),
                    savings_1yr=metadata.get('savings_1yr', 0.0),
                    savings_3yr=metadata.get('savings_3yr', 0.0),
                    savings_target=metadata.get('savings_target', 0.0),
                    savings_percentage=metadata.get('savings_percentage', 0.0),
                    
                    # MISSING CRITICAL SAVINGS FIELDS (From EDA Analysis!)
                    savings_achieved=metadata.get('savings_achieved', ''),
                    fixed_savings=metadata.get('fixed_savings', 0.0),
                    savings_target_full_term=metadata.get('savings_target_full_term', 0.0),
                    final_amount_full_term=metadata.get('final_amount_full_term', 0.0),
                    
                    # Client/Vendor Information
                    client_id=metadata.get('client_id', ''),
                    client_name=metadata.get('client_name', ''),
                    vendor_id=metadata.get('vendor_id', ''),
                    vendor_name=metadata.get('vendor_name', ''),
                    
                    # Contract Information
                    contract_term=metadata.get('contract_term', ''),
                    contract_start=metadata.get('contract_start', ''),
                    contract_end=metadata.get('contract_end', ''),
                    effort_level=metadata.get('effort_level', ''),
                    has_fmv_report=metadata.get('has_fmv_report', False),
                    deal_origin=metadata.get('deal_origin', ''),
                    
                    # Rich Narrative Content (CRITICAL ADDITION!)
                    current_narrative=metadata.get('current_narrative', ''),
                    customer_comments=metadata.get('customer_comments', ''),
                    content_source=metadata.get('content_source', 'document_file'),
                    
                    # Processing metadata
                    chunk_index=metadata.get('chunk_index', 0),
                    parser=metadata.get('parser', ''),
                    extraction_confidence=metadata.get('extraction_confidence', 0.0),
                    
                    # Keep full metadata for compatibility
                    metadata=metadata
                ))
            
            # Apply reranking if requested using proven method
            if rerank and documents and len(search_results) > 0:
                rerank_results = self._try_rerank(query, documents, min(rerank_top_n, len(documents)))
                if rerank_results:
                    for i, rerank_result in enumerate(rerank_results):
                        if i < len(search_results):
                            search_results[i].rerank_score = rerank_result.score
                    
                    # Sort by rerank score
                    search_results = sorted(
                        search_results[:rerank_top_n],
                        key=lambda x: x.rerank_score or 0,
                        reverse=True
                    )
            
            self.logger.info(f"Hybrid search returned {len(search_results)} results for query: '{query[:50]}...'")
            return search_results
            
        except Exception as e:
            self.logger.error(f"Error in hybrid_search_documents: {str(e)}")
            return []
    
    def search_by_business_criteria(
        self,
        query: str,
        vendor: Optional[str] = None,
        client: Optional[str] = None,
        year: Optional[str] = None,
        week_number: Optional[int] = None,
        deal_number: Optional[str] = None,
        file_type: Optional[str] = None,
        document_type: Optional[str] = None,
        top_k: int = 50,
        rerank: bool = True
    ) -> List[DocumentSearchResult]:
        """
        Search documents with business-specific filtering including document type
        
        Args:
            document_type: Filter by LLM-classified document type (e.g., "Implementation Document (IDD)")
        """
        # Build filter
        filter_conditions = {}
        
        if vendor:
            filter_conditions["vendor"] = {"$eq": vendor}
        if client:
            filter_conditions["client"] = {"$eq": client}
        if year:
            filter_conditions["year"] = {"$eq": year}
        if week_number:
            filter_conditions["week_number"] = {"$eq": week_number}
        if deal_number:
            filter_conditions["deal_number"] = {"$eq": deal_number}
        if file_type:
            filter_conditions["file_type"] = {"$eq": file_type}
        if document_type:
            filter_conditions["document_type"] = {"$eq": document_type}
        
        return self.hybrid_search_documents(
            query=query,
            filter_metadata=filter_conditions,
            top_k=top_k,
            rerank=rerank
        )
    
    def search_by_document_type(
        self,
        document_type: str,
        query: str = "",
        top_k: int = 100,
        namespaces: List[str] = ["documents"],
        include_metadata: bool = True
    ) -> List[DocumentSearchResult]:
        """
        Search for documents of a specific type with optional text query
        
        Args:
            document_type: Document type to filter by (e.g., "Implementation Document (IDD)")
            query: Optional text query for semantic search within that document type
            top_k: Number of results to return
            namespaces: Namespaces to search
            include_metadata: Whether to include full metadata
            
        Returns:
            List of matching documents
        """
        # Create filter for specific document type
        type_filter = {"document_type": {"$eq": document_type}}
        
        if query.strip():
            # If query provided, do hybrid search with document type filter
            return self.hybrid_search_documents(
                query=query,
                filter_metadata=type_filter,
                top_k=top_k,
                namespaces=namespaces,
                rerank=True,
                rerank_top_n=min(top_k, 20)
            )
        else:
            # If no query, just filter by document type
            all_results = []
            for namespace in namespaces:
                try:
                    results = self.index.query(
                        namespace=namespace,
                        top_k=top_k,
                        vector=[0.0] * 1024,  # Dummy vector for filter-only search
                        include_metadata=include_metadata,
                        filter=type_filter
                    )
                    
                    for match in results.matches:
                        all_results.append(DocumentSearchResult(
                            id=match.id,
                            score=0.0,  # No relevance score for filter-only
                            metadata=match.metadata,
                            text=""  # Text NOT stored in Pinecone - retrieve from source if needed
                        ))
                except Exception as e:
                    self.logger.warning(f"Search failed in namespace {namespace}: {e}")
                    
            return all_results[:top_k]
    
    def get_document_type_distribution(self, namespace: str = "documents") -> Dict[str, int]:
        """
        Get distribution of document types (Note: Limited by Pinecone query capabilities)
        
        This is a simplified implementation - for full analytics, consider maintaining
        separate statistics or using a dashboard with aggregation capabilities.
        """
        self.logger.info("📊 Document type distribution requires aggregation - consider implementing analytics dashboard")
        return {
            "note": "Use search_by_document_type() to explore specific document types",
            "recommendation": "Implement analytics dashboard for full statistics"
        }
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get statistics about the index"""
        try:
            stats = self.index.describe_index_stats()
            return {
                "total_vector_count": stats.total_vector_count,
                "namespaces": dict(stats.namespaces) if stats.namespaces else {},
                "dimension": stats.dimension,
                "index_fullness": stats.index_fullness
            }
        except Exception as e:
            self.logger.error(f"Error getting index stats: {e}")
            return {}
    
    def delete_by_filter(self, filter_conditions: Dict[str, Any], namespace: str = "documents") -> bool:
        """Delete vectors matching filter conditions"""
        try:
            self.index.delete(filter=filter_conditions, namespace=namespace)
            self.logger.info(f"Deleted vectors matching filter in namespace '{namespace}'")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting by filter: {e}")
            return False 