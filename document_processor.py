"""
Main Document Processing Pipeline
Orchestrates all components for end-to-end document processing
"""

from typing import List, Dict, Any, Optional
import logging
import time
from pathlib import Path
from dataclasses import asdict
from datetime import datetime

# Import our pipeline components
from connectors.dropbox_client import DropboxClient
from ..models.document_models import DocumentMetadata
from connectors.pinecone_client import PineconeDocumentClient
from parsers.document_converter import DocumentConverter
from parsers.pdfplumber_parser import PDFPlumberParser, ParsedContent
try:
    from parsers.docling_parser import DoclingParser, is_docling_available
except ImportError:
    # Docling is an optional dependency; we only require it when the
    # parser_backend is explicitly set to "docling".
    DoclingParser = None  # type: ignore[assignment]
    def is_docling_available() -> bool:  # type: ignore[no-redef]
        return False
try:
    from parsers.mistral_parser import MistralParser, is_mistral_available
except ImportError:
    MistralParser = None  # type: ignore[assignment]
    def is_mistral_available() -> bool:  # type: ignore[no-redef]
        return False
from chunking.semantic_chunker import SemanticChunker, Chunk
from utils.discovery_cache import DiscoveryCache
try:
    from pipeline.processing_batch_manager import ProcessingBatchManager
except ImportError:
    # Will be imported when needed in process_discovered_documents.py
    ProcessingBatchManager = None
try:
    from src.config.colored_logging import ColoredLogger
except ImportError:
    # Fallback for direct execution
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from src.config.colored_logging import ColoredLogger
# from embeddings.embedding_service import EmbeddingService  # Not needed - using Pinecone embeddings


class DocumentProcessor:
    """Enhanced document processor with discovery caching and batch processing support"""
    
    def __init__(self, dropbox_client: DropboxClient, pinecone_client: PineconeDocumentClient,
                 llm_classifier=None, batch_manager=None, batch_mode: bool = False,
                 max_chunk_size: int = 1500, chunk_overlap: int = 200, 
                 enable_discovery_cache: bool = True, cache_save_interval: int = 50,
                 batch_submit_threshold: int | None = None,
                 openai_client: Any | None = None,
                 enable_vision_analysis: bool = False,
                 vision_model: str = "gpt-4o",
                 parser_backend: str = "pdfplumber"):
        """Initialize document processor with optional discovery caching and batch processing
        
        Args:
            llm_classifier: Enhanced LLM classifier for immediate processing
            batch_manager: ProcessingBatchManager for batch LLM processing
            batch_mode: If True, collect LLM requests for batch instead of immediate processing
            enable_discovery_cache: Enable persistent caching of discovery results
            cache_save_interval: Save cache every N documents during discovery
        """
        self.dropbox = dropbox_client
        self.pinecone = pinecone_client
        self.llm_classifier = llm_classifier  # v3: Enhanced LLM classifier
        self.batch_manager = batch_manager    # v3: Batch processing manager
        self.batch_mode = batch_mode          # v3: Batch collection mode
        self.batch_submit_threshold = batch_submit_threshold  # auto-submit threshold (requests)
        self.parser_backend = parser_backend.lower().strip() if parser_backend else "pdfplumber"

        base_logger = logging.getLogger(__name__)

        # Initialize document converter with optional VLM support for PowerPoint
        self.converter = DocumentConverter(
            openai_client=openai_client,
            enable_vision_analysis=enable_vision_analysis,
            vision_model=vision_model
        )
        
        # Select parser backend (pdfplumber by default; Docling or Mistral when requested and available)
        if self.parser_backend == "mistral":
            if MistralParser is None or not is_mistral_available():
                base_logger.warning(
                    "Mistral parser requested but Mistral is not configured. Falling back to PDFPlumber."
                )
                self.parser = PDFPlumberParser()
                self.parser_backend = "pdfplumber"
            else:
                try:
                    self.parser = MistralParser()
                    base_logger.info("DocumentProcessor initialized with Mistral OCR parser backend")
                except Exception as e:
                    base_logger.warning(
                        "Failed to initialize Mistral parser (%s). Falling back to PDFPlumber.",
                        e
                    )
                    self.parser = PDFPlumberParser()
                    self.parser_backend = "pdfplumber"
        elif self.parser_backend == "docling":
            if DoclingParser is None or not is_docling_available():
                base_logger.warning(
                    "Docling parser requested but Docling is not installed or unavailable. "
                    "Falling back to PDFPlumber."
                )
                self.parser = PDFPlumberParser()
                self.parser_backend = "pdfplumber"
            else:
                try:
                    self.parser = DoclingParser()
                    base_logger.info("DocumentProcessor initialized with Docling parser backend")
                except Exception as e:
                    base_logger.warning(
                        "Failed to initialize Docling parser (%s). Falling back to PDFPlumber.",
                        e
                    )
                    self.parser = PDFPlumberParser()
                    self.parser_backend = "pdfplumber"
        else:
            # Default / legacy behavior
            self.parser = PDFPlumberParser()
            self.parser_backend = "pdfplumber"
        
        self.chunker = SemanticChunker(max_chunk_size=max_chunk_size, overlap_size=chunk_overlap)
        
        # Discovery cache system
        self.enable_discovery_cache = enable_discovery_cache
        self.cache_save_interval = cache_save_interval
        self.discovery_cache = None
        
        # Batch processing collections
        self.enhancement_requests = []  # Collect requests for batch processing
        self.processed_documents = []   # Track processed documents for batch mapping
        
        self.logger = ColoredLogger("document_processor")
        
        # Processing statistics
        self.stats = {
            "documents_processed": 0,
            "documents_failed": 0,
            "documents_skipped": 0,
            "total_chunks_created": 0,
            "total_processing_time": 0.0,
            "total_size_processed_mb": 0.0,
            "batch_requests_collected": 0,
            "immediate_llm_calls": 0
        }
    
    def _init_discovery_cache(self, folder_path: str) -> None:
        """Initialize discovery cache for the given folder"""
        if not self.enable_discovery_cache:
            return
        
        # Create unique cache name based on folder
        cache_name = self.discovery_cache.get_folder_hash(folder_path) if hasattr(self, 'discovery_cache') and self.discovery_cache else None
        if not cache_name:
            cache_hash = folder_path.replace('/', '_').replace(' ', '_').lower()
            cache_name = f"folder_{cache_hash}"
        
        self.discovery_cache = DiscoveryCache(cache_name)
        self.logger.info(f"📁 Discovery cache ready: {cache_name}")

    def discover_documents_with_cache(self, folder_path: str, max_age_hours: int = 24) -> List[DocumentMetadata]:
        """Discover documents with caching support to prevent re-running expensive operations
        
        Args:
            folder_path: Target folder to process
            max_age_hours: Maximum age of cache to accept
            
        Returns:
            List of DocumentMetadata objects (from cache or fresh discovery)
        """
        
        if not self.enable_discovery_cache:
            self.logger.info("📂 Discovery cache disabled, running fresh discovery")
            return list(self.dropbox.list_documents(folder_path))
        
        # Initialize cache for this folder
        self._init_discovery_cache(folder_path)
        
        # Try to load from cache first
        cached_results = self.discovery_cache.load_discovery_results(max_age_hours)
        
        if cached_results and cached_results.get('folder_path') == folder_path:
            self.logger.success(f"🎉 Using cached discovery results: {cached_results['document_count']} documents")
            
            # Convert back to DocumentMetadata objects
            documents = []
            for doc_data in cached_results['documents']:
                # Reconstruct DocumentMetadata from dict
                doc = DocumentMetadata(**doc_data)
                documents.append(doc)
            
            return documents
        
        # No valid cache found, run fresh discovery with incremental saving
        self.logger.info("📂 Running fresh discovery with incremental caching...")
        return self._discover_with_incremental_cache(folder_path)
    
    def _discover_with_incremental_cache(self, folder_path: str) -> List[DocumentMetadata]:
        """Run discovery with incremental cache saves to prevent data loss"""
        documents = []
        save_counter = 0
        
        try:
            for doc_metadata in self.dropbox.list_documents(folder_path):
                documents.append(doc_metadata)
                save_counter += 1
                
                # Incremental save every N documents
                if save_counter >= self.cache_save_interval:
                    self.logger.info(f"💾 Incremental cache save: {len(documents)} documents")
                    self.discovery_cache.save_discovery_results(folder_path, documents, partial=True)
                    save_counter = 0
            
            # Final save with complete flag
            if documents:
                self.discovery_cache.save_discovery_results(folder_path, documents, partial=False)
                self.logger.success(f"✅ Discovery cache saved: {len(documents)} documents")
            
        except Exception as e:
            # Save whatever we have so far
            if documents:
                self.discovery_cache.save_discovery_results(folder_path, documents, partial=True)
                self.logger.warning(f"⚠️ Discovery interrupted, saved {len(documents)} documents to cache")
            raise e
        
        return documents

    def get_discovery_cache_info(self, folder_path: str) -> Optional[Dict[str, Any]]:
        """Get information about cached discovery results for a folder"""
        if not self.enable_discovery_cache:
            return None
        
        self._init_discovery_cache(folder_path)
        return self.discovery_cache.get_cache_info()

    def clear_discovery_cache(self, folder_path: str) -> bool:
        """Clear discovery cache for a specific folder"""
        if not self.enable_discovery_cache:
            return False
        
        self._init_discovery_cache(folder_path)
        return self.discovery_cache.clear_cache()

    def _get_optimized_metadata_dict(self, doc_metadata: DocumentMetadata) -> Dict[str, Any]:
        """Get the 27-field simplified metadata schema for Pinecone storage"""
        
        # 27-field simplified schema (57% reduction from 63 fields)
        optimized_fields = [
            # Core Document (3)
            'name', 'file_type', 'deal_creation_date',
            
            # Deal Context (6)
            'deal_id', 'salesforce_deal_id', 'deal_subject', 'deal_status', 'deal_reason', 'deal_start_date',
            
            # Business Relationships (6)
            'client_id', 'client_name', 'salesforce_client_id',
            'vendor_id', 'vendor_name', 'salesforce_vendor_id',
            
            # Financial Metrics (7)
            'proposed_amount', 'final_amount', 'savings_1yr', 'savings_3yr',
            'savings_target', 'savings_achieved', 'savings_target_full_term',
            
            # Rich Content (2)
            'current_narrative', 'customer_comments',
            
            # Email-Specific (2)
            'email_subject', 'email_has_attachments',
            
            # Technical (1)
            # chunk_index is added during chunking, not from DocumentMetadata
        ]
        
        # Extract EXACTLY the essential fields (preserve blanks for future enrichment)
        optimized_dict = {}
        for field in optimized_fields:
            if hasattr(doc_metadata, field):
                value = getattr(doc_metadata, field)
                optimized_dict[field] = value
            else:
                # Field doesn't exist in DocumentMetadata, set as None for future enrichment
                optimized_dict[field] = None
        
        # Verify we have the expected number of fields
        if len(optimized_dict) != len(optimized_fields):
            self.logger.warning(f"⚠️ Field count mismatch: expected {len(optimized_fields)}, got {len(optimized_dict)}. Fields: {list(optimized_dict.keys())}")
        
        return optimized_dict

    def process_document(self, doc_metadata: DocumentMetadata, namespace: str = "documents") -> Dict[str, Any]:
        """Process a single document through the complete pipeline"""
        
        start_time = time.time()
        result = {
            "success": False,
            "document_path": doc_metadata.path,
            "chunks_created": 0,
            "processing_time": 0.0,
            "errors": [],
            "metadata": asdict(doc_metadata)
        }
        
        try:
            self.logger.info(f"Starting processing: {doc_metadata.path}")
            
            # Step 1: Check if we can process this file type
            # Pass doc_metadata.name for extension detection when path lacks extension (Salesforce exports)
            if not self.converter.can_process(doc_metadata.path, doc_metadata.name):
                result["errors"].append(f"Unsupported file type: {doc_metadata.file_type}")
                self.stats["documents_skipped"] += 1
                return result
            
            # Step 2: Download document from Dropbox
            self.logger.debug(f"Downloading document: {doc_metadata.path}")
            content = self.dropbox.download_document(doc_metadata.path)
            
            # Step 2.5: Extract email metadata for .msg files
            if doc_metadata.file_type.lower() == '.msg':
                self.logger.debug(f"Extracting email metadata: {doc_metadata.path}")
                try:
                    email_metadata = self.converter.extract_msg_metadata(content, doc_metadata.path)
                    
                    # Update doc_metadata with email fields
                    for key, value in email_metadata.items():
                        setattr(doc_metadata, key, value)
                    
                    self.logger.info(f"📧 Email metadata extracted for {doc_metadata.name}: "
                                   f"From: {email_metadata.get('email_sender', 'Unknown')}, "
                                   f"Subject: {email_metadata.get('email_subject', 'No subject')[:50]}...")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to extract email metadata from {doc_metadata.path}: {e}")
                    # Continue processing even if email metadata extraction fails
            
            # Step 3: Convert to processable format
            # Pass doc_metadata.name for extension detection when path lacks extension (Salesforce exports)
            self.logger.debug(f"Converting document: {doc_metadata.path}")
            processed_content, content_type = self.converter.convert_to_processable_content(
                doc_metadata.path, content, doc_metadata.name
            )
            
            # Step 4: Parse with PDFPlumber
            self.logger.debug(f"Parsing document: {doc_metadata.path}")
            parsed_content = self.parser.parse(
                processed_content, 
                asdict(doc_metadata), 
                content_type
            )
            
            # Step 4.5: Enhanced LLM Classification (v3 Architecture)

            if self.llm_classifier or (self.batch_mode and self.batch_manager):
                if self.batch_mode and self.batch_manager:
                    # Batch Mode: Collect request for later batch processing
                    try:
                        self.logger.debug(f"Collecting for batch classification: {doc_metadata.path}")
                        
                        enhancement_request = self.batch_manager.collect_enhancement_request(
                            doc_metadata=doc_metadata,
                            content_preview=parsed_content.text[:4000],  # First 4K chars for efficiency
                            page_count=parsed_content.metadata.get('page_count'),
                            word_count=len(parsed_content.text.split()) if parsed_content.text else 0
                        )
                        
                        self.enhancement_requests.append(enhancement_request)
                        self.processed_documents.append({
                            'document_path': doc_metadata.path,
                            'request_index': len(self.enhancement_requests) - 1
                        })
                        
                        # Set placeholder values for batch processing
                        doc_metadata.classification_method = "batch_pending"
                        doc_metadata.classification_reasoning = "Collected for batch processing"
                        
                        self.stats["batch_requests_collected"] += 1
                        self.logger.info(f"📦 Collected for batch: {doc_metadata.name} (batch requests: {self.stats['batch_requests_collected']})")
                        # Auto-submit if threshold reached
                        try:
                            if self.batch_submit_threshold and len(self.enhancement_requests) >= self.batch_submit_threshold:
                                self.logger.info(f"🚀 Auto-submitting batch (threshold {self.batch_submit_threshold} reached)")
                                batch_job_id = self.submit_batch_classification()
                                if batch_job_id:
                                    self.logger.success(f"✅ Auto-submitted batch job: {batch_job_id}")
                                    # Clear collected requests for next batch window
                                    self.clear_batch_collections()
                                else:
                                    self.logger.error("❌ Auto-submit failed to create batch job")
                        except Exception as e:
                            self.logger.error(f"❌ Error during auto-submit: {e}")
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ Failed to collect batch request for {doc_metadata.name}: {e}")
                        doc_metadata.classification_method = "batch_collection_failed"
                        doc_metadata.classification_reasoning = f"Batch collection error: {str(e)}"
                        
                elif self.llm_classifier:
                    # Immediate Mode: Process classification right now
                    try:
                        self.logger.debug(f"Enhanced LLM classification: {doc_metadata.path}")
                        enhanced_classification = self.llm_classifier.classify_document_enhanced(
                            filename=doc_metadata.name,
                            content_preview=parsed_content.text,
                            file_type=doc_metadata.file_type,
                            vendor=doc_metadata.vendor or "",
                            client=doc_metadata.client or "",
                            deal_number=doc_metadata.deal_number or "",
                            page_count=parsed_content.metadata.get('page_count'),
                            word_count=len(parsed_content.text.split()) if parsed_content.text else 0
                        )
                        
                        # Update metadata with enhanced classification results
                        doc_metadata.document_type = enhanced_classification.document_type.value
                        doc_metadata.document_type_confidence = enhanced_classification.confidence
                        # reasoning removed from result to save tokens; no longer stored
                        doc_metadata.classification_method = enhanced_classification.classification_method
                        doc_metadata.classification_tokens_used = enhanced_classification.tokens_used
                        
                        # Store enhanced metadata (v3 features) - pruned summary
                        doc_metadata.product_pricing_depth = enhanced_classification.product_pricing_depth
                        doc_metadata.commercial_terms_depth = enhanced_classification.commercial_terms_depth
                        doc_metadata.proposed_term_start = enhanced_classification.proposed_term_start
                        doc_metadata.proposed_term_end = enhanced_classification.proposed_term_end
                        # key_topics, vendor_products_mentioned, pricing_indicators removed per pruning decision
                        
                        self.stats["immediate_llm_calls"] += 1
                        self.logger.info(f"📊 Enhanced classification complete: {doc_metadata.name}")
                        self.logger.debug(f"   Type: {enhanced_classification.document_type.value} ({enhanced_classification.confidence:.2f})")
                        # summary logging removed per pruning decision
                        self.logger.debug(f"   Pricing depth: {enhanced_classification.product_pricing_depth}")
                        self.logger.debug(f"   Terms depth: {enhanced_classification.commercial_terms_depth}")
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ Enhanced LLM classification failed for {doc_metadata.name}: {e}")
                        # Continue processing without classification - not critical
                        doc_metadata.classification_method = "failed"
                        doc_metadata.classification_reasoning = f"Enhanced classification error: {str(e)}"
            else:
                self.logger.debug(f"Enhanced LLM classification not available for {doc_metadata.name}")
            
            # Step 5: Create semantic chunks
            self.logger.debug(f"Creating chunks: {doc_metadata.path}")
            
            # Only pass essential metadata to chunker (not all parsed_content.metadata)
            essential_chunking_metadata = {
                'document_name': doc_metadata.name,
                'file_type': doc_metadata.file_type,
                'document_path': doc_metadata.path
            }
            
            chunks = self.chunker.chunk_document(
                parsed_content.text, 
                essential_chunking_metadata
            )
            
            # Direct fallback: for short docs or no chunks, create a single bounded full-text chunk
            text_len = len(parsed_content.text or "")
            if (not chunks) or (text_len <= 1500):
                if parsed_content.text:
                    self.logger.warning("Using single bounded full-text chunk (short or unchunkable doc)")
                    bounded_text = parsed_content.text[: max(500, min(4000, text_len)) ]
                    single_chunk = Chunk(
                        text=bounded_text,
                        metadata={
                            **essential_chunking_metadata,
                            'chunk_index': 0,
                            'section_name': 'content',
                            'chunk_type': 'general'
                        },
                        start_index=0,
                        end_index=len(bounded_text)
                    )
                    chunks = [single_chunk]
            
            if not chunks:
                result["errors"].append("No chunks created from document")
                self.stats["documents_failed"] += 1
                return result
            
            # Step 6: Prepare chunks for embedding
            chunk_data = []
            for chunk in chunks:
                chunk_data.append({
                    'text': chunk.text,
                    'metadata': chunk.metadata,
                    'id': f"{doc_metadata.path}_{chunk.metadata.get('chunk_index', 0)}"
                })
            
            # Step 7: Generate embeddings using Pinecone service (1024D)
            self.logger.debug(f"Generating embeddings: {doc_metadata.path}")
            
            # Extract texts for embedding
            chunk_texts = [chunk['text'] for chunk in chunk_data]
            
            # Use Pinecone's embedding service for correct dimensions
            embeddings_result = self.pinecone._generate_embeddings(chunk_texts)
            
            # Prepare chunks with Pinecone embeddings
            embedded_chunks = []
            
            # Convert DocumentMetadata to dict and add parser_backend
            doc_metadata_dict = asdict(doc_metadata)
            doc_metadata_dict['parser_backend'] = self.parser_backend  # Add parser backend info
            
            for i, chunk in enumerate(chunk_data):
                # Create merged metadata with only essential chunk fields + our 27 optimized fields
                chunk_metadata = chunk['metadata']
                
                # Only include essential chunk fields (not all chunk metadata)
                essential_chunk_fields = {
                    'chunk_index': chunk_metadata.get('chunk_index', i),
                    'section_name': chunk_metadata.get('section_name', 'content'),
                    'chunk_type': chunk_metadata.get('chunk_type', 'general')
                    # NOTE: chunk_length removed - can be calculated from text if needed
                }
                
                merged_metadata = {
                    **essential_chunk_fields,  # Only 4 essential chunk fields
                    **doc_metadata_dict,       # Our 27 optimized document fields
                    # REMOVED: 'text' field - stored at top level of Pinecone record, not in metadata
                    # This prevents metadata size limit errors (40KB limit)
                }
                
                embedded_chunk = {
                    'id': chunk['id'],
                    'text': chunk['text'],  # FIXED: Text at top level for Pinecone compatibility
                    'dense_embedding': embeddings_result['dense_embeddings'][i],
                    'sparse_embedding': embeddings_result['sparse_embeddings'][i],
                    'metadata': merged_metadata
                }
                embedded_chunks.append(embedded_chunk)
            
            # Step 8: Upload to Pinecone
            self.logger.debug(f"Uploading to Pinecone: {doc_metadata.path}")
            upload_success = self.pinecone.upsert_chunks(embedded_chunks, namespace)
            
            if not upload_success:
                result["errors"].append("Failed to upload chunks to Pinecone")
                self.stats["documents_failed"] += 1
                return result
            
            # Success!
            processing_time = time.time() - start_time
            result.update({
                "success": True,
                "chunks_created": len(chunks),
                "processing_time": processing_time,
                "content_type": content_type,
                "parser_used": parsed_content.metadata.get("parser", "unknown"),
                "total_pages": parsed_content.metadata.get("total_pages", 0),
                "total_tables": parsed_content.metadata.get("total_tables", 0),
                "text_length": len(parsed_content.text),
                "embedding_model": "pinecone_multilingual_e5_large"  # FIXED: Using Pinecone embeddings
            })
            
            # Update statistics
            self.stats["documents_processed"] += 1
            self.stats["total_chunks_created"] += len(chunks)
            self.stats["total_processing_time"] += processing_time
            self.stats["total_size_processed_mb"] += doc_metadata.size_mb
            
            self.logger.info(f"Successfully processed {doc_metadata.path}: "
                           f"{len(chunks)} chunks in {processing_time:.2f}s")
            
        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Error processing {doc_metadata.path}: {str(e)}"
            self.logger.error(error_msg)
            
            result.update({
                "errors": [error_msg],
                "processing_time": processing_time
            })
            self.stats["documents_failed"] += 1
        
        return result
    
    def process_folder(self, folder_path: str, namespace: str = "documents", 
                      max_documents: int = None, file_types: List[str] = None,
                      use_discovery_cache: bool = True) -> Dict[str, Any]:
        """Process all documents in a Dropbox folder with discovery caching support
        
        ENHANCED: Now includes discovery caching to prevent data loss from failures
        
        Args:
            use_discovery_cache: Use cached discovery results if available
        """
        
        start_time = time.time()
        results = {
            "folder_path": folder_path,
            "namespace": namespace,
            "summary": {
                "total_found": 0,
                "processed": 0,
                "failed": 0,
                "skipped": 0,
                "total_chunks": 0,
                "processing_time": 0.0,
                "discovery_cached": False,
                "cache_age_hours": 0
            },
            "documents": [],
            "errors": []
        }
        
        # FOLDER PATH VALIDATION - CRITICAL BUG FIX
        if not folder_path or folder_path.strip() == "":
            error_msg = (
                "🚨 CRITICAL ERROR: Empty or blank folder path detected! "
                "This would process the ENTIRE Dropbox root directory instead of the target folder. "
                "Expected a specific folder path like '/NPI Data Ownership/2024 Deal Docs'. "
                "This is likely a configuration error."
            )
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            return results
        
        if not isinstance(folder_path, str):
            error_msg = (
                f"🚨 CRITICAL ERROR: Invalid folder path type: {type(folder_path)}. "
                f"Expected string, got {type(folder_path)}. "
                f"Make sure folder_path is a string like '/NPI Data Ownership/2024 Deal Docs'"
            )
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            return results
        
        if not folder_path.startswith('/'):
            error_msg = (
                f"🚨 CRITICAL ERROR: Folder path '{folder_path}' must start with '/'. "
                f"Expected format: '/NPI Data Ownership/2024 Deal Docs'"
            )
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            return results
        
        try:
            self.logger.info(f"Starting folder processing: {folder_path}")
            
            # Use discovery cache if enabled
            if use_discovery_cache and self.enable_discovery_cache:
                self.logger.info("🔍 Checking for cached discovery results...")
                documents = self.discover_documents_with_cache(folder_path)
                
                # Check if results came from cache
                cache_info = self.get_discovery_cache_info(folder_path)
                if cache_info:
                    results["summary"]["discovery_cached"] = True
                    results["summary"]["cache_age_hours"] = cache_info.get("age_hours", 0)
            else:
                # Traditional discovery without caching
                self.logger.info("📂 Running discovery without cache...")
                documents = list(self.dropbox.list_documents(folder_path))
            
            results["summary"]["total_found"] = len(documents)
            
            if not documents:
                self.logger.warning(f"No documents found in folder: {folder_path}")
                return results
            
            # Additional safety check: verify all documents are from expected folder
            wrong_folder_docs = [doc for doc in documents if not doc.path.startswith(folder_path)]
            if wrong_folder_docs:
                error_msg = (
                    f"🚨 SAFETY ERROR: Found {len(wrong_folder_docs)} documents outside target folder! "
                    f"This indicates a serious bug. Target: {folder_path}"
                )
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                # Log first few wrong paths for debugging
                for doc in wrong_folder_docs[:5]:
                    self.logger.error(f"   Wrong path: {doc.path}")
                return results
            
            # Filter by file types if specified
            if file_types:
                documents = [doc for doc in documents 
                           if doc.file_type.lower() in [ft.lower() for ft in file_types]]
                self.logger.info(f"Filtered to {len(documents)} documents by file type")
            
            # Limit number of documents if specified
            if max_documents:
                documents = documents[:max_documents]
                self.logger.info(f"Limited to {len(documents)} documents")
            
            # Process each document
            for i, doc_metadata in enumerate(documents, 1):
                self.logger.info(f"Processing document {i}/{len(documents)}: {doc_metadata.name}")
                
                try:
                    # Process individual document
                    doc_result = self.process_document(doc_metadata, namespace)
                    results["documents"].append(doc_result)
                    
                    # Update summary
                    if doc_result["success"]:
                        results["summary"]["processed"] += 1
                        results["summary"]["total_chunks"] += doc_result["chunks_created"]
                    else:
                        results["summary"]["failed"] += 1
                    
                    # Log progress
                    if i % 10 == 0 or i == len(documents):
                        self.logger.info(f"Progress: {i}/{len(documents)} documents processed")
                
                except Exception as e:
                    error_msg = f"Unexpected error processing {doc_metadata.path}: {str(e)}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    results["summary"]["failed"] += 1
            
            # Calculate skipped documents
            results["summary"]["skipped"] = (results["summary"]["total_found"] - 
                                           results["summary"]["processed"] - 
                                           results["summary"]["failed"])
            
            processing_time = time.time() - start_time
            results["summary"]["processing_time"] = processing_time
            
            self.logger.info(f"Folder processing complete: {folder_path}")
            self.logger.info(f"Summary: {results['summary']['processed']} processed, "
                           f"{results['summary']['failed']} failed, "
                           f"{results['summary']['skipped']} skipped, "
                           f"{results['summary']['total_chunks']} chunks in {processing_time:.2f}s")
            
        except Exception as e:
            error_msg = f"Error processing folder {folder_path}: {str(e)}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
        
        return results
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics and capabilities"""
        
        # Get component capabilities
        converter_info = self.converter.get_file_info("dummy.pdf")  # Just to get supported types
        pinecone_stats = self.pinecone.get_index_stats()
        
        return {
            "pipeline_stats": dict(self.stats),
            "capabilities": {
                "supported_file_types": list(self.converter.supported_extensions.keys()),
                "parser": "pdfplumber",
                "chunking": "semantic_with_business_context",
                "embedding_model": "pinecone_multilingual_e5_large",
                "embedding_dimension": 1024,
                "vector_store": "pinecone_hybrid_search",
                "batch_processing": self.batch_mode
            },
            "component_stats": {
                "pinecone_index": pinecone_stats,
                "embedding_service": {
                    "model": "pinecone_multilingual_e5_large", 
                    "dimension": 1024,
                    "provider": "pinecone_inference_api"
                }
            },
            "performance_metrics": {
                "avg_processing_time_per_doc": (self.stats["total_processing_time"] / 
                                               max(self.stats["documents_processed"], 1)),
                "avg_chunks_per_doc": (self.stats["total_chunks_created"] / 
                                     max(self.stats["documents_processed"], 1)),
                "total_size_processed_mb": self.stats["total_size_processed_mb"],
                "success_rate": (self.stats["documents_processed"] / 
                               max(self.stats["documents_processed"] + self.stats["documents_failed"], 1)),
                "batch_requests_collected": self.stats.get("batch_requests_collected", 0),
                "immediate_llm_calls": self.stats.get("immediate_llm_calls", 0)
            }
        }
    
    # Batch Processing Methods (v3 Architecture)
    
    def submit_batch_classification(self, batch_id: Optional[str] = None) -> Optional[str]:
        """Submit collected enhancement requests for batch processing"""
        if not self.batch_manager or not self.enhancement_requests:
            self.logger.warning("⚠️ No batch manager or enhancement requests available")
            return None
        
        if not batch_id:
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Estimate costs before submission
            cost_estimate = self.batch_manager.estimate_batch_cost(len(self.enhancement_requests))
            
            self.logger.info(f"💰 Batch Cost Estimation for {len(self.enhancement_requests)} documents:")
            self.logger.info(f"   Batch API: ${cost_estimate['batch_cost']:.2f}")
            self.logger.info(f"   Immediate API: ${cost_estimate['immediate_cost']:.2f}")
            self.logger.info(f"   Savings: ${cost_estimate['savings']:.2f} ({cost_estimate['savings_percentage']:.1f}%)")
            
            # Submit batch job
            batch_job_id = self.batch_manager.create_enhanced_classification_batch(
                self.enhancement_requests, 
                batch_id
            )
            
            self.logger.success(f"✅ Batch job submitted: {batch_job_id}")
            self.logger.info(f"📊 Documents queued: {len(self.enhancement_requests)}")
            self.logger.info(f"⏰ Results available within 24 hours")
            
            return batch_job_id
            
        except Exception as e:
            self.logger.error(f"❌ Failed to submit batch job: {e}")
            return None
    
    def get_batch_requests_info(self) -> Dict[str, Any]:
        """Get information about collected batch requests"""
        return {
            "total_requests": len(self.enhancement_requests),
            "processed_documents": len(self.processed_documents),
            "batch_mode": self.batch_mode,
            "has_batch_manager": self.batch_manager is not None,
            "requests_ready_for_submission": len(self.enhancement_requests) > 0
        }
    
    def clear_batch_collections(self):
        """Clear collected batch requests (call after submission)"""
        self.enhancement_requests = []
        self.processed_documents = []
        self.stats["batch_requests_collected"] = 0
        self.logger.info("🧹 Cleared batch collections")
    
    def search_processed_documents(self, query: str, 
                                  filter_criteria: Dict[str, Any] = None,
                                  top_k: int = 10,
                                  namespace: str = "documents") -> List[Dict[str, Any]]:
        """Search through processed documents using the business criteria"""
        
        self.logger.info(f"Searching documents: '{query}' with filters: {filter_criteria}")
        
        # Use business criteria search if filters provided
        if filter_criteria:
            results = self.pinecone.search_by_business_criteria(
                query=query,
                vendor=filter_criteria.get("vendor"),
                client=filter_criteria.get("client"),
                year=filter_criteria.get("year"),
                week_number=filter_criteria.get("week_number"),
                deal_number=filter_criteria.get("deal_number"),
                file_type=filter_criteria.get("file_type"),
                top_k=top_k
            )
        else:
            # Use general hybrid search
            results = self.pinecone.hybrid_search_documents(
                query=query,
                top_k=top_k,
                namespaces=[namespace]
            )
        
        # Convert to simpler format
        search_results = []
        for result in results:
            search_results.append({
                "score": result.pinecone_score,
                "rerank_score": result.rerank_score,
                "text": result.text[:500] + "..." if len(result.text) > 500 else result.text,
                "document_path": result.document_path,
                "file_name": result.file_name,
                "vendor": result.vendor,
                "client": result.client,
                "year": result.year,
                "week_number": result.week_number,
                "deal_number": result.deal_number,
                "file_type": result.file_type,
                "chunk_index": result.chunk_index
            })
        
        self.logger.info(f"Found {len(search_results)} matching results")
        return search_results
    
    def validate_pipeline_health(self) -> Dict[str, Any]:
        """Validate that all pipeline components are working correctly"""
        
        health_status = {
            "overall_healthy": True,
            "component_status": {},
            "warnings": [],
            "errors": []
        }
        
        try:
            # Test Dropbox connection
            try:
                # Try to list a small folder
                list(self.dropbox.list_documents(""))
                health_status["component_status"]["dropbox"] = "healthy"
            except Exception as e:
                health_status["component_status"]["dropbox"] = "error"
                health_status["errors"].append(f"Dropbox connection failed: {str(e)}")
                health_status["overall_healthy"] = False
            
            # Test Pinecone connection
            try:
                stats = self.pinecone.get_index_stats()
                if stats:
                    health_status["component_status"]["pinecone"] = "healthy"
                else:
                    health_status["component_status"]["pinecone"] = "warning"
                    health_status["warnings"].append("Pinecone stats unavailable")
            except Exception as e:
                health_status["component_status"]["pinecone"] = "error"
                health_status["errors"].append(f"Pinecone connection failed: {str(e)}")
                health_status["overall_healthy"] = False
            
            # Test OpenAI embeddings
            try:
                test_embedding = self.embeddings.embed_text("test")
                if len(test_embedding) == self.embeddings.dimension:
                    health_status["component_status"]["embeddings"] = "healthy"
                else:
                    health_status["component_status"]["embeddings"] = "error"
                    health_status["errors"].append("Embedding dimension mismatch")
                    health_status["overall_healthy"] = False
            except Exception as e:
                health_status["component_status"]["embeddings"] = "error"
                health_status["errors"].append(f"Embedding service failed: {str(e)}")
                health_status["overall_healthy"] = False
            
            # Test other components
            health_status["component_status"]["document_converter"] = "healthy"
            health_status["component_status"]["pdf_parser"] = "healthy"
            health_status["component_status"]["semantic_chunker"] = "healthy"
            
        except Exception as e:
            health_status["overall_healthy"] = False
            health_status["errors"].append(f"Health check failed: {str(e)}")
        
        return health_status
    
    def cleanup_failed_uploads(self, namespace: str = "documents") -> Dict[str, Any]:
        """Clean up any documents that may have partial uploads"""
        
        cleanup_result = {
            "cleaned_items": 0,
            "errors": []
        }
        
        try:
            # This would require implementing a way to identify and clean up partial uploads
            # For now, just return a placeholder
            self.logger.info("Cleanup functionality would be implemented here")
            
        except Exception as e:
            cleanup_result["errors"].append(f"Cleanup failed: {str(e)}")
        
        return cleanup_result 

    def set_chunker(self, chunker):
        """Sets the chunking strategy for the processor."""
        self.logger.info(f"Setting chunker to: {type(chunker).__name__}")
        self.chunker = chunker 