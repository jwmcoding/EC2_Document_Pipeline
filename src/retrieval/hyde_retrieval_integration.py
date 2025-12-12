"""
HyDE Retrieval Integration for Business Document Search.

Integrates HyDE (Hypothetical Document Embeddings) with the existing Pinecone
hybrid search system to provide improved retrieval performance for business documents.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from enum import Enum
import time

from .hyde_query_transformer import BusinessDocumentHyDETransformer, HyDEResult, HyDEStrategy
from ..connectors.pinecone_client import PineconeDocumentClient, DocumentSearchResult
from ..config.settings import Settings

logger = logging.getLogger(__name__)


class RetrievalMode(Enum):
    """Different modes for HyDE retrieval"""
    TRADITIONAL = "traditional"  # Original query-based search
    HYDE_ONLY = "hyde_only"      # Only use HyDE-generated documents
    HYDE_COMBINED = "hyde_combined"  # Combine original query + HyDE documents
    HYDE_FALLBACK = "hyde_fallback"  # Try HyDE first, fallback to traditional


@dataclass
class HyDERetrievalResult:
    """Result of HyDE-enhanced retrieval"""
    documents: List[DocumentSearchResult]
    hyde_result: Optional[HyDEResult]
    retrieval_mode: RetrievalMode
    traditional_results: Optional[List[DocumentSearchResult]]
    total_time: float
    performance_metrics: Dict[str, Any]


class HyDERetrievalManager:
    """
    Manages HyDE-enhanced retrieval with A/B testing capabilities.
    
    Provides multiple retrieval modes to compare HyDE performance against
    traditional query-based search for business documents.
    """
    
    def __init__(
        self,
        pinecone_client: PineconeDocumentClient,
        settings: Settings,
        hyde_enabled: bool = True
    ):
        self.pinecone_client = pinecone_client
        self.settings = settings
        self.hyde_enabled = hyde_enabled
        
        # Initialize HyDE transformer
        if hyde_enabled:
            self.hyde_transformer = BusinessDocumentHyDETransformer(settings)
        else:
            self.hyde_transformer = None
            
        # Configuration from settings
        self.default_mode = getattr(settings, 'HYDE_RETRIEVAL_MODE', 'hyde_combined')
        self.include_original_query = getattr(settings, 'HYDE_INCLUDE_ORIGINAL_QUERY', True)
        self.num_hyde_documents = getattr(settings, 'HYDE_NUM_DOCUMENTS', 1)
        
        logger.info(f"🔍 HyDE Retrieval Manager initialized (enabled: {hyde_enabled}, mode: {self.default_mode})")
    
    def search_documents(
        self,
        query: str,
        mode: Optional[RetrievalMode] = None,
        filter_metadata: Optional[Dict] = None,
        top_k: int = 100,
        hyde_strategy: Optional[HyDEStrategy] = None,
        **search_kwargs
    ) -> HyDERetrievalResult:
        """
        Perform document search using specified retrieval mode.
        
        Args:
            query: Original user query
            mode: Retrieval mode to use (defaults to settings)
            filter_metadata: Pinecone filter conditions
            top_k: Number of results to retrieve
            hyde_strategy: Specific HyDE strategy (auto-selected if None)
            **search_kwargs: Additional arguments for hybrid_search_documents
            
        Returns:
            HyDERetrievalResult with documents and performance metrics
        """
        start_time = time.time()
        
        # Determine retrieval mode
        if mode is None:
            mode = RetrievalMode(self.default_mode)
        
        # Extract business context from filters for HyDE
        business_context = self._extract_business_context(filter_metadata)
        
        logger.info(f"🔍 Starting HyDE retrieval with mode: {mode.value}")
        
        # Execute based on mode
        if mode == RetrievalMode.TRADITIONAL:
            return self._traditional_search(query, filter_metadata, top_k, start_time, **search_kwargs)
        
        elif mode == RetrievalMode.HYDE_ONLY:
            return self._hyde_only_search(query, business_context, filter_metadata, top_k, 
                                        hyde_strategy, start_time, **search_kwargs)
        
        elif mode == RetrievalMode.HYDE_COMBINED:
            return self._hyde_combined_search(query, business_context, filter_metadata, top_k,
                                            hyde_strategy, start_time, **search_kwargs)
        
        elif mode == RetrievalMode.HYDE_FALLBACK:
            return self._hyde_fallback_search(query, business_context, filter_metadata, top_k,
                                            hyde_strategy, start_time, **search_kwargs)
        
        else:
            raise ValueError(f"Unknown retrieval mode: {mode}")
    
    def _traditional_search(
        self,
        query: str,
        filter_metadata: Optional[Dict],
        top_k: int,
        start_time: float,
        **search_kwargs
    ) -> HyDERetrievalResult:
        """Perform traditional query-based search"""
        logger.debug("📊 Executing traditional search")
        
        results = self.pinecone_client.hybrid_search_documents(
            query=query,
            filter_metadata=filter_metadata,
            top_k=top_k,
            **search_kwargs
        )
        
        total_time = time.time() - start_time
        
        return HyDERetrievalResult(
            documents=results,
            hyde_result=None,
            retrieval_mode=RetrievalMode.TRADITIONAL,
            traditional_results=results,
            total_time=total_time,
            performance_metrics={
                'num_results': len(results),
                'traditional_time': total_time,
                'hyde_time': 0.0,
                'strategy_used': 'traditional'
            }
        )
    
    def _hyde_only_search(
        self,
        query: str,
        business_context: Optional[Dict],
        filter_metadata: Optional[Dict],
        top_k: int,
        hyde_strategy: Optional[HyDEStrategy],
        start_time: float,
        **search_kwargs
    ) -> HyDERetrievalResult:
        """Perform search using only HyDE-generated documents"""
        if not self.hyde_transformer:
            logger.warning("⚠️ HyDE not enabled, falling back to traditional search")
            return self._traditional_search(query, filter_metadata, top_k, start_time, **search_kwargs)
        
        logger.debug("🎯 Executing HyDE-only search")
        
        # Generate hypothetical documents
        hyde_result = self.hyde_transformer.transform_query(
            query=query,
            business_context=business_context,
            strategy=hyde_strategy,
            num_documents=self.num_hyde_documents
        )
        
        if not hyde_result.hypothetical_documents:
            logger.warning("⚠️ HyDE generation failed, falling back to traditional search")
            return self._traditional_search(query, filter_metadata, top_k, start_time, **search_kwargs)
        
        # Search using the first hypothetical document
        search_query = hyde_result.hypothetical_documents[0]
        
        results = self.pinecone_client.hybrid_search_documents(
            query=search_query,
            filter_metadata=filter_metadata,
            top_k=top_k,
            **search_kwargs
        )
        
        total_time = time.time() - start_time
        
        return HyDERetrievalResult(
            documents=results,
            hyde_result=hyde_result,
            retrieval_mode=RetrievalMode.HYDE_ONLY,
            traditional_results=None,
            total_time=total_time,
            performance_metrics={
                'num_results': len(results),
                'traditional_time': 0.0,
                'hyde_time': hyde_result.generation_time,
                'strategy_used': hyde_result.strategy_used.value,
                'hyde_documents_generated': len(hyde_result.hypothetical_documents),
                'hyde_tokens_used': hyde_result.token_usage['total_tokens']
            }
        )
    
    def _hyde_combined_search(
        self,
        query: str,
        business_context: Optional[Dict],
        filter_metadata: Optional[Dict],
        top_k: int,
        hyde_strategy: Optional[HyDEStrategy],
        start_time: float,
        **search_kwargs
    ) -> HyDERetrievalResult:
        """Perform search combining original query with HyDE documents"""
        if not self.hyde_transformer:
            logger.warning("⚠️ HyDE not enabled, falling back to traditional search")
            return self._traditional_search(query, filter_metadata, top_k, start_time, **search_kwargs)
        
        logger.debug("🔄 Executing HyDE combined search")
        
        # Generate hypothetical documents
        hyde_result = self.hyde_transformer.transform_query(
            query=query,
            business_context=business_context,
            strategy=hyde_strategy,
            num_documents=self.num_hyde_documents
        )
        
        all_results = []
        traditional_results = None
        
        # Search with original query if enabled
        if self.include_original_query:
            traditional_results = self.pinecone_client.hybrid_search_documents(
                query=query,
                filter_metadata=filter_metadata,
                top_k=top_k // 2,  # Split top_k between original and HyDE
                **search_kwargs
            )
            all_results.extend(traditional_results)
        
        # Search with HyDE documents
        if hyde_result.hypothetical_documents:
            remaining_k = top_k - len(all_results) if self.include_original_query else top_k
            
            for i, hyde_doc in enumerate(hyde_result.hypothetical_documents):
                hyde_results = self.pinecone_client.hybrid_search_documents(
                    query=hyde_doc,
                    filter_metadata=filter_metadata,
                    top_k=remaining_k // len(hyde_result.hypothetical_documents),
                    **search_kwargs
                )
                all_results.extend(hyde_results)
        
        # Remove duplicates and re-rank
        unique_results = self._deduplicate_results(all_results)
        final_results = unique_results[:top_k]
        
        total_time = time.time() - start_time
        
        return HyDERetrievalResult(
            documents=final_results,
            hyde_result=hyde_result,
            retrieval_mode=RetrievalMode.HYDE_COMBINED,
            traditional_results=traditional_results,
            total_time=total_time,
            performance_metrics={
                'num_results': len(final_results),
                'num_unique_results': len(unique_results),
                'traditional_time': 0.0,  # Combined in total_time
                'hyde_time': hyde_result.generation_time if hyde_result else 0.0,
                'strategy_used': hyde_result.strategy_used.value if hyde_result else 'none',
                'hyde_documents_generated': len(hyde_result.hypothetical_documents) if hyde_result else 0,
                'hyde_tokens_used': hyde_result.token_usage['total_tokens'] if hyde_result else 0
            }
        )
    
    def _hyde_fallback_search(
        self,
        query: str,
        business_context: Optional[Dict],
        filter_metadata: Optional[Dict],
        top_k: int,
        hyde_strategy: Optional[HyDEStrategy],
        start_time: float,
        **search_kwargs
    ) -> HyDERetrievalResult:
        """Try HyDE first, fallback to traditional if insufficient results"""
        if not self.hyde_transformer:
            logger.warning("⚠️ HyDE not enabled, falling back to traditional search")
            return self._traditional_search(query, filter_metadata, top_k, start_time, **search_kwargs)
        
        logger.debug("🔄 Executing HyDE fallback search")
        
        # Try HyDE first
        hyde_result_obj = self._hyde_only_search(
            query, business_context, filter_metadata, top_k, hyde_strategy, start_time, **search_kwargs
        )
        
        # Check if HyDE results are sufficient
        min_results_threshold = max(3, top_k // 4)  # At least 3 results or 25% of requested
        
        if len(hyde_result_obj.documents) >= min_results_threshold:
            # HyDE results are sufficient
            hyde_result_obj.retrieval_mode = RetrievalMode.HYDE_FALLBACK
            hyde_result_obj.performance_metrics['fallback_triggered'] = False
            return hyde_result_obj
        
        # HyDE results insufficient, fallback to traditional
        logger.info(f"🔄 HyDE results insufficient ({len(hyde_result_obj.documents)} < {min_results_threshold}), falling back to traditional")
        
        traditional_results = self.pinecone_client.hybrid_search_documents(
            query=query,
            filter_metadata=filter_metadata,
            top_k=top_k,
            **search_kwargs
        )
        
        total_time = time.time() - start_time
        
        return HyDERetrievalResult(
            documents=traditional_results,
            hyde_result=hyde_result_obj.hyde_result,
            retrieval_mode=RetrievalMode.HYDE_FALLBACK,
            traditional_results=traditional_results,
            total_time=total_time,
            performance_metrics={
                'num_results': len(traditional_results),
                'traditional_time': total_time - (hyde_result_obj.hyde_result.generation_time if hyde_result_obj.hyde_result else 0),
                'hyde_time': hyde_result_obj.hyde_result.generation_time if hyde_result_obj.hyde_result else 0,
                'strategy_used': 'fallback_to_traditional',
                'fallback_triggered': True,
                'hyde_result_count': len(hyde_result_obj.documents),
                'fallback_threshold': min_results_threshold
            }
        )
    
    def _extract_business_context(self, filter_metadata: Optional[Dict]) -> Optional[Dict]:
        """Extract business context from Pinecone filter metadata for HyDE"""
        if not filter_metadata:
            return None
        
        business_context = {}
        
        # Direct field mappings
        for field in ['vendor', 'client', 'year', 'document_type']:
            if field in filter_metadata:
                value = filter_metadata[field]
                # Handle exact matches vs filter objects
                if isinstance(value, dict) and '$eq' in value:
                    business_context[field] = value['$eq']
                elif isinstance(value, str):
                    business_context[field] = value
        
        return business_context if business_context else None
    
    def _deduplicate_results(self, results: List[DocumentSearchResult]) -> List[DocumentSearchResult]:
        """Remove duplicate results based on document path, keeping highest scored"""
        seen_paths = {}
        unique_results = []
        
        for result in results:
            path = getattr(result, 'document_path', result.file_name)
            
            if path not in seen_paths or result.pinecone_score > seen_paths[path].pinecone_score:
                seen_paths[path] = result
        
        # Sort by score (highest first)
        unique_results = sorted(seen_paths.values(), key=lambda x: x.pinecone_score, reverse=True)
        
        return unique_results
    
    def compare_retrieval_modes(
        self,
        query: str,
        filter_metadata: Optional[Dict] = None,
        top_k: int = 100,
        modes: Optional[List[RetrievalMode]] = None
    ) -> Dict[RetrievalMode, HyDERetrievalResult]:
        """
        Compare different retrieval modes for A/B testing.
        
        Args:
            query: Search query
            filter_metadata: Pinecone filters
            top_k: Number of results per mode
            modes: List of modes to compare (defaults to all)
            
        Returns:
            Dictionary mapping modes to their results
        """
        if modes is None:
            modes = list(RetrievalMode)
        
        results = {}
        
        logger.info(f"🔬 Comparing {len(modes)} retrieval modes for query: {query[:50]}...")
        
        for mode in modes:
            try:
                logger.debug(f"Testing mode: {mode.value}")
                result = self.search_documents(
                    query=query,
                    mode=mode,
                    filter_metadata=filter_metadata,
                    top_k=top_k
                )
                results[mode] = result
                
                logger.info(f"✅ {mode.value}: {len(result.documents)} results in {result.total_time:.2f}s")
                
            except Exception as e:
                logger.error(f"❌ Error testing mode {mode.value}: {str(e)}")
                continue
        
        return results
    
    def get_performance_summary(self, result: HyDERetrievalResult) -> Dict[str, Any]:
        """Get a human-readable performance summary"""
        metrics = result.performance_metrics.copy()
        
        # Add derived metrics
        metrics['retrieval_mode'] = result.retrieval_mode.value
        metrics['total_time'] = result.total_time
        metrics['has_hyde_results'] = result.hyde_result is not None
        
        if result.hyde_result:
            metrics['hyde_confidence'] = result.hyde_result.confidence_score
            metrics['hyde_strategy'] = result.hyde_result.strategy_used.value
        
        return metrics