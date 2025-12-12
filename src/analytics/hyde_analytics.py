"""
HyDE Analytics and Performance Tracking System.

Provides comprehensive logging, metrics collection, and performance analysis
for HyDE (Hypothetical Document Embeddings) implementation in business
document retrieval systems.
"""

import logging
import json
import time
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
from collections import defaultdict, Counter

from ..retrieval.hyde_retrieval_integration import HyDERetrievalResult, RetrievalMode
from ..retrieval.hyde_query_transformer import HyDEResult, HyDEStrategy

logger = logging.getLogger(__name__)


@dataclass
class HyDEMetrics:
    """Individual HyDE operation metrics"""
    query_id: str
    timestamp: str
    query: str
    retrieval_mode: str
    hyde_strategy: Optional[str]
    generation_time: float
    search_time: float
    total_time: float
    tokens_used: int
    documents_generated: int
    results_found: int
    confidence_score: float
    success: bool
    error_message: Optional[str] = None
    business_context: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceComparison:
    """Comparison metrics between HyDE and traditional search"""
    query_id: str
    timestamp: str
    query: str
    traditional_metrics: Dict[str, Any]
    hyde_metrics: Dict[str, Any]
    winner: str  # 'hyde', 'traditional', 'tie'
    confidence_delta: float
    performance_delta: float
    result_count_delta: int
    overlap_percentage: float


@dataclass
class SessionAnalytics:
    """Analytics for a complete session"""
    session_id: str
    start_time: str
    end_time: str
    total_queries: int
    hyde_queries: int
    traditional_queries: int
    avg_response_time: float
    success_rate: float
    cost_estimate: float
    user_satisfaction_indicators: Dict[str, Any]


class HyDEAnalyticsTracker:
    """
    Comprehensive analytics and performance tracking for HyDE operations.
    
    Tracks performance metrics, cost analysis, and user interaction patterns
    to provide insights for HyDE optimization and business value assessment.
    """
    
    def __init__(self, analytics_dir: str = "logs/hyde_analytics"):
        self.analytics_dir = Path(analytics_dir)
        self.analytics_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory storage for current session
        self.session_metrics: List[HyDEMetrics] = []
        self.performance_comparisons: List[PerformanceComparison] = []
        self.session_start = datetime.now(timezone.utc)
        self.session_id = f"hyde_session_{self.session_start.strftime('%Y%m%d_%H%M%S')}"
        
        # Cost tracking
        self.token_costs = {
            'gpt-4.1-mini': {
                'input': 0.40 / 1_000_000,  # $0.40 per 1M input tokens
                'output': 1.60 / 1_000_000   # $1.60 per 1M output tokens
            }
        }
        
        logger.info(f"📊 HyDE Analytics Tracker initialized (session: {self.session_id})")
    
    def track_hyde_operation(
        self,
        query: str,
        retrieval_result: HyDERetrievalResult,
        business_context: Optional[Dict] = None
    ) -> str:
        """
        Track a single HyDE operation with comprehensive metrics.
        
        Args:
            query: Original search query
            retrieval_result: HyDE retrieval result
            business_context: Business context from filters
            
        Returns:
            Unique query ID for this operation
        """
        query_id = f"q_{len(self.session_metrics) + 1}_{int(time.time())}"
        
        # Extract metrics from retrieval result
        hyde_result = retrieval_result.hyde_result
        performance_metrics = retrieval_result.performance_metrics
        
        metrics = HyDEMetrics(
            query_id=query_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            query=query,
            retrieval_mode=retrieval_result.retrieval_mode.value,
            hyde_strategy=hyde_result.strategy_used.value if hyde_result else None,
            generation_time=hyde_result.generation_time if hyde_result else 0.0,
            search_time=retrieval_result.total_time - (hyde_result.generation_time if hyde_result else 0.0),
            total_time=retrieval_result.total_time,
            tokens_used=hyde_result.token_usage['total_tokens'] if hyde_result else 0,
            documents_generated=len(hyde_result.hypothetical_documents) if hyde_result else 0,
            results_found=len(retrieval_result.documents),
            confidence_score=hyde_result.confidence_score if hyde_result else 0.0,
            success=len(retrieval_result.documents) > 0,
            business_context=business_context
        )
        
        self.session_metrics.append(metrics)
        
        # Log key metrics
        logger.info(f"📊 HyDE operation tracked: {query_id} | "
                   f"Mode: {metrics.retrieval_mode} | "
                   f"Results: {metrics.results_found} | "
                   f"Time: {metrics.total_time:.2f}s | "
                   f"Tokens: {metrics.tokens_used}")
        
        return query_id
    
    def track_performance_comparison(
        self,
        query: str,
        traditional_result: HyDERetrievalResult,
        hyde_result: HyDERetrievalResult
    ) -> str:
        """
        Track a performance comparison between traditional and HyDE search.
        
        Args:
            query: Search query
            traditional_result: Traditional search result
            hyde_result: HyDE search result
            
        Returns:
            Comparison ID
        """
        comparison_id = f"comp_{len(self.performance_comparisons) + 1}_{int(time.time())}"
        
        # Calculate comparative metrics
        traditional_metrics = {
            'response_time': traditional_result.total_time,
            'result_count': len(traditional_result.documents),
            'avg_score': self._calculate_average_score(traditional_result.documents),
            'top_score': max([doc.score for doc in traditional_result.documents]) if traditional_result.documents else 0.0
        }
        
        hyde_metrics = {
            'response_time': hyde_result.total_time,
            'result_count': len(hyde_result.documents),
            'avg_score': self._calculate_average_score(hyde_result.documents),
            'top_score': max([doc.score for doc in hyde_result.documents]) if hyde_result.documents else 0.0,
            'generation_time': hyde_result.hyde_result.generation_time if hyde_result.hyde_result else 0.0,
            'tokens_used': hyde_result.hyde_result.token_usage['total_tokens'] if hyde_result.hyde_result else 0,
            'strategy_used': hyde_result.hyde_result.strategy_used.value if hyde_result.hyde_result else 'none'
        }
        
        # Determine winner
        winner, confidence_delta, performance_delta = self._determine_comparison_winner(
            traditional_metrics, hyde_metrics
        )
        
        # Calculate result overlap
        overlap_percentage = self._calculate_result_overlap(
            traditional_result.documents, hyde_result.documents
        )
        
        comparison = PerformanceComparison(
            query_id=comparison_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            query=query,
            traditional_metrics=traditional_metrics,
            hyde_metrics=hyde_metrics,
            winner=winner,
            confidence_delta=confidence_delta,
            performance_delta=performance_delta,
            result_count_delta=hyde_metrics['result_count'] - traditional_metrics['result_count'],
            overlap_percentage=overlap_percentage
        )
        
        self.performance_comparisons.append(comparison)
        
        logger.info(f"📊 Performance comparison tracked: {comparison_id} | "
                   f"Winner: {winner} | "
                   f"Confidence Δ: {confidence_delta:+.3f} | "
                   f"Performance Δ: {performance_delta:+.2f}s")
        
        return comparison_id
    
    def _calculate_average_score(self, documents: List[Any]) -> float:
        """Calculate average relevance score for top documents"""
        if not documents:
            return 0.0
        
        top_docs = documents[:10]  # Consider top 10 for average
        return statistics.mean([doc.score for doc in top_docs])
    
    def _determine_comparison_winner(
        self,
        traditional_metrics: Dict[str, Any],
        hyde_metrics: Dict[str, Any]
    ) -> Tuple[str, float, float]:
        """Determine winner of performance comparison"""
        
        # Confidence delta (average score difference)
        confidence_delta = hyde_metrics['avg_score'] - traditional_metrics['avg_score']
        
        # Performance delta (response time difference - negative means HyDE is slower)
        performance_delta = traditional_metrics['response_time'] - hyde_metrics['response_time']
        
        # Scoring system
        hyde_score = 0
        traditional_score = 0
        
        # Result quality (50% weight)
        if confidence_delta > 0.02:
            hyde_score += 0.5
        elif confidence_delta < -0.02:
            traditional_score += 0.5
        else:
            hyde_score += 0.25
            traditional_score += 0.25
        
        # Result count (20% weight)
        count_delta = hyde_metrics['result_count'] - traditional_metrics['result_count']
        if count_delta > 0:
            hyde_score += 0.2
        elif count_delta < 0:
            traditional_score += 0.2
        else:
            hyde_score += 0.1
            traditional_score += 0.1
        
        # Performance penalty for significant slowdown (30% weight)
        if performance_delta < -2.0:  # HyDE is >2s slower
            traditional_score += 0.3
        elif performance_delta > 0.5:  # HyDE is >0.5s faster
            hyde_score += 0.3
        elif performance_delta > -1.0:  # HyDE is <1s slower (acceptable)
            hyde_score += 0.15
            traditional_score += 0.15
        else:
            traditional_score += 0.2
            hyde_score += 0.1
        
        # Determine winner
        if hyde_score > traditional_score + 0.1:
            winner = "hyde"
        elif traditional_score > hyde_score + 0.1:
            winner = "traditional"
        else:
            winner = "tie"
        
        return winner, confidence_delta, performance_delta
    
    def _calculate_result_overlap(self, traditional_docs: List[Any], hyde_docs: List[Any]) -> float:
        """Calculate percentage overlap between result sets"""
        if not traditional_docs or not hyde_docs:
            return 0.0
        
        traditional_paths = {getattr(doc, 'file_name', str(doc)) for doc in traditional_docs[:20]}
        hyde_paths = {getattr(doc, 'file_name', str(doc)) for doc in hyde_docs[:20]}
        
        overlap = len(traditional_paths.intersection(hyde_paths))
        total_unique = len(traditional_paths.union(hyde_paths))
        
        return (overlap / total_unique * 100) if total_unique > 0 else 0.0
    
    def calculate_cost_analysis(self, model: str = 'gpt-4.1-mini') -> Dict[str, float]:
        """Calculate cost analysis for current session"""
        if model not in self.token_costs:
            logger.warning(f"Unknown model for cost calculation: {model}")
            return {}
        
        total_tokens = sum(metric.tokens_used for metric in self.session_metrics)
        
        # Estimate input/output token split (roughly 3:1 ratio for document generation)
        estimated_input_tokens = total_tokens * 0.75
        estimated_output_tokens = total_tokens * 0.25
        
        input_cost = estimated_input_tokens * self.token_costs[model]['input']
        output_cost = estimated_output_tokens * self.token_costs[model]['output']
        total_cost = input_cost + output_cost
        
        return {
            'total_tokens': total_tokens,
            'estimated_input_tokens': estimated_input_tokens,
            'estimated_output_tokens': estimated_output_tokens,
            'input_cost_usd': input_cost,
            'output_cost_usd': output_cost,
            'total_cost_usd': total_cost,
            'cost_per_query': total_cost / len(self.session_metrics) if self.session_metrics else 0.0
        }
    
    def generate_session_summary(self) -> SessionAnalytics:
        """Generate comprehensive session analytics"""
        end_time = datetime.now(timezone.utc)
        
        if not self.session_metrics:
            return SessionAnalytics(
                session_id=self.session_id,
                start_time=self.session_start.isoformat(),
                end_time=end_time.isoformat(),
                total_queries=0,
                hyde_queries=0,
                traditional_queries=0,
                avg_response_time=0.0,
                success_rate=0.0,
                cost_estimate=0.0,
                user_satisfaction_indicators={}
            )
        
        # Calculate metrics
        total_queries = len(self.session_metrics)
        hyde_queries = sum(1 for m in self.session_metrics if m.retrieval_mode != 'traditional')
        traditional_queries = total_queries - hyde_queries
        
        avg_response_time = statistics.mean([m.total_time for m in self.session_metrics])
        success_rate = sum(1 for m in self.session_metrics if m.success) / total_queries * 100
        
        cost_analysis = self.calculate_cost_analysis()
        cost_estimate = cost_analysis.get('total_cost_usd', 0.0)
        
        # User satisfaction indicators
        satisfaction_indicators = {
            'avg_results_per_query': statistics.mean([m.results_found for m in self.session_metrics]),
            'avg_confidence_score': statistics.mean([m.confidence_score for m in self.session_metrics]),
            'zero_result_rate': sum(1 for m in self.session_metrics if m.results_found == 0) / total_queries * 100,
            'high_confidence_rate': sum(1 for m in self.session_metrics if m.confidence_score > 0.8) / total_queries * 100
        }
        
        return SessionAnalytics(
            session_id=self.session_id,
            start_time=self.session_start.isoformat(),
            end_time=end_time.isoformat(),
            total_queries=total_queries,
            hyde_queries=hyde_queries,
            traditional_queries=traditional_queries,
            avg_response_time=avg_response_time,
            success_rate=success_rate,
            cost_estimate=cost_estimate,
            user_satisfaction_indicators=satisfaction_indicators
        )
    
    def generate_strategy_performance_report(self) -> Dict[HyDEStrategy, Dict[str, Any]]:
        """Generate performance report by HyDE strategy"""
        strategy_metrics = defaultdict(list)
        
        for metric in self.session_metrics:
            if metric.hyde_strategy:
                try:
                    strategy = HyDEStrategy(metric.hyde_strategy)
                    strategy_metrics[strategy].append(metric)
                except ValueError:
                    continue
        
        report = {}
        
        for strategy, metrics in strategy_metrics.items():
            if not metrics:
                continue
            
            report[strategy] = {
                'total_queries': len(metrics),
                'avg_response_time': statistics.mean([m.total_time for m in metrics]),
                'avg_generation_time': statistics.mean([m.generation_time for m in metrics]),
                'avg_results_found': statistics.mean([m.results_found for m in metrics]),
                'avg_confidence_score': statistics.mean([m.confidence_score for m in metrics]),
                'success_rate': sum(1 for m in metrics if m.success) / len(metrics) * 100,
                'avg_tokens_used': statistics.mean([m.tokens_used for m in metrics]),
                'total_cost_estimate': sum(m.tokens_used for m in metrics) * self.token_costs['gpt-4.1-mini']['input'] * 1.33  # Rough estimate
            }
        
        return report
    
    def export_analytics_data(self, include_raw_data: bool = True) -> Dict[str, Any]:
        """Export comprehensive analytics data for analysis"""
        session_summary = self.generate_session_summary()
        strategy_report = self.generate_strategy_performance_report()
        cost_analysis = self.calculate_cost_analysis()
        
        export_data = {
            'session_summary': asdict(session_summary),
            'strategy_performance': {
                strategy.value: metrics for strategy, metrics in strategy_report.items()
            },
            'cost_analysis': cost_analysis,
            'performance_comparisons_summary': self._summarize_performance_comparisons(),
            'export_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if include_raw_data:
            export_data['raw_metrics'] = [asdict(metric) for metric in self.session_metrics]
            export_data['raw_comparisons'] = [asdict(comp) for comp in self.performance_comparisons]
        
        return export_data
    
    def _summarize_performance_comparisons(self) -> Dict[str, Any]:
        """Summarize performance comparison results"""
        if not self.performance_comparisons:
            return {}
        
        winners = Counter([comp.winner for comp in self.performance_comparisons])
        
        return {
            'total_comparisons': len(self.performance_comparisons),
            'winner_distribution': dict(winners),
            'hyde_win_rate': (winners['hyde'] / len(self.performance_comparisons)) * 100,
            'avg_confidence_delta': statistics.mean([comp.confidence_delta for comp in self.performance_comparisons]),
            'avg_performance_delta': statistics.mean([comp.performance_delta for comp in self.performance_comparisons]),
            'avg_overlap_percentage': statistics.mean([comp.overlap_percentage for comp in self.performance_comparisons])
        }
    
    def save_session_analytics(self) -> str:
        """Save session analytics to file"""
        analytics_data = self.export_analytics_data()
        
        filename = f"{self.session_id}_analytics.json"
        filepath = self.analytics_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(analytics_data, f, indent=2, default=str)
        
        logger.info(f"📊 Analytics data saved: {filepath}")
        return str(filepath)
    
    def create_performance_dashboard_data(self) -> Dict[str, Any]:
        """Create data structure for Streamlit dashboard"""
        session_summary = self.generate_session_summary()
        strategy_report = self.generate_strategy_performance_report()
        
        # Prepare time series data
        time_series_data = []
        for i, metric in enumerate(self.session_metrics):
            time_series_data.append({
                'query_number': i + 1,
                'response_time': metric.total_time,
                'results_found': metric.results_found,
                'confidence_score': metric.confidence_score,
                'strategy': metric.hyde_strategy or 'traditional',
                'timestamp': metric.timestamp
            })
        
        # Prepare comparison data
        comparison_data = []
        for comp in self.performance_comparisons:
            comparison_data.append({
                'query': comp.query[:50] + "..." if len(comp.query) > 50 else comp.query,
                'winner': comp.winner,
                'confidence_delta': comp.confidence_delta,
                'performance_delta': comp.performance_delta,
                'overlap_percentage': comp.overlap_percentage
            })
        
        return {
            'session_summary': asdict(session_summary),
            'strategy_performance': {
                strategy.value: metrics for strategy, metrics in strategy_report.items()
            },
            'time_series_data': time_series_data,
            'comparison_data': comparison_data,
            'cost_analysis': self.calculate_cost_analysis()
        }