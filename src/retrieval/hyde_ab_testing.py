"""
A/B Testing Framework for HyDE vs Traditional Retrieval Methods.

Provides comprehensive testing and evaluation capabilities to compare
HyDE (Hypothetical Document Embeddings) performance against traditional
query-based retrieval for business documents.
"""

import logging
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import statistics

from .hyde_retrieval_integration import HyDERetrievalManager, RetrievalMode, HyDERetrievalResult
from ..config.settings import Settings

logger = logging.getLogger(__name__)


@dataclass
class ABTestQuery:
    """A single query for A/B testing"""
    query: str
    expected_vendors: Optional[List[str]] = None
    expected_document_types: Optional[List[str]] = None
    filters: Optional[Dict] = None
    ground_truth_docs: Optional[List[str]] = None
    difficulty_level: str = "medium"  # easy, medium, hard
    category: str = "general"  # pricing, technical, compliance, etc.


@dataclass
class ABTestResult:
    """Result of a single A/B test"""
    query: str
    traditional_result: HyDERetrievalResult
    hyde_result: HyDERetrievalResult
    metrics: Dict[str, Any]
    timestamp: str
    winner: str  # "traditional", "hyde", "tie"
    confidence_delta: float
    performance_delta: float


@dataclass
class ABTestSuite:
    """Complete A/B test suite results"""
    test_id: str
    timestamp: str
    settings_used: Dict[str, Any]
    individual_results: List[ABTestResult]
    summary_metrics: Dict[str, Any]
    recommendations: List[str]


class HyDEABTestFramework:
    """
    A/B Testing Framework for HyDE evaluation.
    
    Provides structured testing to evaluate HyDE performance against
    traditional retrieval methods with detailed metrics and analysis.
    """
    
    def __init__(
        self,
        retrieval_manager: HyDERetrievalManager,
        settings: Settings,
        results_dir: str = "logs/hyde_ab_tests"
    ):
        self.retrieval_manager = retrieval_manager
        self.settings = settings
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"🧪 HyDE A/B Test Framework initialized (results: {self.results_dir})")
    
    def create_business_test_queries(self) -> List[ABTestQuery]:
        """Create a comprehensive set of test queries for business documents"""
        return [
            # Pricing queries
            ABTestQuery(
                query="What are the licensing costs for Microsoft Office 365?",
                expected_vendors=["Microsoft"],
                expected_document_types=["Quote/Proposal", "Contract"],
                category="pricing",
                difficulty_level="easy"
            ),
            ABTestQuery(
                query="Compare pricing between Oracle and SAP database solutions",
                expected_vendors=["Oracle", "SAP"],
                expected_document_types=["Quote/Proposal", "Vendor Comparison"],
                category="pricing",
                difficulty_level="hard"
            ),
            
            # Technical queries
            ABTestQuery(
                query="What are the system requirements for implementing Salesforce?",
                expected_vendors=["Salesforce"],
                expected_document_types=["Technical Specification", "Implementation Document"],
                category="technical",
                difficulty_level="medium"
            ),
            ABTestQuery(
                query="How does AWS backup and disaster recovery work?",
                expected_vendors=["AWS", "Amazon"],
                expected_document_types=["Technical Specification"],
                category="technical",
                difficulty_level="medium"
            ),
            
            # Compliance queries
            ABTestQuery(
                query="What security certifications does our cloud provider have?",
                expected_document_types=["Compliance/Security Document"],
                category="compliance",
                difficulty_level="medium"
            ),
            ABTestQuery(
                query="GDPR compliance requirements for customer data processing",
                expected_document_types=["Compliance/Security Document", "Contract"],
                category="compliance",
                difficulty_level="hard"
            ),
            
            # Contract queries
            ABTestQuery(
                query="What are the renewal terms in our IBM contract?",
                expected_vendors=["IBM"],
                expected_document_types=["Contract", "Amendment/Addendum"],
                category="contracts",
                difficulty_level="easy"
            ),
            ABTestQuery(
                query="Find all contracts expiring in 2024",
                expected_document_types=["Contract"],
                filters={"year": "2024"},
                category="contracts",
                difficulty_level="medium"
            ),
            
            # Vendor comparison queries
            ABTestQuery(
                query="Compare Cisco vs Juniper networking solutions",
                expected_vendors=["Cisco", "Juniper"],
                expected_document_types=["Vendor Comparison", "Technical Specification"],
                category="vendor_comparison",
                difficulty_level="hard"
            ),
            
            # General business queries
            ABTestQuery(
                query="What are the key deliverables in our latest SOW?",
                expected_document_types=["Statement of Work (SOW)"],
                category="general",
                difficulty_level="easy"
            ),
            ABTestQuery(
                query="Show me all documents related to cloud migration projects",
                expected_document_types=["Technical Specification", "Implementation Document"],
                category="general",
                difficulty_level="medium"
            ),
            
            # Edge cases
            ABTestQuery(
                query="Documents mentioning artificial intelligence or machine learning",
                category="edge_case",
                difficulty_level="hard"
            ),
            ABTestQuery(
                query="Find invoices with amounts over $100,000",
                expected_document_types=["Invoice/Billing"],
                category="edge_case",
                difficulty_level="medium"
            )
        ]
    
    def run_ab_test_suite(
        self,
        test_queries: Optional[List[ABTestQuery]] = None,
        num_results: int = 50,
        save_results: bool = True
    ) -> ABTestSuite:
        """
        Run a complete A/B test suite comparing HyDE vs traditional retrieval.
        
        Args:
            test_queries: List of queries to test (uses default business queries if None)
            num_results: Number of results to retrieve per query
            save_results: Whether to save results to file
            
        Returns:
            ABTestSuite with complete results and analysis
        """
        if test_queries is None:
            test_queries = self.create_business_test_queries()
        
        test_id = f"hyde_ab_test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"🧪 Starting A/B test suite: {test_id} ({len(test_queries)} queries)")
        
        individual_results = []
        start_time = time.time()
        
        for i, test_query in enumerate(test_queries):
            logger.info(f"🔍 Testing query {i+1}/{len(test_queries)}: {test_query.query[:50]}...")
            
            try:
                result = self._run_single_ab_test(test_query, num_results)
                individual_results.append(result)
                
                logger.info(f"✅ Query {i+1} complete - Winner: {result.winner}")
                
            except Exception as e:
                logger.error(f"❌ Error testing query {i+1}: {str(e)}")
                continue
        
        total_time = time.time() - start_time
        
        # Calculate summary metrics
        summary_metrics = self._calculate_summary_metrics(individual_results, total_time)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(individual_results, summary_metrics)
        
        # Create test suite
        test_suite = ABTestSuite(
            test_id=test_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            settings_used=self._get_test_settings(),
            individual_results=individual_results,
            summary_metrics=summary_metrics,
            recommendations=recommendations
        )
        
        if save_results:
            self._save_test_results(test_suite)
        
        logger.info(f"🎉 A/B test suite complete: {len(individual_results)} successful tests in {total_time:.1f}s")
        
        return test_suite
    
    def _run_single_ab_test(self, test_query: ABTestQuery, num_results: int) -> ABTestResult:
        """Run a single A/B test comparing traditional vs HyDE retrieval"""
        
        # Test traditional retrieval
        traditional_result = self.retrieval_manager.search_documents(
            query=test_query.query,
            mode=RetrievalMode.TRADITIONAL,
            filter_metadata=test_query.filters,
            top_k=num_results
        )
        
        # Test HyDE retrieval
        hyde_result = self.retrieval_manager.search_documents(
            query=test_query.query,
            mode=RetrievalMode.HYDE_COMBINED,  # Use combined for fair comparison
            filter_metadata=test_query.filters,
            top_k=num_results
        )
        
        # Calculate comparative metrics
        metrics = self._calculate_comparative_metrics(
            test_query, traditional_result, hyde_result
        )
        
        # Determine winner
        winner, confidence_delta, performance_delta = self._determine_winner(
            traditional_result, hyde_result, metrics
        )
        
        return ABTestResult(
            query=test_query.query,
            traditional_result=traditional_result,
            hyde_result=hyde_result,
            metrics=metrics,
            timestamp=datetime.now(timezone.utc).isoformat(),
            winner=winner,
            confidence_delta=confidence_delta,
            performance_delta=performance_delta
        )
    
    def _calculate_comparative_metrics(
        self,
        test_query: ABTestQuery,
        traditional_result: HyDERetrievalResult,
        hyde_result: HyDERetrievalResult
    ) -> Dict[str, Any]:
        """Calculate comprehensive metrics comparing both retrieval methods"""
        
        metrics = {
            "result_counts": {
                "traditional": len(traditional_result.documents),
                "hyde": len(hyde_result.documents)
            },
            "response_times": {
                "traditional": traditional_result.total_time,
                "hyde": hyde_result.total_time,
                "delta": hyde_result.total_time - traditional_result.total_time
            },
            "confidence_scores": {
                "traditional": self._calculate_confidence_score(traditional_result),
                "hyde": self._calculate_confidence_score(hyde_result)
            }
        }
        
        # Vendor matching analysis
        if test_query.expected_vendors:
            metrics["vendor_matching"] = {
                "traditional": self._count_vendor_matches(traditional_result, test_query.expected_vendors),
                "hyde": self._count_vendor_matches(hyde_result, test_query.expected_vendors)
            }
        
        # Document type analysis
        if test_query.expected_document_types:
            metrics["document_type_matching"] = {
                "traditional": self._count_document_type_matches(traditional_result, test_query.expected_document_types),
                "hyde": self._count_document_type_matches(hyde_result, test_query.expected_document_types)
            }
        
        # Result overlap analysis
        overlap_analysis = self._analyze_result_overlap(traditional_result, hyde_result)
        metrics["overlap_analysis"] = overlap_analysis
        
        # Score distribution analysis
        metrics["score_distributions"] = {
            "traditional": self._analyze_score_distribution(traditional_result),
            "hyde": self._analyze_score_distribution(hyde_result)
        }
        
        return metrics
    
    def _calculate_confidence_score(self, result: HyDERetrievalResult) -> float:
        """Calculate a confidence score based on result quality"""
        if not result.documents:
            return 0.0
        
        # Base score from number of results
        result_score = min(len(result.documents) / 10.0, 1.0)
        
        # Average document score
        avg_score = statistics.mean([doc.pinecone_score for doc in result.documents[:10]])
        
        # Combine scores
        confidence = (result_score * 0.3) + (avg_score * 0.7)
        
        return min(confidence, 1.0)
    
    def _count_vendor_matches(self, result: HyDERetrievalResult, expected_vendors: List[str]) -> Dict[str, int]:
        """Count how many results match expected vendors"""
        vendor_counts = {}
        
        for vendor in expected_vendors:
            count = sum(
                1 for doc in result.documents[:10]
                if doc.vendor and vendor.lower() in doc.vendor.lower()
            )
            vendor_counts[vendor] = count
        
        return vendor_counts
    
    def _count_document_type_matches(self, result: HyDERetrievalResult, expected_types: List[str]) -> Dict[str, int]:
        """Count how many results match expected document types"""
        type_counts = {}
        
        for doc_type in expected_types:
            count = sum(
                1 for doc in result.documents[:10]
                if doc.metadata and doc.metadata.get('document_type') and 
                doc_type.lower() in doc.metadata.get('document_type', '').lower()
            )
            type_counts[doc_type] = count
        
        return type_counts
    
    def _analyze_result_overlap(self, traditional: HyDERetrievalResult, hyde: HyDERetrievalResult) -> Dict[str, Any]:
        """Analyze overlap between traditional and HyDE results"""
        traditional_paths = {doc.file_name for doc in traditional.documents[:20]}
        hyde_paths = {doc.file_name for doc in hyde.documents[:20]}
        
        overlap = traditional_paths.intersection(hyde_paths)
        unique_traditional = traditional_paths - hyde_paths
        unique_hyde = hyde_paths - traditional_paths
        
        return {
            "total_overlap": len(overlap),
            "overlap_percentage": len(overlap) / max(len(traditional_paths), 1) * 100,
            "unique_traditional": len(unique_traditional),
            "unique_hyde": len(unique_hyde),
            "total_unique_results": len(traditional_paths.union(hyde_paths))
        }
    
    def _analyze_score_distribution(self, result: HyDERetrievalResult) -> Dict[str, float]:
        """Analyze the score distribution of results"""
        if not result.documents:
            return {"mean": 0.0, "median": 0.0, "max": 0.0, "min": 0.0}
        
        scores = [doc.pinecone_score for doc in result.documents[:20]]
        
        return {
            "mean": statistics.mean(scores),
            "median": statistics.median(scores),
            "max": max(scores),
            "min": min(scores),
            "std_dev": statistics.stdev(scores) if len(scores) > 1 else 0.0
        }
    
    def _determine_winner(
        self,
        traditional: HyDERetrievalResult,
        hyde: HyDERetrievalResult,
        metrics: Dict[str, Any]
    ) -> Tuple[str, float, float]:
        """Determine the winner based on multiple criteria"""
        
        # Confidence score comparison
        trad_confidence = metrics["confidence_scores"]["traditional"]
        hyde_confidence = metrics["confidence_scores"]["hyde"]
        confidence_delta = hyde_confidence - trad_confidence
        
        # Performance comparison (lower time is better)
        trad_time = metrics["response_times"]["traditional"]
        hyde_time = metrics["response_times"]["hyde"]
        performance_delta = trad_time - hyde_time  # Positive means HyDE is faster
        
        # Result count comparison
        trad_count = metrics["result_counts"]["traditional"]
        hyde_count = metrics["result_counts"]["hyde"]
        
        # Scoring system
        hyde_score = 0
        traditional_score = 0
        
        # Confidence score (40% weight)
        if confidence_delta > 0.05:
            hyde_score += 0.4
        elif confidence_delta < -0.05:
            traditional_score += 0.4
        else:
            hyde_score += 0.2
            traditional_score += 0.2
        
        # Result count (20% weight)
        if hyde_count > trad_count:
            hyde_score += 0.2
        elif trad_count > hyde_count:
            traditional_score += 0.2
        else:
            hyde_score += 0.1
            traditional_score += 0.1
        
        # Performance penalty for HyDE if significantly slower (20% weight)
        if performance_delta < -2.0:  # HyDE is >2s slower
            traditional_score += 0.2
        elif performance_delta > 1.0:  # HyDE is >1s faster
            hyde_score += 0.2
        else:
            hyde_score += 0.1
            traditional_score += 0.1
        
        # Vendor/document type matching (20% weight)
        vendor_bonus = 0
        if "vendor_matching" in metrics:
            hyde_vendor_total = sum(metrics["vendor_matching"]["hyde"].values())
            trad_vendor_total = sum(metrics["vendor_matching"]["traditional"].values())
            if hyde_vendor_total > trad_vendor_total:
                vendor_bonus += 0.1
            elif trad_vendor_total > hyde_vendor_total:
                vendor_bonus -= 0.1
        
        if "document_type_matching" in metrics:
            hyde_type_total = sum(metrics["document_type_matching"]["hyde"].values())
            trad_type_total = sum(metrics["document_type_matching"]["traditional"].values())
            if hyde_type_total > trad_type_total:
                vendor_bonus += 0.1
            elif trad_type_total > hyde_type_total:
                vendor_bonus -= 0.1
        
        hyde_score += max(0, vendor_bonus)
        traditional_score += max(0, -vendor_bonus)
        
        # Determine winner
        if hyde_score > traditional_score + 0.1:
            winner = "hyde"
        elif traditional_score > hyde_score + 0.1:
            winner = "traditional"
        else:
            winner = "tie"
        
        return winner, confidence_delta, performance_delta
    
    def _calculate_summary_metrics(self, results: List[ABTestResult], total_time: float) -> Dict[str, Any]:
        """Calculate summary metrics across all test results"""
        if not results:
            return {}
        
        winner_counts = {"hyde": 0, "traditional": 0, "tie": 0}
        confidence_deltas = []
        performance_deltas = []
        
        for result in results:
            winner_counts[result.winner] += 1
            confidence_deltas.append(result.confidence_delta)
            performance_deltas.append(result.performance_delta)
        
        total_tests = len(results)
        
        return {
            "test_summary": {
                "total_tests": total_tests,
                "successful_tests": total_tests,
                "total_time": total_time,
                "avg_time_per_test": total_time / total_tests
            },
            "winner_distribution": {
                "hyde_wins": winner_counts["hyde"],
                "traditional_wins": winner_counts["traditional"],
                "ties": winner_counts["tie"],
                "hyde_win_percentage": (winner_counts["hyde"] / total_tests) * 100,
                "traditional_win_percentage": (winner_counts["traditional"] / total_tests) * 100
            },
            "performance_analysis": {
                "avg_confidence_delta": statistics.mean(confidence_deltas),
                "avg_performance_delta": statistics.mean(performance_deltas),
                "confidence_improvement_rate": sum(1 for d in confidence_deltas if d > 0) / total_tests * 100,
                "performance_degradation_rate": sum(1 for d in performance_deltas if d < -1.0) / total_tests * 100
            }
        }
    
    def _generate_recommendations(self, results: List[ABTestResult], summary: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on test results"""
        recommendations = []
        
        hyde_win_rate = summary["winner_distribution"]["hyde_win_percentage"]
        avg_confidence_delta = summary["performance_analysis"]["avg_confidence_delta"]
        avg_performance_delta = summary["performance_analysis"]["avg_performance_delta"]
        
        # Overall recommendation
        if hyde_win_rate > 60:
            recommendations.append(f"🎯 STRONG RECOMMENDATION: Enable HyDE for production use. {hyde_win_rate:.1f}% win rate shows significant improvement.")
        elif hyde_win_rate > 40:
            recommendations.append(f"⚖️ MIXED RESULTS: Consider selective HyDE usage. {hyde_win_rate:.1f}% win rate suggests benefits for specific query types.")
        else:
            recommendations.append(f"⚠️ LIMITED BENEFIT: Current HyDE configuration shows {hyde_win_rate:.1f}% win rate. Consider tuning or traditional approach.")
        
        # Confidence analysis
        if avg_confidence_delta > 0.1:
            recommendations.append(f"✅ HyDE consistently improves result confidence (+{avg_confidence_delta:.3f} average).")
        elif avg_confidence_delta < -0.1:
            recommendations.append(f"⚠️ HyDE reduces result confidence ({avg_confidence_delta:.3f} average). Review prompt strategies.")
        
        # Performance analysis
        if avg_performance_delta < -2.0:
            recommendations.append(f"🐌 Performance concern: HyDE adds {abs(avg_performance_delta):.1f}s average latency. Consider optimization.")
        elif avg_performance_delta > 0.5:
            recommendations.append(f"⚡ Unexpected: HyDE appears faster by {avg_performance_delta:.1f}s. Verify test conditions.")
        
        # Query-specific recommendations
        category_analysis = self._analyze_by_category(results)
        for category, performance in category_analysis.items():
            if performance["hyde_win_rate"] > 70:
                recommendations.append(f"🎯 Enable HyDE specifically for {category} queries ({performance['hyde_win_rate']:.1f}% win rate).")
            elif performance["hyde_win_rate"] < 30:
                recommendations.append(f"⚠️ Avoid HyDE for {category} queries ({performance['hyde_win_rate']:.1f}% win rate).")
        
        return recommendations
    
    def _analyze_by_category(self, results: List[ABTestResult]) -> Dict[str, Dict[str, float]]:
        """Analyze performance by query category"""
        # This would require category information from the original queries
        # For now, return a simple analysis
        return {
            "overall": {
                "hyde_win_rate": sum(1 for r in results if r.winner == "hyde") / len(results) * 100,
                "avg_confidence_delta": statistics.mean([r.confidence_delta for r in results])
            }
        }
    
    def _get_test_settings(self) -> Dict[str, Any]:
        """Get current test settings for documentation"""
        return {
            "hyde_enabled": self.settings.HYDE_ENABLED,
            "hyde_model": self.settings.HYDE_MODEL,
            "hyde_temperature": self.settings.HYDE_TEMPERATURE,
            "hyde_max_tokens": self.settings.HYDE_MAX_TOKENS,
            "hyde_num_documents": self.settings.HYDE_NUM_DOCUMENTS,
            "retrieval_mode": self.settings.HYDE_RETRIEVAL_MODE
        }
    
    def _save_test_results(self, test_suite: ABTestSuite) -> None:
        """Save test results to file"""
        filename = f"{test_suite.test_id}.json"
        filepath = self.results_dir / filename
        
        # Convert to dict for JSON serialization
        results_dict = asdict(test_suite)
        
        with open(filepath, 'w') as f:
            json.dump(results_dict, f, indent=2, default=str)
        
        logger.info(f"💾 Test results saved to: {filepath}")
    
    def load_test_results(self, test_id: str) -> Optional[ABTestSuite]:
        """Load previously saved test results"""
        filename = f"{test_id}.json"
        filepath = self.results_dir / filename
        
        if not filepath.exists():
            logger.error(f"Test results not found: {filepath}")
            return None
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Convert back to dataclass (simplified version)
            return ABTestSuite(**data)
            
        except Exception as e:
            logger.error(f"Error loading test results: {str(e)}")
            return None