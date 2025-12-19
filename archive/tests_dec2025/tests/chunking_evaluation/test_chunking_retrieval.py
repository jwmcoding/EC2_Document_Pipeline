#!/usr/bin/env python3
"""
Chunking Strategy Retrieval Comparison Test

This script compares the retrieval performance of business-aware vs semantic chunking
by testing queries against the existing namespaces in the production index.
"""

import os
import sys
import json
import time
from datetime import datetime
from typing import List, Dict, Any

# Load environment and set up imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings
from src.config.colored_logging import ColoredLogger

class ChunkingRetrievalTester:
    """Tests retrieval performance between chunking strategies"""
    
    def __init__(self):
        self.logger = ColoredLogger("retrieval_tester")
        self.settings = Settings()
        
        # Use production index where the test data exists
        self.pinecone_client = PineconeDocumentClient(
            self.settings.PINECONE_API_KEY,
            index_name="business-documents",
            environment=self.settings.PINECONE_ENVIRONMENT
        )
        
        self.test_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def get_test_queries(self) -> List[str]:
        """Define test queries relevant to our actual test files (2024 Deal Docs)"""
        return [
            # Vendor-specific queries matching our test files
            "DigiCert SSL certificate pricing and renewal terms",
            "Dell hardware specifications and support services",
            "DocuSign implementation costs and contract terms",
            "F5 security appliance features and licensing",
            "Oracle database licensing and pricing structure",
            "Pure Storage array specifications and warranty",
            "Logitech equipment pricing and support terms",
            "Alteryx analytics platform licensing costs",
            "Qualys vulnerability scanning service terms",
            "Hyperscience document processing features",
            
            # Client-specific queries
            "Best Buy contract expiration dates",
            "Excellus BlueCross BlueShield deal terms",
            "Zoom Video Communications licensing costs",
            "LinkedIn implementation timeline",
            "BJC Healthcare support services",
            
            # Document type queries (IDDs, FMVs, expiring contracts)
            "initial due diligence document details",
            "fair market value pricing analysis",
            "contract expiration and renewal options",
            "vendor contact information and support",
            "deal pricing structure and payment terms"
        ]
    
    def test_retrieval_performance(self, query: str, namespace: str, top_k: int = 10) -> Dict[str, Any]:
        """Test retrieval performance for a single query in a namespace"""
        
        self.logger.info(f"Testing query in {namespace}: '{query[:50]}...'")
        
        start_time = time.time()
        
        try:
            # Perform hybrid search (same as production)
            results = self.pinecone_client.hybrid_search_documents(
                query=query,
                top_k=top_k,
                namespaces=[namespace],  # Note: this is a list parameter
                alpha=0.7  # Standard hybrid search balance
            )
            
            search_time = time.time() - start_time
            
            if results:
                # Use rerank_score if available, otherwise pinecone_score
                scores = [result.rerank_score if result.rerank_score is not None else result.pinecone_score 
                         for result in results]
                avg_score = sum(scores) / len(scores) if scores else 0.0
                max_score = max(scores) if scores else 0.0
                
                # Count results with good relevance (score > 0.8)
                relevant_count = sum(1 for score in scores if score > 0.8)
                
                return {
                    'success': True,
                    'result_count': len(results),
                    'avg_score': avg_score,
                    'max_score': max_score,
                    'relevant_count': relevant_count,
                    'search_time': search_time,
                    'results': [{'id': r.id, 'text': r.text[:100], 'score': r.rerank_score if r.rerank_score is not None else r.pinecone_score} 
                             for r in results[:3]]  # Top 3 for analysis
                }
            else:
                return {
                    'success': False,
                    'result_count': 0,
                    'avg_score': 0.0,
                    'max_score': 0.0,
                    'relevant_count': 0,
                    'search_time': search_time,
                    'results': []
                }
                
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'search_time': time.time() - start_time
            }
    
    def run_comparison_test(self):
        """Run the complete comparison test"""
        
        self.logger.info("🧪 Starting Chunking Strategy Retrieval Comparison")
        self.logger.info("=" * 60)
        
        queries = self.get_test_queries()
        namespaces = ['business_aware-test', 'semantic-test']
        
        all_results = {}
        
        # Test each query against both namespaces
        for i, query in enumerate(queries, 1):
            self.logger.info(f"\n📝 Query {i}/{len(queries)}: {query}")
            
            query_results = {}
            
            for namespace in namespaces:
                strategy = namespace.split('-')[0]  # business_aware or semantic
                result = self.test_retrieval_performance(query, namespace)
                query_results[strategy] = result
                
                if result.get('success'):
                    self.logger.info(f"  ✅ {strategy}: {result['result_count']} results, "
                                   f"avg score: {result['avg_score']:.3f}, "
                                   f"time: {result['search_time']:.3f}s")
                else:
                    self.logger.error(f"  ❌ {strategy}: Failed")
            
            all_results[query] = query_results
        
        # Analyze results
        analysis = self._analyze_results(all_results)
        
        # Save detailed results
        self._save_results(all_results, analysis)
        
        # Display summary
        self._display_summary(analysis)
    
    def _analyze_results(self, results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze the comparison results"""
        
        strategies = ['business_aware', 'semantic']
        analysis = {strategy: {
            'total_queries': 0,
            'successful_queries': 0,
            'total_results': 0,
            'avg_score': 0.0,
            'avg_search_time': 0.0,
            'relevant_results': 0,
            'query_scores': []
        } for strategy in strategies}
        
        for query, query_results in results.items():
            for strategy in strategies:
                result = query_results.get(strategy, {})
                stats = analysis[strategy]
                
                stats['total_queries'] += 1
                
                if result.get('success'):
                    stats['successful_queries'] += 1
                    stats['total_results'] += result.get('result_count', 0)
                    stats['relevant_results'] += result.get('relevant_count', 0)
                    stats['avg_search_time'] += result.get('search_time', 0)
                    
                    score = result.get('avg_score', 0)
                    stats['query_scores'].append(score)
        
        # Calculate averages
        for strategy in strategies:
            stats = analysis[strategy]
            if stats['successful_queries'] > 0:
                stats['avg_score'] = sum(stats['query_scores']) / len(stats['query_scores'])
                stats['avg_search_time'] = stats['avg_search_time'] / stats['successful_queries']
                stats['avg_results_per_query'] = stats['total_results'] / stats['successful_queries']
                stats['relevant_percentage'] = (stats['relevant_results'] / max(stats['total_results'], 1)) * 100
        
        return analysis
    
    def _save_results(self, results: Dict[str, Any], analysis: Dict[str, Any]):
        """Save detailed results to files"""
        
        # Save detailed results
        detailed_file = f"chunking_retrieval_detailed_{self.test_id}.json"
        with open(detailed_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Save analysis summary
        summary_file = f"chunking_retrieval_summary_{self.test_id}.json"
        with open(summary_file, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        self.logger.success(f"💾 Results saved:")
        self.logger.info(f"   📄 Detailed: {detailed_file}")
        self.logger.info(f"   📊 Summary: {summary_file}")
    
    def _display_summary(self, analysis: Dict[str, Any]):
        """Display comparison summary"""
        
        self.logger.info("\n" + "=" * 60)
        self.logger.success("🎯 CHUNKING STRATEGY COMPARISON RESULTS")
        self.logger.info("=" * 60)
        
        business_stats = analysis['business_aware']
        semantic_stats = analysis['semantic']
        
        self.logger.info("📊 BUSINESS-AWARE CHUNKING:")
        self.logger.info(f"   ✅ Successful queries: {business_stats['successful_queries']}/{business_stats['total_queries']}")
        self.logger.info(f"   📄 Avg results per query: {business_stats.get('avg_results_per_query', 0):.1f}")
        self.logger.info(f"   ⭐ Avg relevance score: {business_stats['avg_score']:.3f}")
        self.logger.info(f"   🎯 Relevant results: {business_stats.get('relevant_percentage', 0):.1f}%")
        self.logger.info(f"   ⏱️  Avg search time: {business_stats['avg_search_time']:.3f}s")
        
        self.logger.info("\n📊 SEMANTIC CHUNKING:")
        self.logger.info(f"   ✅ Successful queries: {semantic_stats['successful_queries']}/{semantic_stats['total_queries']}")
        self.logger.info(f"   📄 Avg results per query: {semantic_stats.get('avg_results_per_query', 0):.1f}")
        self.logger.info(f"   ⭐ Avg relevance score: {semantic_stats['avg_score']:.3f}")
        self.logger.info(f"   🎯 Relevant results: {semantic_stats.get('relevant_percentage', 0):.1f}%")
        self.logger.info(f"   ⏱️  Avg search time: {semantic_stats['avg_search_time']:.3f}s")
        
        # Determine winner
        self.logger.info("\n🏆 COMPARISON:")
        
        if business_stats['avg_score'] > semantic_stats['avg_score']:
            score_winner = "Business-Aware"
            score_diff = business_stats['avg_score'] - semantic_stats['avg_score']
        else:
            score_winner = "Semantic"
            score_diff = semantic_stats['avg_score'] - business_stats['avg_score']
        
        if business_stats['avg_search_time'] < semantic_stats['avg_search_time']:
            speed_winner = "Business-Aware"
            speed_diff = semantic_stats['avg_search_time'] - business_stats['avg_search_time']
        else:
            speed_winner = "Semantic" 
            speed_diff = business_stats['avg_search_time'] - semantic_stats['avg_search_time']
        
        self.logger.info(f"   🥇 Higher relevance: {score_winner} (+{score_diff:.3f})")
        self.logger.info(f"   🚀 Faster search: {speed_winner} (+{speed_diff:.3f}s)")
        
        # Overall recommendation
        if score_winner == speed_winner:
            self.logger.success(f"\n🎉 WINNER: {score_winner} chunking performs better overall!")
        else:
            self.logger.info(f"\n🤔 MIXED RESULTS: {score_winner} is more relevant, {speed_winner} is faster")

def main():
    """Main function"""
    
    print("🧪 Chunking Strategy Retrieval Comparison Test")
    print("=" * 50)
    print("Comparing business_aware-test vs semantic-test namespaces")
    print()
    
    try:
        tester = ChunkingRetrievalTester()
        tester.run_comparison_test()
        print("\n✅ Test completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 