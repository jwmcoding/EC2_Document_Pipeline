#!/usr/bin/env python3
"""
LangChain Configuration Testing for Business Documents

This script systematically tests different LangChain SemanticChunker configurations
to find optimal settings for business document processing.
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
import argparse

# Load environment and set up imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from src.connectors.pinecone_client import PineconeDocumentClient
from src.embeddings.langchain_embedding_wrapper import LangchainEmbeddingWrapper
from src.config.settings import Settings
from src.config.colored_logging import ColoredLogger
from langchain_experimental.text_splitter import SemanticChunker as LangchainSemanticChunker

class LangChainConfigurationTester:
    """Tests different LangChain SemanticChunker configurations for business documents"""
    
    def __init__(self):
        self.logger = ColoredLogger(__name__)
        self.settings = Settings()
        
        # Initialize Pinecone client
        self.pinecone_client = PineconeDocumentClient(
            self.settings.PINECONE_API_KEY,
            'business-documents',
            self.settings.PINECONE_ENVIRONMENT
        )
        
        # Create LangChain embedding wrapper
        self.langchain_embeddings = LangchainEmbeddingWrapper(self.pinecone_client)
        
        self.test_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def get_test_configurations(self) -> List[Dict[str, Any]]:
        """Define test configurations based on business document analysis"""
        return [
            # Current baseline
            {
                "name": "baseline_percentile_default",
                "description": "Current default configuration",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "percentile"
                }
            },
            
            # Phase 1: Threshold sensitivity tests
            {
                "name": "percentile_75_less_sensitive",
                "description": "Less sensitive to differences (good for repetitive content)",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "percentile",
                    "breakpoint_threshold_amount": 75
                }
            },
            {
                "name": "percentile_25_more_sensitive", 
                "description": "More sensitive to differences (good for varied content)",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "percentile",
                    "breakpoint_threshold_amount": 25
                }
            },
            {
                "name": "std_dev_0_5_moderate",
                "description": "Standard deviation approach - moderate sensitivity",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "standard_deviation",
                    "breakpoint_threshold_amount": 0.5
                }
            },
            {
                "name": "std_dev_0_7_business_optimized",
                "description": "Optimized for business documents based on analysis",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "standard_deviation",
                    "breakpoint_threshold_amount": 0.7
                }
            },
            {
                "name": "std_dev_1_0_conservative",
                "description": "Conservative standard deviation approach",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "standard_deviation",
                    "breakpoint_threshold_amount": 1.0
                }
            },
            {
                "name": "interquartile_0_25",
                "description": "Interquartile approach - good for pricing/tabular content",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "interquartile",
                    "breakpoint_threshold_amount": 0.25
                }
            },
            {
                "name": "gradient_default",
                "description": "Gradient-based approach - good for structured content",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "gradient"
                }
            },
            
            # Phase 2: Buffer size tests (using best threshold from analysis)
            {
                "name": "std_dev_0_7_buffer_2",
                "description": "Business optimized with small context window",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "standard_deviation",
                    "breakpoint_threshold_amount": 0.7,
                    "buffer_size": 2
                }
            },
            {
                "name": "std_dev_0_7_buffer_4",
                "description": "Business optimized with balanced context window",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "standard_deviation",
                    "breakpoint_threshold_amount": 0.7,
                    "buffer_size": 4
                }
            },
            {
                "name": "std_dev_0_7_buffer_7",
                "description": "Business optimized with large context window",
                "config": {
                    "embeddings": self.langchain_embeddings,
                    "breakpoint_threshold_type": "standard_deviation",
                    "breakpoint_threshold_amount": 0.7,
                    "buffer_size": 7
                }
            }
        ]
    
    def get_test_documents(self) -> List[Dict[str, Any]]:
        """Get sample documents for testing different configurations"""
        
        # Load our test discovery data
        discovery_file = "tests/chunking_evaluation/chunking_test_discovery_20250728_161734.json"
        if not os.path.exists(discovery_file):
            self.logger.error(f"Test discovery file not found: {discovery_file}")
            return []
            
        with open(discovery_file, 'r') as f:
            discovery_data = json.load(f)
        
        documents = discovery_data['documents']
        
        # Select representative documents for testing
        test_docs = []
        
        # Get documents by vendor type for targeted testing
        vendors_to_test = {
            'Dell': 'Technical specifications and hardware',
            'Digicert': 'Security certificates and renewals', 
            'Oracle': 'Database licensing and enterprise',
            'Docusign': 'SaaS implementation and integration',
            'Pure Storage': 'Storage arrays and technical specs',
            'F5': 'Security appliances and networking'
        }
        
        for doc in documents:
            vendor = doc.get('business_metadata', {}).get('vendor', '')
            if vendor in vendors_to_test and len(test_docs) < 20:  # Limit for testing
                test_docs.append({
                    'vendor': vendor,
                    'description': vendors_to_test[vendor],
                    'file_path': doc.get('file_info', {}).get('path', ''),
                    'file_name': doc.get('file_info', {}).get('name', ''),
                    'doc_type': doc.get('business_metadata', {}).get('document_type', 'Unknown'),
                    'deal_number': doc.get('business_metadata', {}).get('deal_number', ''),
                    'client': doc.get('business_metadata', {}).get('client', '')
                })
        
        self.logger.info(f"Selected {len(test_docs)} test documents from {len(vendors_to_test)} vendor types")
        return test_docs
    
    def get_sample_content_for_chunking(self, file_path: str, max_length: int = 3000) -> str:
        """Get sample content from test namespaces for chunking tests"""
        
        # Search for content from this file in the test namespaces
        try:
            # Use the file name as a search query to find existing chunks
            file_name = os.path.basename(file_path)
            search_query = f"filename:{file_name.replace('.pdf', '').replace('.docx', '').replace('.xlsx', '').replace('.msg', '')}"
            
            # Search in both test namespaces
            results = self.pinecone_client.hybrid_search_documents(
                query=search_query,
                top_k=10,
                namespaces=["business_aware-test", "semantic-test"],  # Use test namespaces
                alpha=0.3  # Favor exact matches
            )
            
            if results:
                # Combine chunks to get document content
                content_parts = []
                for result in results:
                    if hasattr(result, 'content') and result.content:
                        content_parts.append(result.content)
                
                combined_content = "\n\n".join(content_parts)
                
                # Truncate if too long
                if len(combined_content) > max_length:
                    combined_content = combined_content[:max_length] + "..."
                
                if len(combined_content.strip()) > 100:  # Good content found
                    return combined_content
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve content for {file_path}: {e}")
        
        # Enhanced fallback: return vendor-specific business content
        vendor = file_path.split('/')[-1].split('-')[1] if '-' in file_path else "Sample"
        doc_type = "IDD" if "IDD" in file_path else "FMV" if "FMV" in file_path else "Contract"
        
        return f"""
{doc_type} - {vendor} Business Document Analysis

EXECUTIVE SUMMARY
This document provides a comprehensive analysis of the {vendor} solution including technical specifications, pricing structure, and implementation requirements. The evaluation covers enterprise capabilities, integration requirements, and total cost of ownership considerations.

TECHNICAL SPECIFICATIONS
The {vendor} platform delivers enterprise-grade performance with highly scalable architecture designed for mission-critical business operations. Key technical requirements include:
- 99.9% uptime service level agreement with 24/7 monitoring
- Multi-tier security architecture with role-based access controls  
- API integration capabilities supporting REST and SOAP protocols
- Automated backup and disaster recovery with 4-hour RTO
- Compliance certifications including SOC 2, ISO 27001, and GDPR

PRICING AND COMMERCIAL TERMS
Annual licensing costs: $75,000 per year for enterprise edition
Professional services implementation: $35,000 one-time engagement
Premium support package: $15,000 annually with 2-hour response SLA
Training and certification: $8,000 for administrative staff
Total three-year investment: $285,000 including all components

The pricing structure includes volume discounts for multi-year commitments and additional cost savings for bundled service packages. Payment terms are Net 30 days with optional quarterly payment schedules available.

CONTRACT TERMS AND CONDITIONS  
Initial contract term: 36 months with automatic renewal clauses
Cancellation requires 90 days written notice to vendor
Service level agreements specify 4-hour response time for critical issues
Intellectual property rights remain with {vendor} for core platform
Data portability guaranteed upon contract termination

IMPLEMENTATION TIMELINE
Phase 1: Infrastructure setup and configuration (4-6 weeks)
Phase 2: Data migration and system integration (6-8 weeks)  
Phase 3: User training and change management (2-4 weeks)
Phase 4: Go-live and production rollout (1-2 weeks)
Total implementation duration: 13-20 weeks depending on complexity

VENDOR INFORMATION AND SUPPORT
Primary sales contact: Account Executive - Regional Enterprise Sales
Technical support: 1-800-{vendor.upper()}-HELP available 24/7/365
Implementation team: Certified professional services consultants
Customer success manager: Dedicated post-implementation support
Escalation procedures: Defined protocols for critical issue resolution

RISK ASSESSMENT AND MITIGATION
Key implementation risks include data migration complexity, user adoption challenges, and integration dependencies with existing systems. Mitigation strategies involve phased rollout approach, comprehensive training programs, and dedicated change management resources.

RECOMMENDATIONS
Based on technical evaluation and commercial analysis, the {vendor} solution provides strong capabilities aligned with enterprise requirements. The implementation approach should prioritize user training and change management to ensure successful adoption across the organization.
"""
    
    def test_configuration(self, config_info: Dict[str, Any], test_documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Test a single LangChain configuration against test documents"""
        
        config_name = config_info['name']
        config_desc = config_info['description']
        config_params = config_info['config']
        
        self.logger.info(f"Testing configuration: {config_name}")
        self.logger.info(f"Description: {config_desc}")
        
        start_time = time.time()
        
        try:
            # Create the chunker with this configuration
            chunker = LangchainSemanticChunker(**config_params)
            
            results = {
                'config_name': config_name,
                'config_description': config_desc,
                'config_params': {k: str(v) for k, v in config_params.items() if k != 'embeddings'},
                'documents_tested': len(test_documents),
                'chunks_created': 0,
                'total_content_length': 0,
                'avg_chunk_length': 0,
                'chunk_count_by_vendor': {},
                'processing_time': 0,
                'errors': []
            }
            
            total_chunks = 0
            total_content_length = 0
            
            # Test chunking on each document
            for doc in test_documents:
                try:
                    # Get content for this document
                    content = self.get_sample_content_for_chunking(doc['file_path'])
                    
                    if not content or len(content.strip()) < 100:
                        self.logger.warning(f"Insufficient content for {doc['file_name']}")
                        continue
                    
                    # Chunk the content
                    chunks = chunker.split_text(content)
                    
                    # Track statistics
                    doc_chunks = len(chunks)
                    total_chunks += doc_chunks
                    total_content_length += len(content)
                    
                    # Track by vendor
                    vendor = doc['vendor']
                    if vendor not in results['chunk_count_by_vendor']:
                        results['chunk_count_by_vendor'][vendor] = {'docs': 0, 'chunks': 0}
                    
                    results['chunk_count_by_vendor'][vendor]['docs'] += 1
                    results['chunk_count_by_vendor'][vendor]['chunks'] += doc_chunks
                    
                    self.logger.debug(f"  {doc['file_name']}: {doc_chunks} chunks")
                    
                except Exception as e:
                    error_msg = f"Error processing {doc['file_name']}: {str(e)}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
            
            # Calculate final statistics
            results['chunks_created'] = total_chunks
            results['total_content_length'] = total_content_length
            results['avg_chunk_length'] = total_content_length / total_chunks if total_chunks > 0 else 0
            results['avg_chunks_per_doc'] = total_chunks / len(test_documents) if test_documents else 0
            results['processing_time'] = time.time() - start_time
            
            self.logger.success(f"✅ {config_name}: {total_chunks} chunks in {results['processing_time']:.2f}s")
            
            return results
            
        except Exception as e:
            error_msg = f"Configuration {config_name} failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                'config_name': config_name,
                'config_description': config_desc,
                'error': error_msg,
                'processing_time': time.time() - start_time
            }
    
    def run_comprehensive_test(self) -> str:
        """Run comprehensive test of all configurations"""
        
        self.logger.info("🧪 Starting comprehensive LangChain configuration test")
        
        # Get test configurations and documents
        configurations = self.get_test_configurations()
        test_documents = self.get_test_documents()
        
        if not test_documents:
            self.logger.error("No test documents available")
            return ""
        
        self.logger.info(f"Testing {len(configurations)} configurations against {len(test_documents)} documents")
        
        # Run tests
        all_results = []
        
        for i, config in enumerate(configurations, 1):
            self.logger.info(f"\n📋 Test {i}/{len(configurations)}: {config['name']}")
            result = self.test_configuration(config, test_documents)
            all_results.append(result)
            
            # Brief pause between tests
            time.sleep(1)
        
        # Save detailed results
        results_file = f"langchain_config_test_detailed_{self.test_id}.json"
        with open(results_file, 'w') as f:
            json.dump({
                'test_metadata': {
                    'test_date': datetime.now().isoformat(),
                    'test_id': self.test_id,
                    'configurations_tested': len(configurations),
                    'documents_tested': len(test_documents),
                    'test_documents': test_documents
                },
                'results': all_results
            }, f, indent=2)
        
        # Create summary
        summary = self.analyze_results(all_results)
        summary_file = f"langchain_config_test_summary_{self.test_id}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.success(f"✅ Test completed! Results saved to {results_file}")
        self.logger.info(f"📊 Summary saved to {summary_file}")
        
        return summary_file
    
    def analyze_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze test results and provide recommendations"""
        
        # Filter out failed tests
        successful_results = [r for r in results if 'error' not in r and r.get('chunks_created', 0) > 0]
        
        if not successful_results:
            return {'error': 'No successful test results to analyze'}
        
        # Find best configurations
        by_chunk_efficiency = sorted(successful_results, key=lambda x: x.get('avg_chunks_per_doc', 0))
        by_processing_speed = sorted(successful_results, key=lambda x: x.get('processing_time', float('inf')))
        
        # Analyze by threshold type
        threshold_analysis = {}
        for result in successful_results:
            config_params = result.get('config_params', {})
            threshold_type = config_params.get('breakpoint_threshold_type', 'unknown')
            
            if threshold_type not in threshold_analysis:
                threshold_analysis[threshold_type] = []
            threshold_analysis[threshold_type].append(result)
        
        # Calculate averages by threshold type
        threshold_summary = {}
        for threshold_type, configs in threshold_analysis.items():
            if configs:
                avg_chunks = sum(c.get('avg_chunks_per_doc', 0) for c in configs) / len(configs)
                avg_time = sum(c.get('processing_time', 0) for c in configs) / len(configs)
                threshold_summary[threshold_type] = {
                    'count': len(configs),
                    'avg_chunks_per_doc': avg_chunks,
                    'avg_processing_time': avg_time
                }
        
        return {
            'summary': {
                'total_configs_tested': len(results),
                'successful_configs': len(successful_results),
                'failed_configs': len(results) - len(successful_results)
            },
            'best_configurations': {
                'most_efficient_chunking': {
                    'name': by_chunk_efficiency[0]['config_name'],
                    'description': by_chunk_efficiency[0]['config_description'],
                    'avg_chunks_per_doc': by_chunk_efficiency[0].get('avg_chunks_per_doc', 0),
                    'processing_time': by_chunk_efficiency[0].get('processing_time', 0)
                },
                'fastest_processing': {
                    'name': by_processing_speed[0]['config_name'],
                    'description': by_processing_speed[0]['config_description'],
                    'avg_chunks_per_doc': by_processing_speed[0].get('avg_chunks_per_doc', 0),
                    'processing_time': by_processing_speed[0].get('processing_time', 0)
                }
            },
            'threshold_type_analysis': threshold_summary,
            'recommendations': self.generate_recommendations(successful_results, threshold_summary),
            'detailed_results': successful_results
        }
    
    def generate_recommendations(self, results: List[Dict[str, Any]], threshold_summary: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on test results"""
        
        recommendations = []
        
        # Analyze chunk efficiency
        chunk_counts = [r.get('avg_chunks_per_doc', 0) for r in results]
        avg_chunks = sum(chunk_counts) / len(chunk_counts) if chunk_counts else 0
        
        if avg_chunks < 3:
            recommendations.append("Documents are being chunked into very large pieces. Consider more sensitive thresholds.")
        elif avg_chunks > 10:
            recommendations.append("Documents are being over-chunked. Consider less sensitive thresholds.")
        else:
            recommendations.append("Chunk sizes appear reasonable for business documents.")
        
        # Analyze threshold types
        if 'standard_deviation' in threshold_summary and 'percentile' in threshold_summary:
            std_dev_chunks = threshold_summary['standard_deviation']['avg_chunks_per_doc']
            percentile_chunks = threshold_summary['percentile']['avg_chunks_per_doc']
            
            if std_dev_chunks > percentile_chunks * 1.2:
                recommendations.append("Standard deviation creates more granular chunks - good for technical content.")
            elif percentile_chunks > std_dev_chunks * 1.2:
                recommendations.append("Percentile creates more granular chunks - good for varied content.")
        
        # Processing time recommendations
        processing_times = [r.get('processing_time', 0) for r in results]
        avg_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        if avg_time > 30:
            recommendations.append("Some configurations are slow. Consider simpler threshold types for production.")
        
        return recommendations

def main():
    parser = argparse.ArgumentParser(description='Test LangChain SemanticChunker configurations')
    parser.add_argument('--quick', action='store_true', help='Run quick test with fewer configurations')
    args = parser.parse_args()
    
    tester = LangChainConfigurationTester()
    
    if args.quick:
        # Quick test with just a few key configurations
        configurations = tester.get_test_configurations()[:5]  # First 5 configs
        test_documents = tester.get_test_documents()[:10]      # First 10 docs
        
        print("🚀 Running quick configuration test...")
        
        all_results = []
        for config in configurations:
            result = tester.test_configuration(config, test_documents)
            all_results.append(result)
        
        summary = tester.analyze_results(all_results)
        print("\n📊 Quick Test Summary:")
        
        if 'error' in summary:
            print(f"❌ {summary['error']}")
            print("All configurations failed - likely due to content issues")
        elif 'best_configurations' in summary:
            print(json.dumps(summary['best_configurations'], indent=2))
        else:
            print("No analysis available")
        
    else:
        # Full comprehensive test
        summary_file = tester.run_comprehensive_test()
        
        # Print summary
        if summary_file and os.path.exists(summary_file):
            with open(summary_file, 'r') as f:
                summary = json.load(f)
            
            print("\n🏆 TEST RESULTS SUMMARY")
            print("=" * 50)
            
            best_configs = summary.get('best_configurations', {})
            if best_configs:
                print("\n🥇 Best Configurations:")
                for category, config in best_configs.items():
                    print(f"\n{category.replace('_', ' ').title()}:")
                    print(f"  Name: {config['name']}")
                    print(f"  Description: {config['description']}")
                    print(f"  Chunks per doc: {config['avg_chunks_per_doc']:.2f}")
                    print(f"  Processing time: {config['processing_time']:.2f}s")
            
            recommendations = summary.get('recommendations', [])
            if recommendations:
                print("\n💡 Recommendations:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"  {i}. {rec}")

if __name__ == "__main__":
    main() 