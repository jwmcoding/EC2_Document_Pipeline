#!/usr/bin/env python3
"""
Batch Classification Results Checker

This script checks the status of batch classification jobs and retrieves results
when they're complete, updating the discovery JSON with classification data.

Usage:
  python check_batch_classification.py --job-id batch_abc123
  python check_batch_classification.py --discovery-file discovery_results.json --update
"""

import os
import sys
import argparse
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from src.config.colored_logging import ColoredLogger, setup_colored_logging
from src.config.settings import Settings


class BatchResultsChecker:
    """Handles checking and retrieving batch classification results"""
    
    def __init__(self):
        setup_colored_logging()
        self.logger = ColoredLogger("batch_checker")
        self.settings = Settings()
        
        if not self.settings.OPENAI_API_KEY:
            self.logger.error("❌ OPENAI_API_KEY not found in environment")
            sys.exit(1)
            
        from openai import OpenAI
        self.client = OpenAI(api_key=self.settings.OPENAI_API_KEY)
    
    def check_job_status(self, job_id: str) -> Dict[str, Any]:
        """Check the status of a specific batch job"""
        try:
            batch_job = self.client.batches.retrieve(job_id)
            
            status_info = {
                "id": batch_job.id,
                "status": batch_job.status,
                "created_at": datetime.fromtimestamp(batch_job.created_at).isoformat(),
                "completed_at": datetime.fromtimestamp(batch_job.completed_at).isoformat() if batch_job.completed_at else None,
                "failed_at": datetime.fromtimestamp(batch_job.failed_at).isoformat() if batch_job.failed_at else None,
                "expires_at": datetime.fromtimestamp(batch_job.expires_at).isoformat() if batch_job.expires_at else None,
                "request_counts": batch_job.request_counts.__dict__ if batch_job.request_counts else None,
                "metadata": batch_job.metadata
            }
            
            return status_info
            
        except Exception as e:
            self.logger.error(f"❌ Error checking job status: {e}")
            return None
    
    def retrieve_results(self, job_id: str) -> List[Dict[str, Any]]:
        """Retrieve results from a completed batch job"""
        try:
            batch_job = self.client.batches.retrieve(job_id)
            
            if batch_job.status != "completed":
                raise ValueError(f"Batch job not completed. Status: {batch_job.status}")
            
            # Download results file
            result_file_id = batch_job.output_file_id
            result_content = self.client.files.content(result_file_id).content
            
            # Parse JSONL results
            results = []
            for line in result_content.decode('utf-8').strip().split('\n'):
                if line:
                    result = json.loads(line)
                    results.append(result)
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Error retrieving results: {e}")
            return []
    
    def update_discovery_with_results(self, discovery_file: str, batch_results: List[Dict[str, Any]]) -> bool:
        """Update discovery JSON file with batch classification results"""
        try:
            # Load discovery data
            with open(discovery_file, 'r') as f:
                discovery_data = json.load(f)
            
            # Create mapping from custom_id to results
            results_map = {}
            total_tokens_used = 0
            
            for result in batch_results:
                if result.get('response') and result['response'].get('body'):
                    custom_id = result['custom_id']
                    response_body = result['response']['body']
                    
                    # Extract usage information
                    usage = response_body.get('usage', {})
                    total_tokens_used += usage.get('total_tokens', 0)
                    
                    # Extract classification from first choice
                    choices = response_body.get('choices', [])
                    if choices:
                        content = choices[0].get('message', {}).get('content', '')
                        try:
                            classification_data = json.loads(content)
                            results_map[custom_id] = {
                                'classification': classification_data,
                                'tokens_used': usage.get('total_tokens', 0)
                            }
                        except json.JSONDecodeError:
                            self.logger.warning(f"⚠️ Failed to parse classification JSON for {custom_id}")
            
            # Update documents with classification results
            updated_count = 0
            
            for doc in discovery_data.get('documents', []):
                # Try to find matching result by document index
                # Custom IDs were in format: doc_{batch_id}_{idx}
                for custom_id, result_data in results_map.items():
                    # Simple matching - could be enhanced with better mapping
                    if f"_{updated_count}" in custom_id:
                        classification = result_data['classification']
                        
                        doc['llm_classification'] = {
                            "document_type": classification.get('document_type', 'Unknown'),
                            "confidence": classification.get('confidence', 0.0),
                            "reasoning": classification.get('reasoning', ''),
                            "classification_method": 'gpt-4.1-mini-batch',
                            "alternative_types": classification.get('alternative_types', []),
                            "tokens_used": result_data['tokens_used'],
                            "classification_timestamp": datetime.now().isoformat(),
                            "batch_processed": True
                        }
                        updated_count += 1
                        break
            
            # Update discovery metadata
            if 'statistics' not in discovery_data['discovery_metadata']:
                discovery_data['discovery_metadata']['statistics'] = {}
            
            stats = discovery_data['discovery_metadata']['statistics']
            stats['classified_documents'] = updated_count
            stats['total_tokens_used'] = total_tokens_used
            stats['batch_processing_completed'] = datetime.now().isoformat()
            
            # Calculate document type distribution
            doc_types = {}
            for doc in discovery_data.get('documents', []):
                if 'llm_classification' in doc:
                    doc_type = doc['llm_classification']['document_type']
                    doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
            
            stats['document_types'] = doc_types
            stats['classification_rate'] = updated_count / len(discovery_data.get('documents', [])) if discovery_data.get('documents') else 0
            
            # Save updated discovery file
            with open(discovery_file, 'w') as f:
                json.dump(discovery_data, f, indent=2)
            
            self.logger.success(f"✅ Updated {updated_count} documents with batch classification results")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error updating discovery file: {e}")
            return False
    
    def calculate_actual_cost(self, batch_results: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calculate actual cost from batch results"""
        total_input_tokens = 0
        total_output_tokens = 0
        
        for result in batch_results:
            if result.get('response') and result['response'].get('body'):
                usage = result['response']['body'].get('usage', {})
                total_input_tokens += usage.get('prompt_tokens', 0)
                total_output_tokens += usage.get('completion_tokens', 0)
        
        # Batch API rates for GPT-4.1-mini (50% discount)
        input_cost = (total_input_tokens / 1_000_000) * 0.20  # $0.20 per 1M tokens
        output_cost = (total_output_tokens / 1_000_000) * 0.80  # $0.80 per 1M tokens
        total_cost = input_cost + output_cost
        
        return {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "total_tokens": total_input_tokens + total_output_tokens,
            "input_cost": input_cost,
            "output_cost": output_cost,
            "total_cost": total_cost
        }
    
    def monitor_job_until_complete(self, job_id: str, check_interval: int = 300) -> bool:
        """Monitor a batch job until completion"""
        self.logger.info(f"🔍 Monitoring batch job: {job_id}")
        self.logger.info(f"⏰ Checking every {check_interval} seconds...")
        
        while True:
            status_info = self.check_job_status(job_id)
            
            if not status_info:
                return False
            
            status = status_info['status']
            
            if status == 'completed':
                self.logger.success(f"✅ Batch job completed!")
                return True
            elif status == 'failed':
                self.logger.error(f"❌ Batch job failed!")
                return False
            elif status == 'expired':
                self.logger.error(f"❌ Batch job expired!")
                return False
            else:
                # Still processing
                request_counts = status_info.get('request_counts', {})
                if request_counts:
                    completed = request_counts.get('completed', 0)
                    total = request_counts.get('total', 0)
                    if total > 0:
                        progress = (completed / total) * 100
                        self.logger.info(f"📊 Progress: {completed}/{total} ({progress:.1f}%) - Status: {status}")
                    else:
                        self.logger.info(f"📊 Status: {status}")
                else:
                    self.logger.info(f"📊 Status: {status}")
                
                # Wait before next check
                time.sleep(check_interval)


def create_argument_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description="Check and retrieve batch classification results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check status of a specific batch job
  python check_batch_classification.py --job-id batch_abc123

  # Monitor job until completion and update discovery file
  python check_batch_classification.py --job-id batch_abc123 --discovery-file discovery_results.json --update --monitor

  # Check all pending batch jobs in a discovery file
  python check_batch_classification.py --discovery-file discovery_results.json --check-all
        """
    )
    
    parser.add_argument("--job-id", type=str,
                       help="Batch job ID to check")
    parser.add_argument("--discovery-file", type=str,
                       help="Discovery JSON file to update with results")
    parser.add_argument("--update", action="store_true",
                       help="Update discovery file with batch results")
    parser.add_argument("--monitor", action="store_true",
                       help="Monitor job until completion")
    parser.add_argument("--check-all", action="store_true",
                       help="Check all pending batch jobs in discovery file")
    parser.add_argument("--check-interval", type=int, default=300,
                       help="Check interval in seconds when monitoring (default: 300)")
    
    return parser


def main():
    """Main entry point"""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    checker = BatchResultsChecker()
    
    if args.job_id:
        # Check specific job
        if args.monitor:
            # Monitor until completion
            success = checker.monitor_job_until_complete(args.job_id, args.check_interval)
            if not success:
                return
        
        # Check current status
        status_info = checker.check_job_status(args.job_id)
        
        if status_info:
            checker.logger.info(f"📊 Batch Job Status:")
            checker.logger.info(f"   ID: {status_info['id']}")
            checker.logger.info(f"   Status: {status_info['status']}")
            checker.logger.info(f"   Created: {status_info['created_at']}")
            
            if status_info['completed_at']:
                checker.logger.info(f"   Completed: {status_info['completed_at']}")
            
            if status_info['request_counts']:
                counts = status_info['request_counts']
                checker.logger.info(f"   Progress: {counts.get('completed', 0)}/{counts.get('total', 0)}")
        
        # Retrieve and update results if job is complete and update is requested
        if status_info and status_info['status'] == 'completed' and args.update and args.discovery_file:
            checker.logger.info(f"📥 Retrieving batch results...")
            
            results = checker.retrieve_results(args.job_id)
            if results:
                checker.logger.info(f"✅ Retrieved {len(results)} results")
                
                # Calculate actual costs
                cost_info = checker.calculate_actual_cost(results)
                checker.logger.info(f"💰 Actual Cost: ${cost_info['total_cost']:.2f}")
                checker.logger.info(f"   Input tokens: {cost_info['input_tokens']:,}")
                checker.logger.info(f"   Output tokens: {cost_info['output_tokens']:,}")
                
                # Update discovery file
                success = checker.update_discovery_with_results(args.discovery_file, results)
                if success:
                    checker.logger.success(f"✅ Discovery file updated: {args.discovery_file}")
    
    elif args.discovery_file and args.check_all:
        # Check all pending batch jobs in discovery file
        try:
            with open(args.discovery_file, 'r') as f:
                discovery_data = json.load(f)
            
            batch_jobs = discovery_data.get('discovery_metadata', {}).get('batch_jobs', [])
            
            if not batch_jobs:
                checker.logger.info("📝 No batch jobs found in discovery file")
                return
            
            for job_info in batch_jobs:
                job_id = job_info.get('job_id')
                if job_id:
                    checker.logger.info(f"\n🔍 Checking job: {job_id}")
                    status_info = checker.check_job_status(job_id)
                    
                    if status_info:
                        checker.logger.info(f"   Status: {status_info['status']}")
                        
                        if status_info['status'] == 'completed' and args.update:
                            results = checker.retrieve_results(job_id)
                            if results:
                                checker.update_discovery_with_results(args.discovery_file, results)
                    
        except Exception as e:
            checker.logger.error(f"❌ Error checking discovery file: {e}")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
