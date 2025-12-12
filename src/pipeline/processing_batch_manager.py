"""
Processing Batch Manager for Enhanced Document Classification

Manages OpenAI Batch API operations for document processing with enhanced V3 classification.
Adapted from discovery BatchAPIManager to handle full document content and structured outputs.
"""

import os
import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import asdict

# Import the enhanced classification schema
from src.classification.llm_document_classifier import ENHANCED_CLASSIFICATION_SCHEMA
from src.config.colored_logging import ColoredLogger
from src.models.document_models import DocumentMetadata


class ProcessingBatchManager:
    """Manages OpenAI Batch API operations for enhanced document processing classification"""
    
    def __init__(self, openai_api_key: str):
        from openai import OpenAI
        self.client = OpenAI(api_key=openai_api_key)
        self.logger = ColoredLogger("processing_batch")
        
    def create_enhanced_classification_batch(self, processing_requests: List[Dict], batch_id: str) -> str:
        """Create a batch enhanced classification job and return the batch job ID"""
        
        # Create JSONL file for batch processing
        jsonl_path = f"batch_processing_{batch_id}.jsonl"
        mapping_path = f"batch_mapping_{batch_id}.json"
        mappings: List[Dict[str, str]] = []
        
        with open(jsonl_path, 'w') as f:
            for idx, request in enumerate(processing_requests):
                # Extract request data
                doc_metadata = request['doc_metadata']
                content_preview = request['content_preview']
                
                # Create enhanced classification request using V3 prompt and schema
                classification_request = {
                    "custom_id": f"proc_{batch_id}_{idx}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-4.1-mini",
                        "temperature": 0.1,
                        "response_format": {
                            "type": "json_schema",
                            "json_schema": {
                                "name": "document_classification",
                                "schema": ENHANCED_CLASSIFICATION_SCHEMA
                            }
                        },
                        "messages": [
                            {
                                "role": "user",
                                "content": f"""You are an expert business document analyst. Analyze this document and extract comprehensive metadata.

DOCUMENT TYPES (choose exactly one):
1. Email: Email messages, correspondence, communication
2. FMV Report: Fair Market Value reports, pricing analysis, market valuations
3. Quote/Proposal: Sales quotes, business proposals, RFP responses, pricing quotes
4. Contract/SoW/MSA: Contracts, statements of work, agreements, deliverables specs
5. Product Lit: Product literature, marketing materials, datasheets, brochures

PRICING DEPTH EXAMPLES:
• LOW: Basic mentions ("competitive pricing", "cost-effective solution")
• MEDIUM: Some specific prices ("$1,200/month", "20% discount") 
• HIGH: Detailed pricing tables, breakdowns, multiple price points, cost analysis

COMMERCIAL TERMS DEPTH EXAMPLES:
• LOW: Basic mentions ("standard terms", "negotiable")
• MEDIUM: Some specific terms ("30-day payment", "annual contract")
• HIGH: Detailed terms, conditions, SLAs, payment schedules, deliverables, penalties

DOCUMENT CONTEXT:
Filename: {doc_metadata['name']}
File Type: {doc_metadata['file_type']}
Vendor: {doc_metadata.get('vendor', 'Unknown')}
Client: {doc_metadata.get('client', 'Unknown')}
Deal Number: {doc_metadata.get('deal_number', 'Unknown')}
Page Count: {request.get('page_count', 'Unknown')}
Word Count: {request.get('word_count', 'Unknown')}

CONTENT PREVIEW:
{content_preview[:4000]}...

ANALYSIS INSTRUCTIONS:
1. Classify document type based on content and context
2. Write exactly 2 sentences summarizing the document content
3. Rate product pricing depth: low/medium/high based on examples above
4. Rate commercial terms depth: low/medium/high based on examples above
5. For Quotes/Contracts: extract term start/end dates in YYYY-MM-DD format (null if not found)
6. Identify 3-5 key topics or themes
7. Extract specific vendor products/models mentioned
8. Extract specific pricing indicators (numbers, rates, costs)
9. Provide confidence (0.7-1.0 for clear docs, 0.4-0.7 for unclear)

Return structured JSON matching the required schema."""
                            }
                        ]
                    }
                }
                
                f.write(json.dumps(classification_request) + '\n')
                # Persist mapping between custom_id and document_path for later updates
                mappings.append({
                    "custom_id": classification_request["custom_id"],
                    "document_path": request.get("document_path") or doc_metadata.get("path", "")
                })
        
        # Upload file to OpenAI
        self.logger.info(f"📤 Uploading batch file: {jsonl_path}")
        with open(jsonl_path, 'rb') as f:
            batch_file = self.client.files.create(file=f, purpose="batch")
        
        # Create batch job
        batch_job = self.client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        
        self.logger.success(f"✅ Batch job created: {batch_job.id}")
        self.logger.info(f"📊 Documents queued: {len(processing_requests)}")
        
        # Write mapping file to disk for updater to use
        try:
            mapping_payload = {
                "batch_id": batch_id,
                "openai_job_id": batch_job.id,
                "created_at": datetime.now().isoformat(),
                "mappings": mappings
            }
            with open(mapping_path, 'w') as mf:
                json.dump(mapping_payload, mf, indent=2)
            self.logger.info(f"🗂️ Saved batch mapping: {mapping_path} ({len(mappings)} entries)")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to save batch mapping file: {e}")

        # Maintain an index from job_id → mapping file for easy lookup
        try:
            index_path = "batch_job_index.json"
            index_data: Dict[str, Any] = {}
            if os.path.exists(index_path):
                try:
                    with open(index_path, 'r') as jf:
                        index_data = json.load(jf)
                except Exception:
                    index_data = {}
            index_data[batch_job.id] = {
                "batch_id": batch_id,
                "mapping_path": mapping_path,
                "created_at": datetime.now().isoformat()
            }
            with open(index_path, 'w') as jf:
                json.dump(index_data, jf, indent=2)
            self.logger.info(f"🔗 Updated batch job index: {index_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to update batch job index: {e}")

        # Clean up local JSONL request file
        os.remove(jsonl_path)
        
        return batch_job.id
    
    def collect_enhancement_request(self, doc_metadata: DocumentMetadata, content_preview: str, 
                                   page_count: Optional[int] = None, word_count: Optional[int] = None) -> Dict:
        """Collect an enhancement request for batch processing"""
        return {
            'doc_metadata': asdict(doc_metadata),
            'content_preview': content_preview,
            'page_count': page_count,
            'word_count': word_count,
            'request_id': str(uuid.uuid4()),
            'document_path': doc_metadata.path
        }
    
    def check_batch_status(self, batch_job_id: str) -> Dict[str, Any]:
        """Check the status of a batch job"""
        batch_job = self.client.batches.retrieve(batch_job_id)
        return {
            "id": batch_job.id,
            "status": batch_job.status,
            "created_at": batch_job.created_at,
            "completed_at": batch_job.completed_at,
            "failed_at": batch_job.failed_at,
            "request_counts": batch_job.request_counts.__dict__ if batch_job.request_counts else None
        }
    
    def retrieve_batch_results(self, batch_job_id: str) -> List[Dict[str, Any]]:
        """Retrieve and parse batch job results"""
        batch_job = self.client.batches.retrieve(batch_job_id)
        
        if batch_job.status != "completed":
            raise ValueError(f"Batch job not completed. Status: {batch_job.status}")
        
        # Download results
        result_file_id = batch_job.output_file_id
        result_content = self.client.files.content(result_file_id).content
        
        # Parse JSONL results
        results = []
        for line in result_content.decode('utf-8').strip().split('\n'):
            if line:
                result = json.loads(line)
                results.append(result)
        
        return results
    
    def estimate_batch_cost(self, num_documents: int, avg_tokens_per_request: int = 2000) -> Dict[str, float]:
        """Estimate costs for batch vs immediate enhanced classification"""
        
        # Enhanced classification uses more tokens due to content analysis
        # Estimate tokens (input + output for enhanced classification)
        input_tokens = num_documents * avg_tokens_per_request  # Higher due to content + schema
        output_tokens = num_documents * 300  # Larger structured JSON response
        
        # Batch API costs (50% discount)
        batch_input_cost = (input_tokens / 1_000_000) * 0.20  # $0.20 per 1M tokens  
        batch_output_cost = (output_tokens / 1_000_000) * 0.80  # $0.80 per 1M tokens
        batch_total = batch_input_cost + batch_output_cost
        
        # Immediate API costs
        immediate_input_cost = (input_tokens / 1_000_000) * 0.40  # $0.40 per 1M tokens
        immediate_output_cost = (output_tokens / 1_000_000) * 1.60  # $1.60 per 1M tokens
        immediate_total = immediate_input_cost + immediate_output_cost
        
        return {
            "batch_cost": batch_total,
            "immediate_cost": immediate_total,
            "savings": immediate_total - batch_total,
            "savings_percentage": ((immediate_total - batch_total) / immediate_total) * 100,
            "num_documents": num_documents,
            "estimated_input_tokens": input_tokens,
            "estimated_output_tokens": output_tokens
        }
    
    def calculate_actual_cost(self, batch_results: List[Dict]) -> Dict[str, Any]:
        """Calculate actual cost from batch results"""
        total_input_tokens = 0
        total_output_tokens = 0
        successful_requests = 0
        failed_requests = 0
        
        for result in batch_results:
            if result.get('response') and result['response'].get('body'):
                body = result['response']['body']
                if 'usage' in body:
                    usage = body['usage']
                    total_input_tokens += usage.get('prompt_tokens', 0)
                    total_output_tokens += usage.get('completion_tokens', 0)
                    successful_requests += 1
                else:
                    failed_requests += 1
            else:
                failed_requests += 1
        
        # Calculate actual costs with batch pricing
        actual_input_cost = (total_input_tokens / 1_000_000) * 0.20
        actual_output_cost = (total_output_tokens / 1_000_000) * 0.80
        actual_total_cost = actual_input_cost + actual_output_cost
        
        return {
            "actual_cost": actual_total_cost,
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "total_requests": len(batch_results)
        }
    
    def parse_enhanced_classification_results(self, batch_results: List[Dict]) -> Dict[str, Dict]:
        """Parse batch results into enhanced classification data by document path"""
        parsed_results = {}
        
        for result in batch_results:
            try:
                custom_id = result.get('custom_id', '')
                
                # Extract document index from custom_id (format: proc_{batch_id}_{idx})
                if not custom_id.startswith('proc_'):
                    continue
                
                # Get the response content
                if not (result.get('response') and result['response'].get('body')):
                    self.logger.warning(f"⚠️ No response body for {custom_id}")
                    continue
                
                body = result['response']['body']
                if 'choices' not in body or not body['choices']:
                    self.logger.warning(f"⚠️ No choices in response for {custom_id}")
                    continue
                
                # Parse the enhanced classification JSON
                choice = body['choices'][0]
                if 'message' not in choice or 'content' not in choice['message']:
                    self.logger.warning(f"⚠️ No message content for {custom_id}")
                    continue
                
                content = choice['message']['content']
                enhanced_data = json.loads(content)
                
                # Store result with custom_id as key (we'll need to map this back to document path)
                parsed_results[custom_id] = {
                    'enhanced_classification': enhanced_data,
                    'tokens_used': body.get('usage', {}).get('total_tokens', 0),
                    'success': True
                }
                
            except Exception as e:
                self.logger.error(f"❌ Error parsing result for {custom_id}: {e}")
                parsed_results[custom_id] = {
                    'enhanced_classification': None,
                    'tokens_used': 0,
                    'success': False,
                    'error': str(e)
                }
        
        return parsed_results 