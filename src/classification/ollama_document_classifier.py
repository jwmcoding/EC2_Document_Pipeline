"""
Ollama Document Classifier using Local LLM (Phi-4)

Classifies business documents using local Ollama inference with Phi-4 model,
providing the same interface as GPT-4.1-mini but running entirely offline.
"""

import json
import logging
import requests
from typing import Dict, List, Optional, Tuple, Any
import time
from tenacity import retry, stop_after_attempt, wait_exponential

from .base_llm_classifier import (
    BaseLLMClassifier, DocumentType, LLMClassificationResult, 
    EnhancedLLMClassificationResult
)

class OllamaDocumentClassifier(BaseLLMClassifier):
    """Local document classification using Ollama with Phi-4"""
    
    def __init__(self, model_name: str = "phi4", ollama_base_url: str = "http://localhost:11434"):
        """
        Initialize Ollama classifier
        
        Args:
            model_name: Ollama model name (e.g., "phi4", "phi3", "llama3.2")
            ollama_base_url: Base URL for Ollama API
        """
        super().__init__(model_name)
        self.ollama_base_url = ollama_base_url.rstrip('/')
        self.api_url = f"{self.ollama_base_url}/api/generate"
        self.logger = logging.getLogger(__name__)
        
        # Verify Ollama is running and model is available
        self._verify_ollama_connection()
        
        self.logger.info(f"Initialized Ollama classifier with model: {model_name}")
        
        # Optimized classification prompt for Phi-4
        self.classification_prompt = """
You are an expert business document analyst. Classify this document into EXACTLY ONE of these 5 types:

DOCUMENT TYPES:
1. Email: Email messages (.msg files), correspondence, communication threads, email forwards
2. FMV Report: Fair Market Value reports, pricing analysis, market valuations, cost assessments
3. Quote/Proposal: Sales quotes, business proposals, RFP responses, bid documents, pricing quotes
4. Contract/SoW/MSA: Contracts, statements of work, master service agreements, legal agreements
5. Product Lit: Product literature, marketing materials, datasheets, brochures, technical specs

CLASSIFICATION RULES:
- If file type is .msg OR content shows email headers → classify as "Email"
- If not an email, classify based on document's business purpose and content
- Choose the SINGLE most appropriate type
- Provide confidence between 0.4-1.0

DOCUMENT CONTEXT:
Filename: {filename}
File Type: {file_type}
Vendor: {vendor}
Client: {client}
Deal Number: {deal_number}

CONTENT PREVIEW:
{content_preview}

ANALYSIS STEPS:
1. Check if this is an email format first
2. Analyze filename patterns and keywords
3. Review content structure and purpose
4. Choose the most appropriate single type
5. Assess confidence level

Respond ONLY with valid JSON in this exact format:
{{
    "document_type": "exact_type_name_from_list",
    "confidence": 0.85,
    "reasoning": "Brief explanation in 1-2 sentences",
    "alternatives": [
        {{"type": "alternative_type", "confidence": 0.65}}
    ]
}}
"""

        # Enhanced classification prompt for comprehensive metadata
        self.enhanced_classification_prompt = """
You are an expert business document analyst. Analyze this document and extract comprehensive metadata.

DOCUMENT TYPES (choose exactly one):
1. Email: Email messages, correspondence
2. FMV Report: Fair Market Value reports, pricing analysis, market valuations
3. Quote/Proposal: Sales quotes, business proposals, RFP responses
4. Contract/SoW/MSA: Contracts, statements of work, agreements
5. Product Lit: Product literature, marketing materials, datasheets

DEPTH RATINGS:
Pricing Depth (low/medium/high):
- LOW: Basic mentions ("competitive pricing", "cost-effective")
- MEDIUM: Some specific prices ("$1,200/month", "20% discount")
- HIGH: Detailed pricing tables, breakdowns, multiple price points

Terms Depth (low/medium/high):
- LOW: Basic mentions ("standard terms", "negotiable")
- MEDIUM: Some specific terms ("30-day payment", "annual contract")
- HIGH: Detailed conditions, SLAs, payment schedules, deliverables

DOCUMENT CONTEXT:
Filename: {filename}
File Type: {file_type}
Vendor: {vendor}
Client: {client}
Deal Number: {deal_number}
Page Count: {page_count}
Word Count: {word_count}

CONTENT PREVIEW:
{content_preview}

ANALYSIS TASKS:
1. Classify document type based on content and context
2. Write exactly 2 sentences summarizing the document
3. Rate pricing depth: low/medium/high
4. Rate terms depth: low/medium/high
5. Extract term dates in YYYY-MM-DD format (or null)
6. Identify 3-5 key topics
7. List vendor products/models mentioned
8. Extract specific pricing indicators

Respond with valid JSON only:
{{
    "document_type": "exact_type_from_list",
    "confidence": 0.85,
    "content_summary": "Two sentence summary of document content.",
    "product_pricing_depth": "medium",
    "commercial_terms_depth": "low",
    "proposed_term_start": "2024-01-01",
    "proposed_term_end": "2024-12-31",
    "reasoning": "Brief classification explanation",
    "key_topics": ["Topic 1", "Topic 2", "Topic 3"],
    "vendor_products_mentioned": ["Product A", "Product B"],
    "pricing_indicators": ["$1,200", "monthly fee"]
}}
"""
    
    def _verify_ollama_connection(self):
        """Verify Ollama is running and model is available"""
        try:
            # Check if Ollama is running
            response = requests.get(f"{self.ollama_base_url}/api/tags", timeout=5)
            if response.status_code != 200:
                raise ConnectionError(f"Ollama not accessible at {self.ollama_base_url}")
            
            # Check if model is available
            models = response.json().get('models', [])
            model_names = [model.get('name', '').split(':')[0] for model in models]
            
            if self.model_name not in model_names:
                self.logger.warning(f"⚠️ Model {self.model_name} not found in Ollama. Available: {model_names}")
                self.logger.warning(f"   Run: ollama pull {self.model_name}")
            else:
                self.logger.info(f"✅ Ollama model {self.model_name} available")
                
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Cannot connect to Ollama at {self.ollama_base_url}: {e}")
    
    def _call_ollama(self, prompt: str, max_tokens: int = 500) -> str:
        """Make API call to Ollama"""
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,  # Low temperature for consistent results
                "num_predict": max_tokens,
                "stop": ["Human:", "Assistant:"]
            }
        }
        
        response = requests.post(
            self.api_url,
            json=payload,
            timeout=60,  # Longer timeout for local inference
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code != 200:
            raise Exception(f"Ollama API error: {response.status_code} - {response.text}")
        
        result = response.json()
        if 'response' not in result:
            raise Exception(f"Invalid Ollama response: {result}")
        
        return result['response']
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
    def classify_document(
        self,
        filename: str,
        content_preview: str = "",
        file_type: str = "",
        vendor: str = "",
        client: str = "",
        deal_number: str = ""
    ) -> LLMClassificationResult:
        """
        Classify document using Ollama/Phi-4 with business context
        """
        try:
            # Prepare prompt with all available context
            prompt = self.classification_prompt.format(
                filename=filename,
                file_type=file_type or "unknown",
                vendor=vendor or "unknown",
                client=client or "unknown", 
                deal_number=deal_number or "unknown",
                content_preview=content_preview[:1500] if content_preview else "No content available"
            )
            
            self.logger.debug(f"Classifying document with Ollama: {filename}")
            start_time = time.time()
            
            # Call Ollama API
            response_text = self._call_ollama(prompt, max_tokens=300)
            
            # Track processing time (approximate token usage)
            processing_time = time.time() - start_time
            estimated_tokens = len(prompt.split()) + len(response_text.split())
            self.total_tokens_used += estimated_tokens
            self.classification_count += 1
            
            # Parse response - extract JSON from response
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start == -1 or json_end == 0:
                raise ValueError("No JSON found in Ollama response")
            
            json_text = response_text[json_start:json_end]
            result_json = json.loads(json_text)
            
            # Map string to DocumentType enum
            doc_type_str = result_json["document_type"]
            doc_type = self._map_string_to_enum(doc_type_str)
            
            confidence = float(result_json["confidence"])
            reasoning = result_json["reasoning"]
            
            # Parse alternative types
            alternatives = []
            for alt in result_json.get("alternatives", []):
                try:
                    alt_type = self._map_string_to_enum(alt["type"])
                    alt_confidence = float(alt["confidence"])
                    alternatives.append((alt_type, alt_confidence))
                except (ValueError, KeyError) as e:
                    self.logger.warning(f"Skipping invalid alternative: {alt} - {e}")
                    continue
            
            self.logger.info(f"✅ Ollama classified '{filename}' as {doc_type.value} (confidence: {confidence:.2f}, time: {processing_time:.1f}s)")
            
            return LLMClassificationResult(
                document_type=doc_type,
                confidence=confidence,
                reasoning=reasoning,
                alternative_types=alternatives,
                classification_method="ollama",
                tokens_used=estimated_tokens
            )
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON response from Ollama for {filename}: {e}")
            return self._fallback_classification(filename, "json_parse_error")
            
        except Exception as e:
            self.logger.error(f"❌ Ollama classification failed for {filename}: {e}")
            return self._fallback_classification(filename, "api_error")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
    def classify_document_enhanced(
        self,
        filename: str,
        content_preview: str = "",
        file_type: str = "",
        vendor: str = "",
        client: str = "",
        deal_number: str = "",
        page_count: Optional[int] = None,
        word_count: Optional[int] = None
    ) -> EnhancedLLMClassificationResult:
        """
        Enhanced document classification using Ollama/Phi-4
        """
        try:
            # Prepare enhanced prompt
            prompt = self.enhanced_classification_prompt.format(
                filename=filename,
                file_type=file_type or "unknown",
                vendor=vendor or "unknown",
                client=client or "unknown", 
                deal_number=deal_number or "unknown",
                page_count=page_count or "unknown",
                word_count=word_count or "unknown",
                content_preview=content_preview[:1500] if content_preview else "No content available"
            )
            
            self.logger.debug(f"Enhanced classification with Ollama: {filename}")
            start_time = time.time()
            
            # Call Ollama with larger response limit
            response_text = self._call_ollama(prompt, max_tokens=800)
            
            # Track usage
            processing_time = time.time() - start_time
            estimated_tokens = len(prompt.split()) + len(response_text.split())
            self.total_tokens_used += estimated_tokens
            self.classification_count += 1
            
            # Parse JSON response
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start == -1 or json_end == 0:
                raise ValueError("No JSON found in Ollama response")
            
            json_text = response_text[json_start:json_end]
            result_json = json.loads(json_text)
            
            # Map document type string to enum
            doc_type = self._map_string_to_enum(result_json["document_type"])
            
            # Create enhanced result object
            enhanced_result = EnhancedLLMClassificationResult(
                document_type=doc_type,
                confidence=float(result_json["confidence"]),
                content_summary=result_json["content_summary"],
                product_pricing_depth=result_json["product_pricing_depth"],
                commercial_terms_depth=result_json["commercial_terms_depth"],
                proposed_term_start=result_json.get("proposed_term_start"),
                proposed_term_end=result_json.get("proposed_term_end"),
                reasoning=result_json["reasoning"],
                key_topics=result_json.get("key_topics", []),
                vendor_products_mentioned=result_json.get("vendor_products_mentioned", []),
                pricing_indicators=result_json.get("pricing_indicators", []),
                alternative_types=[],  # Could enhance this if needed
                tokens_used=estimated_tokens,
                classification_method="ollama_enhanced"
            )
            
            self.logger.info(f"📊 Ollama enhanced classification complete: {filename}")
            self.logger.debug(f"   Type: {doc_type.value} ({enhanced_result.confidence:.2f})")
            self.logger.debug(f"   Time: {processing_time:.1f}s")
            
            return enhanced_result
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Enhanced classification JSON error for {filename}: {e}")
            return self._enhanced_fallback_classification(filename, "json_parse_error")
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced classification failed for {filename}: {e}")
            return self._enhanced_fallback_classification(filename, "api_error")
    
    def batch_classify_documents(
        self,
        documents: List[Dict],
        batch_size: int = 3,  # Smaller batches for local processing
        delay_between_batches: float = 2.0  # Longer delay for local model
    ) -> List[LLMClassificationResult]:
        """
        Classify multiple documents with rate limiting optimized for local inference
        """
        results = []
        total_docs = len(documents)
        
        self.logger.info(f"🚀 Starting Ollama batch classification of {total_docs} documents")
        
        for i in range(0, total_docs, batch_size):
            batch = documents[i:i + batch_size]
            batch_start = i + 1
            batch_end = min(i + batch_size, total_docs)
            
            self.logger.info(f"📄 Processing Ollama batch {batch_start}-{batch_end}/{total_docs}")
            
            for doc in batch:
                result = self.classify_document(
                    filename=doc.get('filename', ''),
                    content_preview=doc.get('content_preview', ''),
                    file_type=doc.get('file_type', ''),
                    vendor=doc.get('vendor', ''),
                    client=doc.get('client', ''),
                    deal_number=doc.get('deal_number', '')
                )
                results.append(result)
            
            # Rate limiting delay between batches
            if batch_end < total_docs:
                time.sleep(delay_between_batches)
        
        # Log final statistics
        successful_classifications = len([r for r in results if r.classification_method == "ollama"])
        fallback_classifications = len([r for r in results if r.classification_method != "ollama"])
        avg_confidence = sum(r.confidence for r in results) / len(results) if results else 0
        
        self.logger.info(f"✅ Ollama batch classification complete:")
        self.logger.info(f"   📊 Total: {total_docs} documents")
        self.logger.info(f"   🎯 Ollama Success: {successful_classifications} ({successful_classifications/total_docs*100:.1f}%)")
        self.logger.info(f"   🔄 Fallbacks: {fallback_classifications} ({fallback_classifications/total_docs*100:.1f}%)")
        self.logger.info(f"   📈 Avg Confidence: {avg_confidence:.2f}")
        self.logger.info(f"   ⚡ Estimated Tokens: {self.total_tokens_used}")
        
        return results
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics for monitoring (no costs for local models)"""
        return {
            "total_classifications": self.classification_count,
            "estimated_tokens_processed": self.total_tokens_used,
            "estimated_cost_usd": 0.0,  # Local inference is free
            "avg_tokens_per_classification": round(self.total_tokens_used / max(1, self.classification_count), 1),
            "model": self.model_name,
            "provider": "ollama",
            "ollama_base_url": self.ollama_base_url
        }
    
    def _fallback_classification(self, filename: str, error_type: str) -> LLMClassificationResult:
        """Fallback classification using simple filename patterns when Ollama fails"""
        
        clean_name = filename.lower()
        confidence = 0.6  # Medium confidence for pattern matching
        
        # Simple regex patterns as fallback (same logic as OpenAI version)
        if any(pattern in clean_name for pattern in ['fmv-report', 'fmv_report', 'fair market value report', 'valuation report']):
            return LLMClassificationResult(
                document_type=DocumentType.FMV_REPORT,
                confidence=confidence,
                reasoning=f"Fallback pattern matching (FMV Report keywords) - {error_type}",
                alternative_types=[],
                classification_method="regex_fallback"
            )
        elif any(pattern in clean_name for pattern in ['quote', 'proposal', 'rfp', 'bid', 'pricing']):
            return LLMClassificationResult(
                document_type=DocumentType.QUOTE_PROPOSAL,
                confidence=confidence,
                reasoning=f"Fallback pattern matching (Quote/Proposal keywords) - {error_type}",
                alternative_types=[],
                classification_method="regex_fallback"
            )
        elif any(pattern in clean_name for pattern in ['contract', 'sow', 'msa', 'agreement', 'statement of work', 'master service']):
            return LLMClassificationResult(
                document_type=DocumentType.CONTRACT_SOW_MSA,
                confidence=confidence,
                reasoning=f"Fallback pattern matching (Contract/SoW/MSA keywords) - {error_type}",
                alternative_types=[],
                classification_method="regex_fallback"
            )
        elif any(pattern in clean_name for pattern in ['fmv', 'fair-market', 'fair_market', 'market-value', 'market_value', 'pricing analysis']):
            return LLMClassificationResult(
                document_type=DocumentType.FMV_REPORT,
                confidence=confidence,
                reasoning=f"Fallback pattern matching (FMV keywords) - {error_type}",
                alternative_types=[],
                classification_method="regex_fallback"
            )
        elif any(pattern in clean_name for pattern in ['product', 'datasheet', 'brochure', 'spec', 'technical', 'literature', 'marketing']):
            return LLMClassificationResult(
                document_type=DocumentType.PRODUCT_LIT,
                confidence=confidence,
                reasoning=f"Fallback pattern matching (Product Literature keywords) - {error_type}",
                alternative_types=[],
                classification_method="regex_fallback"
            )
        elif any(pattern in clean_name for pattern in ['email', 'msg', 'correspondence', 'communication']):
            return LLMClassificationResult(
                document_type=DocumentType.EMAIL,
                confidence=confidence,
                reasoning=f"Fallback pattern matching (Email keywords) - {error_type}",
                alternative_types=[],
                classification_method="regex_fallback"
            )
        else:
            # Default to most common type with low confidence
            return LLMClassificationResult(
                document_type=DocumentType.QUOTE_PROPOSAL,
                confidence=0.2,
                reasoning=f"Fallback default classification - no clear patterns found - {error_type}",
                alternative_types=[],
                classification_method="default_fallback"
            )

    def _enhanced_fallback_classification(self, filename: str, error_type: str) -> EnhancedLLMClassificationResult:
        """Enhanced fallback classification using filename patterns when Ollama fails"""
        
        clean_name = filename.lower()
        confidence = 0.5  # Lower confidence for fallback
        
        # Determine document type using existing logic (same as OpenAI version)
        if any(pattern in clean_name for pattern in ['fmv-report', 'fmv_report', 'fair market value report', 'valuation report', 'fmv', 'fair-market']):
            doc_type = DocumentType.FMV_REPORT
            summary = f"Fair market value or pricing analysis document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "medium"
            terms_depth = "low"
        elif any(pattern in clean_name for pattern in ['quote', 'proposal', 'rfp', 'bid', 'pricing']):
            doc_type = DocumentType.QUOTE_PROPOSAL
            summary = f"Sales quote or business proposal document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "medium"
            terms_depth = "medium"
        elif any(pattern in clean_name for pattern in ['contract', 'sow', 'msa', 'agreement', 'statement of work', 'master service']):
            doc_type = DocumentType.CONTRACT_SOW_MSA
            summary = f"Contract or statement of work document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"
            terms_depth = "high"
        elif any(pattern in clean_name for pattern in ['product', 'datasheet', 'brochure', 'spec', 'technical', 'literature', 'marketing']):
            doc_type = DocumentType.PRODUCT_LIT
            summary = f"Product literature or marketing material based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"
            terms_depth = "low"
        elif any(pattern in clean_name for pattern in ['email', 'msg', 'correspondence', 'communication']):
            doc_type = DocumentType.EMAIL
            summary = f"Email or communication document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"
            terms_depth = "low"
        else:
            # Default to Quote/Proposal
            doc_type = DocumentType.QUOTE_PROPOSAL
            summary = f"Business document with unclear type based on filename. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"
            terms_depth = "low"
        
        return EnhancedLLMClassificationResult(
            document_type=doc_type,
            confidence=confidence,
            content_summary=summary,
            product_pricing_depth=pricing_depth,
            commercial_terms_depth=terms_depth,
            proposed_term_start=None,
            proposed_term_end=None,
            reasoning=f"Ollama fallback pattern matching - {error_type}",
            key_topics=[],
            vendor_products_mentioned=[],
            pricing_indicators=[],
            alternative_types=[],
            classification_method="ollama_enhanced_regex_fallback"
        ) 