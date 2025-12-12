"""
LLM Document Classifier using GPT-4.1-mini

Classifies business documents into 5 specific types:
- Email
- FMV Report
- Quote/Proposal  
- Contract/SoW/MSA
- Product Lit
"""

import json
import logging
from typing import Dict, List, Optional, Tuple
import openai
from tenacity import retry, stop_after_attempt, wait_exponential

from .base_llm_classifier import (
    BaseLLMClassifier, DocumentType, LLMClassificationResult, 
    EnhancedLLMClassificationResult
)

# JSON Schema for GPT-4.1-mini Structured Outputs
ENHANCED_CLASSIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "document_type": {
            "type": "string",
            "enum": ["Email", "FMV Report", "Quote/Proposal", "Contract/SoW/MSA", "Product Lit"],
            "description": "Document type: Quote/Proposal includes order forms; Contract/SoW/MSA for binding legal agreements only"
        },
        "confidence": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0
        },
        "product_pricing_depth": {
            "type": "string",
            "enum": ["low", "medium", "high"],
            "description": "Depth of product pricing information"
        },
        "commercial_terms_depth": {
            "type": "string", 
            "enum": ["low", "medium", "high"],
            "description": "Depth of commercial terms and conditions"
        },
        "proposed_term_start": {
            "type": ["string", "null"],
            "description": "Contract/quote start date in YYYY-MM-DD format, null if not found"
        },
        "proposed_term_end": {
            "type": ["string", "null"], 
            "description": "Contract/quote end date in YYYY-MM-DD format, null if not found"
        },
        
    },
    "required": [
        "document_type", "confidence", 
        "product_pricing_depth", "commercial_terms_depth"
    ],
    "additionalProperties": False
}

class LLMDocumentClassifier(BaseLLMClassifier):
    """Intelligent document classification using GPT-4.1-mini"""
    
    def __init__(self, api_key: str, model: str = "gpt-4.1-mini"):
        super().__init__(model)
        self.model = model  # Fix for missing model attribute
        self.client = openai.OpenAI(api_key=api_key)
        self.logger = logging.getLogger(__name__)
        
        self.logger.info(f"Initialized LLM classifier with model: {model}")
        
        # Optimized classification prompt for business documents
        self.classification_prompt = """
You are an expert business document analyst. Classify this document into EXACTLY ONE of these 5 types:

DOCUMENT TYPES:
1. Email: Email messages (.msg files), correspondence, communication threads, email forwards. Focus on the EMAIL FORMAT rather than content type.
2. FMV Report: Fair Market Value reports, pricing analysis, market valuations, cost assessments, pricing benchmarks, valuation documents
3. Quote/Proposal: Sales quotes, business proposals, RFP responses, bid documents, project proposals, solution proposals, pricing quotes, ORDER FORMS, purchase orders, sales orders
4. Contract/SoW/MSA: LEGAL CONTRACTS ONLY - Master Service Agreements (MSAs), Statements of Work (SoWs), Enterprise License Agreements (ELAs), service contracts, licensing agreements, legal terms & conditions, binding agreements with legal obligations
5. Product Lit: Product literature, marketing materials, datasheets, brochures, technical specifications, product documentation, feature guides

CRITICAL DISTINCTION - Quote/Proposal vs Contract/SoW/MSA:
• Quote/Proposal: ORDER FORMS, pricing documents, proposals seeking approval, bids, quotes (pre-contract sales documents)
• Contract/SoW/MSA: BINDING LEGAL AGREEMENTS with terms, conditions, obligations, deliverables, legal language, signatures required

CLASSIFICATION PRIORITY:
- If file type is .msg OR content shows email headers/structure → classify as "Email"
- If not an email format, then classify based on the document's business purpose and content

DOCUMENT CONTEXT:
Filename: {filename}
File Type: {file_type}
Vendor: {vendor}
Client: {client}  
Deal Number: {deal_number}

CONTENT PREVIEW:
{content_preview}

ANALYSIS INSTRUCTIONS:
1. FIRST: Check if this is an email (.msg file or email-like content with headers)
2. If it's an email, classify as "Email" regardless of email content
3. If not an email, analyze filename patterns:
   - FMV → FMV Report
   - MSA, SOW, Contract, ELA, Agreement → Contract/SoW/MSA (ONLY if binding legal contract)
   - Order, Quote, Proposal, Bid → Quote/Proposal (including order forms)
   - Datasheet, Spec, Brochure → Product Lit
4. Consider file type and business context
5. Review content keywords and structure:
   - Legal language, terms & conditions, binding obligations → Contract/SoW/MSA
   - Pricing, ordering, proposals seeking approval → Quote/Proposal
6. Choose the SINGLE most appropriate type
7. Provide confidence (0.7-1.0 for clear matches, 0.4-0.7 for uncertain)
8. Give concise reasoning (1-2 sentences max)
9. List up to 2 alternatives if classification is uncertain

Return ONLY this JSON format:
{{
    "document_type": "exact_enum_name_from_list",
    "confidence": 0.85,
    "reasoning": "Brief explanation based on filename/content analysis",
    "alternatives": [
        {{"type": "alternative_type", "confidence": 0.65}},
        {{"type": "another_alternative", "confidence": 0.45}}
    ]
}}
"""

        # Enhanced classification prompt for comprehensive metadata extraction (v3)
        self.enhanced_classification_prompt = """
You are an expert business document analyst. Analyze this document and extract comprehensive metadata.

DOCUMENT TYPES (choose exactly one):
1. Email: Email messages, correspondence, communication
2. FMV Report: Fair Market Value reports, pricing analysis, market valuations
3. Quote/Proposal: Sales quotes, business proposals, RFP responses, pricing quotes, ORDER FORMS, purchase orders, sales orders
4. Contract/SoW/MSA: LEGAL CONTRACTS ONLY - MSAs, SoWs, ELAs, service contracts, licensing agreements, binding legal agreements
5. Product Lit: Product literature, marketing materials, datasheets, brochures

KEY DISTINCTION - Quote/Proposal vs Contract/SoW/MSA:
• Quote/Proposal: ORDER FORMS, pricing documents, proposals seeking approval, bids (pre-contract sales documents)
• Contract/SoW/MSA: BINDING LEGAL AGREEMENTS with legal terms, conditions, obligations, deliverables

PRICING DEPTH EXAMPLES:
• LOW: Basic mentions ("competitive pricing", "cost-effective solution")
• MEDIUM: Some specific prices ("$1,200/month", "20% discount") 
• HIGH: Detailed pricing tables, breakdowns, multiple price points, cost analysis

COMMERCIAL TERMS DEPTH EXAMPLES:
• LOW: Basic mentions ("standard terms", "negotiable")
• MEDIUM: Some specific terms ("30-day payment", "annual contract")
• HIGH: Detailed terms, conditions, SLAs, payment schedules, deliverables, penalties

DOCUMENT CONTEXT:
Filename: {filename}
File Type: {file_type}
Vendor: {vendor}
Client: {client}
Deal Number: {deal_number}
Page Count: {page_count}
Word Count: {word_count}

CONTENT PREVIEW (First 1500 characters):
{content_preview}

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

Return structured JSON matching the required schema.
"""

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
        Classify document using GPT-4.1-mini with business context
        
        Args:
            filename: Document filename
            content_preview: First ~1000 characters of document content
            file_type: File extension (.pdf, .docx, etc.)
            vendor: Vendor company name
            client: Client company name  
            deal_number: Deal identifier
            
        Returns:
            LLMClassificationResult with type, confidence, and reasoning
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
            
            self.logger.debug(f"Classifying document: {filename}")
            
            # Call GPT-4.1-mini API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=250,
                temperature=0.1,  # Low temperature for consistent results
                response_format={"type": "json_object"}  # Ensure JSON response
            )
            
            # Track token usage
            tokens_used = response.usage.total_tokens
            self.total_tokens_used += tokens_used
            self.classification_count += 1
            
            # Parse LLM response
            response_text = response.choices[0].message.content
            result_json = json.loads(response_text)
            
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
            
            self.logger.info(f"✅ Classified '{filename}' as {doc_type.value} (confidence: {confidence:.2f}, tokens: {tokens_used})")
            
            return LLMClassificationResult(
                document_type=doc_type,
                confidence=confidence,
                reasoning=reasoning,
                alternative_types=alternatives,
                classification_method="llm",
                tokens_used=tokens_used
            )
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON response for {filename}: {e}")
            return self._fallback_classification(filename, "json_parse_error")
            
        except Exception as e:
            self.logger.error(f"❌ LLM classification failed for {filename}: {e}")
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
        Enhanced document classification with comprehensive metadata extraction (v3)
        
        Args:
            filename: Document filename
            content_preview: First ~1500 characters of document content
            file_type: File extension (.pdf, .docx, etc.)
            vendor: Vendor company name
            client: Client company name  
            deal_number: Deal identifier
            page_count: Number of pages in document
            word_count: Word count from parsed content
            
        Returns:
            EnhancedLLMClassificationResult with comprehensive metadata
        """
        try:
            # Prepare enhanced prompt with all context
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
            
            self.logger.debug(f"Enhanced classification: {filename}")
            
            # Call GPT-4.1-mini with structured output
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,  # Increased for enhanced metadata
                temperature=0.1,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "document_classification",
                        "schema": ENHANCED_CLASSIFICATION_SCHEMA
                    }
                }
            )
            
            # Track token usage
            tokens_used = response.usage.total_tokens
            self.total_tokens_used += tokens_used
            self.classification_count += 1
            
            # Parse structured response
            response_text = response.choices[0].message.content
            result_json = json.loads(response_text)
            
            # Map document type string to enum
            doc_type = self._map_string_to_enum(result_json["document_type"])
            
            # Create enhanced result object
            enhanced_result = EnhancedLLMClassificationResult(
                document_type=doc_type,
                confidence=float(result_json["confidence"]),
                product_pricing_depth=result_json["product_pricing_depth"],
                commercial_terms_depth=result_json["commercial_terms_depth"],
                proposed_term_start=result_json.get("proposed_term_start"),
                proposed_term_end=result_json.get("proposed_term_end"),
                alternative_types=[],  # Could enhance this too if needed
                tokens_used=tokens_used
            )
            
            self.logger.info(f"📊 Enhanced classification complete: {filename}")
            self.logger.debug(f"   Type: {doc_type.value} ({enhanced_result.confidence:.2f})")
            self.logger.debug(f"   Pricing depth: {enhanced_result.product_pricing_depth}")
            self.logger.debug(f"   Terms depth: {enhanced_result.commercial_terms_depth}")
            if enhanced_result.proposed_term_start:
                self.logger.debug(f"   Term: {enhanced_result.proposed_term_start} to {enhanced_result.proposed_term_end}")
            
            return enhanced_result
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Enhanced classification JSON error for {filename}: {e}")
            return self._enhanced_fallback_classification(filename, "json_parse_error")
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced classification failed for {filename}: {e}")
            return self._enhanced_fallback_classification(filename, "api_error")
    
    def _map_string_to_enum(self, type_string: str) -> DocumentType:
        """Map LLM response string to DocumentType enum"""
        
        # Direct mapping attempts
        type_mappings = {
            # Expected full format responses
            "FMV Report": DocumentType.FMV_REPORT,
            "Quote/Proposal": DocumentType.QUOTE_PROPOSAL,
            "Contract/SoW/MSA": DocumentType.CONTRACT_SOW_MSA,
            "Product Lit": DocumentType.PRODUCT_LIT,
            "Email": DocumentType.EMAIL,
            
            # Alternative full formats
            "FMV_REPORT": DocumentType.FMV_REPORT,
            "QUOTE_PROPOSAL": DocumentType.QUOTE_PROPOSAL,
            "CONTRACT_SOW_MSA": DocumentType.CONTRACT_SOW_MSA,
            "PRODUCT_LIT": DocumentType.PRODUCT_LIT,
            "EMAIL": DocumentType.EMAIL,
            
            # Abbreviated forms that LLM actually returns
            "IDD": DocumentType.CONTRACT_SOW_MSA,  # Implementation & Design Document -> Contract/SoW/MSA
            "FMV": DocumentType.FMV_REPORT,  # Fair Market Value
            "Email/Communication": DocumentType.EMAIL,
            "Other": DocumentType.QUOTE_PROPOSAL,  # Miscellaneous business documents -> Quote/Proposal
            "Implementation and Design Document": DocumentType.CONTRACT_SOW_MSA,
            "IDD (Implementation and Design Document)": DocumentType.CONTRACT_SOW_MSA,
            
            # Common variations
            "Fair Market Value": DocumentType.FMV_REPORT,
            "Implementation Document": DocumentType.CONTRACT_SOW_MSA,
            "Communication": DocumentType.EMAIL,
            "Correspondence": DocumentType.EMAIL,
        }
        
        # Try exact match first
        if type_string in type_mappings:
            return type_mappings[type_string]
        
        # Try case-insensitive search
        type_lower = type_string.lower()
        for key, value in type_mappings.items():
            if key.lower() == type_lower:
                return value
        
        # Try partial matching for common patterns
        if any(pattern in type_lower for pattern in ["fmv report", "valuation report", "fmv", "fair market", "market value"]):
            return DocumentType.FMV_REPORT
        elif any(pattern in type_lower for pattern in ["quote", "proposal", "rfp", "bid"]):
            return DocumentType.QUOTE_PROPOSAL
        elif any(pattern in type_lower for pattern in ["contract", "sow", "msa", "agreement", "statement of work"]):
            return DocumentType.CONTRACT_SOW_MSA
        elif any(pattern in type_lower for pattern in ["product", "literature", "datasheet", "brochure", "marketing"]):
            return DocumentType.PRODUCT_LIT
        elif any(pattern in type_lower for pattern in ["email", "msg", "correspondence", "communication"]):
            return DocumentType.EMAIL
        
        # If no match found, raise error to trigger fallback
        raise ValueError(f"Could not map '{type_string}' to any DocumentType")
    
    def _fallback_classification(self, filename: str, error_type: str) -> LLMClassificationResult:
        """Fallback classification using simple filename patterns when LLM fails"""
        
        clean_name = filename.lower()
        confidence = 0.6  # Medium confidence for pattern matching
        
        # Simple regex patterns as fallback
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
                document_type=DocumentType.FMV_REPORT, # Changed from DocumentType.FMV to DocumentType.FMV_REPORT
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
        """Enhanced fallback classification using filename patterns when LLM fails"""
        
        clean_name = filename.lower()
        confidence = 0.5  # Lower confidence for fallback
        
        # Determine document type using existing logic
        if any(pattern in clean_name for pattern in ['fmv-report', 'fmv_report', 'fair market value report', 'valuation report', 'fmv', 'fair-market']):
            doc_type = DocumentType.FMV_REPORT
            summary = f"Fair market value or pricing analysis document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "medium"  # Assume medium for FMV reports
            terms_depth = "low"
        elif any(pattern in clean_name for pattern in ['quote', 'proposal', 'rfp', 'bid', 'pricing']):
            doc_type = DocumentType.QUOTE_PROPOSAL
            summary = f"Sales quote or business proposal document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "medium"  # Likely has pricing
            terms_depth = "medium"  # Likely has some terms
        elif any(pattern in clean_name for pattern in ['contract', 'sow', 'msa', 'agreement', 'statement of work', 'master service']):
            doc_type = DocumentType.CONTRACT_SOW_MSA
            summary = f"Contract or statement of work document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"  # Contracts often reference pricing
            terms_depth = "high"  # Contracts have detailed terms
        elif any(pattern in clean_name for pattern in ['product', 'datasheet', 'brochure', 'spec', 'technical', 'literature', 'marketing']):
            doc_type = DocumentType.PRODUCT_LIT
            summary = f"Product literature or marketing material based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"  # Usually minimal pricing
            terms_depth = "low"
        elif any(pattern in clean_name for pattern in ['email', 'msg', 'correspondence', 'communication']):
            doc_type = DocumentType.EMAIL
            summary = f"Email or communication document based on filename pattern. Unable to analyze full content due to {error_type}."
            pricing_depth = "low"  # Emails rarely have detailed pricing
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
            product_pricing_depth=pricing_depth,
            commercial_terms_depth=terms_depth,
            proposed_term_start=None,
            proposed_term_end=None,
            reasoning=f"Fallback pattern matching - {error_type}",
            alternative_types=[],
            classification_method="enhanced_regex_fallback"
        )
    
    def batch_classify_documents(
        self,
        documents: List[Dict],
        batch_size: int = 5,
        delay_between_batches: float = 1.0
    ) -> List[LLMClassificationResult]:
        """
        Classify multiple documents with rate limiting
        
        Args:
            documents: List of document dicts with keys: filename, content_preview, etc.
            batch_size: Number of documents to process before delay
            delay_between_batches: Seconds to wait between batches
            
        Returns:
            List of LLMClassificationResult objects
        """
        results = []
        total_docs = len(documents)
        
        self.logger.info(f"🚀 Starting batch classification of {total_docs} documents")
        
        for i in range(0, total_docs, batch_size):
            batch = documents[i:i + batch_size]
            batch_start = i + 1
            batch_end = min(i + batch_size, total_docs)
            
            self.logger.info(f"📄 Processing batch {batch_start}-{batch_end}/{total_docs}")
            
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
                import time
                time.sleep(delay_between_batches)
        
        # Log final statistics
        successful_classifications = len([r for r in results if r.classification_method == "llm"])
        fallback_classifications = len([r for r in results if r.classification_method != "llm"])
        avg_confidence = sum(r.confidence for r in results) / len(results) if results else 0
        
        self.logger.info(f"✅ Batch classification complete:")
        self.logger.info(f"   📊 Total: {total_docs} documents")
        self.logger.info(f"   🎯 LLM Success: {successful_classifications} ({successful_classifications/total_docs*100:.1f}%)")
        self.logger.info(f"   🔄 Fallbacks: {fallback_classifications} ({fallback_classifications/total_docs*100:.1f}%)")
        self.logger.info(f"   📈 Avg Confidence: {avg_confidence:.2f}")
        self.logger.info(f"   💰 Total Tokens: {self.total_tokens_used}")
        
        return results
    
    def get_usage_stats(self) -> Dict[str, any]:
        """Get usage statistics for cost monitoring"""
        
        # GPT-4.1-mini pricing (estimated)
        cost_per_1k_input = 0.00015  # $0.15 per 1M tokens  
        cost_per_1k_output = 0.0006  # $0.60 per 1M tokens
        
        # Estimate token distribution (rough approximation)
        estimated_input_tokens = int(self.total_tokens_used * 0.8)  # ~80% input
        estimated_output_tokens = int(self.total_tokens_used * 0.2)  # ~20% output
        
        estimated_cost = (
            (estimated_input_tokens / 1000) * cost_per_1k_input +
            (estimated_output_tokens / 1000) * cost_per_1k_output
        )
        
        return {
            "total_classifications": self.classification_count,
            "total_tokens_used": self.total_tokens_used,
            "estimated_input_tokens": estimated_input_tokens,
            "estimated_output_tokens": estimated_output_tokens,
            "estimated_cost_usd": round(estimated_cost, 4),
            "avg_tokens_per_classification": round(self.total_tokens_used / max(1, self.classification_count), 1),
            "model": self.model
        } 