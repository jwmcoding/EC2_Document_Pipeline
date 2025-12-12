"""
HyDE (Hypothetical Document Embeddings) Query Transformer for Business Documents.

Generates hypothetical documents that match the style and content of IT vendor contracts,
quotes, sales literature, and business documents for improved retrieval performance.

Based on research showing HyDE can improve retrieval performance by 10-30% by generating
context-aware hypothetical documents that better match the target corpus.
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import openai
from openai import OpenAI
import time
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config.settings import Settings

logger = logging.getLogger(__name__)


class HyDEStrategy(Enum):
    """Different HyDE generation strategies for business documents"""
    GENERIC_BUSINESS = "generic_business"
    IT_VENDOR_CONTRACT = "it_vendor_contract"
    PRICING_PROPOSAL = "pricing_proposal"
    TECHNICAL_SPECIFICATION = "technical_specification"
    COMPLIANCE_SECURITY = "compliance_security"
    VENDOR_COMPARISON = "vendor_comparison"


@dataclass
class HyDEResult:
    """Result of HyDE document generation"""
    hypothetical_documents: List[str]
    strategy_used: HyDEStrategy
    generation_time: float
    token_usage: Dict[str, int]
    confidence_score: float
    business_context: Optional[Dict[str, Any]] = None


class BusinessDocumentHyDETransformer:
    """
    HyDE transformer specialized for business documents with domain-specific prompts.
    
    Generates hypothetical documents that match the style and content of:
    - IT vendor contracts and quotes
    - Sales literature and proposals
    - Technical specifications
    - Compliance and security documents
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model = getattr(settings, 'HYDE_MODEL', 'gpt-4.1-mini')
        self.temperature = getattr(settings, 'HYDE_TEMPERATURE', 0.3)
        self.max_tokens = getattr(settings, 'HYDE_MAX_TOKENS', 300)
        
        # Load domain-specific prompt templates
        self.prompt_templates = self._load_prompt_templates()
        
        logger.info(f"🔍 HyDE Transformer initialized with model: {self.model}")
    
    def _load_prompt_templates(self) -> Dict[HyDEStrategy, str]:
        """Load specialized prompt templates for different business document types"""
        return {
            HyDEStrategy.GENERIC_BUSINESS: """
Write a comprehensive business document that would contain the answer to this query: "{query}"

The document should be written in the style of typical business documentation with:
- Professional business language
- Specific vendor and client information
- Technical details and pricing information
- Contract terms and compliance requirements

Focus on creating content that would realistically appear in business documents, proposals, or contracts.

Document:""",
            
            HyDEStrategy.IT_VENDOR_CONTRACT: """
Write a detailed IT vendor contract or proposal section that addresses this query: "{query}"

The document should include:
- Vendor company information and offerings
- Specific product names, models, and technical specifications
- Pricing details including license costs, support fees, implementation costs
- Service level agreements and performance metrics
- Contract terms, renewal conditions, and compliance requirements
- Implementation timeline and deliverables

Write as if this is an actual vendor proposal or contract section from companies like Microsoft, Oracle, Cisco, IBM, etc.

Contract/Proposal Section:""",
            
            HyDEStrategy.PRICING_PROPOSAL: """
Create a detailed pricing proposal or quote that answers this query: "{query}"

Include:
- Itemized pricing breakdown with specific costs
- Product/service descriptions with technical details
- Volume discounts, payment terms, and renewal pricing
- Implementation and professional services costs
- Ongoing support and maintenance fees
- Total cost of ownership calculations

Write in the style of actual vendor quotes with specific dollar amounts and business terms.

Pricing Proposal:""",
            
            HyDEStrategy.TECHNICAL_SPECIFICATION: """
Write a technical specification document that addresses this query: "{query}"

Include:
- Detailed technical requirements and specifications
- System architecture and integration details
- Performance metrics and capacity planning
- Security requirements and compliance standards
- Implementation methodology and best practices
- Risk assessment and mitigation strategies

Write as a formal technical specification or implementation document.

Technical Specification:""",
            
            HyDEStrategy.COMPLIANCE_SECURITY: """
Create a compliance or security document that addresses this query: "{query}"

Include:
- Regulatory compliance requirements (SOX, GDPR, HIPAA, etc.)
- Security frameworks and standards (ISO 27001, NIST, etc.)
- Risk assessment and audit findings
- Control implementations and remediation plans
- Policy statements and procedural requirements
- Certification and attestation details

Write as a formal compliance or security assessment document.

Compliance/Security Document:""",
            
            HyDEStrategy.VENDOR_COMPARISON: """
Write a vendor comparison or evaluation document that addresses this query: "{query}"

Include:
- Side-by-side vendor comparisons with specific criteria
- Scoring matrices and evaluation results
- Pros and cons analysis for each vendor option
- Cost comparison and ROI analysis
- Reference implementations and case studies
- Recommendation summary with justification

Write as a formal vendor evaluation or selection document.

Vendor Comparison:"""
        }
    
    def _select_strategy(self, query: str, business_context: Optional[Dict] = None) -> HyDEStrategy:
        """
        Intelligently select the best HyDE strategy based on query content and business context.
        
        Args:
            query: User's search query
            business_context: Optional business metadata (vendor, client, document type filters)
        """
        query_lower = query.lower()
        
        # Check for specific document type filters
        if business_context and 'document_type' in business_context:
            doc_type = business_context['document_type'].lower()
            if 'contract' in doc_type or 'sow' in doc_type:
                return HyDEStrategy.IT_VENDOR_CONTRACT
            elif 'quote' in doc_type or 'proposal' in doc_type:
                return HyDEStrategy.PRICING_PROPOSAL
            elif 'technical' in doc_type or 'specification' in doc_type:
                return HyDEStrategy.TECHNICAL_SPECIFICATION
            elif 'compliance' in doc_type or 'security' in doc_type:
                return HyDEStrategy.COMPLIANCE_SECURITY
        
        # Query-based strategy selection
        pricing_keywords = ['cost', 'price', 'pricing', 'quote', 'budget', 'fee', 'license', 'subscription']
        if any(keyword in query_lower for keyword in pricing_keywords):
            return HyDEStrategy.PRICING_PROPOSAL
        
        contract_keywords = ['contract', 'agreement', 'terms', 'sow', 'statement of work', 'vendor']
        if any(keyword in query_lower for keyword in contract_keywords):
            return HyDEStrategy.IT_VENDOR_CONTRACT
        
        technical_keywords = ['technical', 'specification', 'architecture', 'implementation', 'integration']
        if any(keyword in query_lower for keyword in technical_keywords):
            return HyDEStrategy.TECHNICAL_SPECIFICATION
        
        compliance_keywords = ['compliance', 'security', 'audit', 'risk', 'policy', 'regulation']
        if any(keyword in query_lower for keyword in compliance_keywords):
            return HyDEStrategy.COMPLIANCE_SECURITY
        
        comparison_keywords = ['compare', 'comparison', 'versus', 'vs', 'evaluate', 'evaluation', 'best']
        if any(keyword in query_lower for keyword in comparison_keywords):
            return HyDEStrategy.VENDOR_COMPARISON
        
        # Default to generic business for general queries
        return HyDEStrategy.GENERIC_BUSINESS
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8)
    )
    def _generate_hypothetical_document(self, prompt: str) -> Tuple[str, Dict[str, int]]:
        """Generate a single hypothetical document using OpenAI"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                top_p=0.9
            )
            
            content = response.choices[0].message.content.strip()
            token_usage = {
                'input_tokens': response.usage.prompt_tokens,
                'output_tokens': response.usage.completion_tokens,
                'total_tokens': response.usage.total_tokens
            }
            
            return content, token_usage
            
        except Exception as e:
            logger.error(f"Error generating hypothetical document: {str(e)}")
            raise
    
    def transform_query(
        self, 
        query: str, 
        business_context: Optional[Dict] = None,
        strategy: Optional[HyDEStrategy] = None,
        num_documents: int = 1
    ) -> HyDEResult:
        """
        Transform a query into hypothetical documents for improved retrieval.
        
        Args:
            query: Original user query
            business_context: Business metadata from filters (vendor, client, document type)
            strategy: Specific HyDE strategy to use (auto-selected if None)
            num_documents: Number of hypothetical documents to generate
            
        Returns:
            HyDEResult with generated hypothetical documents and metadata
        """
        start_time = time.time()
        
        # Select strategy if not provided
        if strategy is None:
            strategy = self._select_strategy(query, business_context)
        
        logger.info(f"🔍 HyDE generating {num_documents} document(s) using strategy: {strategy.value}")
        
        # Get prompt template
        prompt_template = self.prompt_templates[strategy]
        
        # Generate hypothetical documents
        hypothetical_documents = []
        total_token_usage = {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}
        
        for i in range(num_documents):
            try:
                # Enhance prompt with business context if available
                enhanced_query = self._enhance_query_with_context(query, business_context)
                prompt = prompt_template.format(query=enhanced_query)
                
                document, token_usage = self._generate_hypothetical_document(prompt)
                hypothetical_documents.append(document)
                
                # Accumulate token usage
                for key in total_token_usage:
                    total_token_usage[key] += token_usage[key]
                
                logger.debug(f"📄 Generated hypothetical document {i+1}: {len(document)} chars")
                
            except Exception as e:
                logger.error(f"Failed to generate hypothetical document {i+1}: {str(e)}")
                continue
        
        generation_time = time.time() - start_time
        confidence_score = len(hypothetical_documents) / num_documents  # Success rate
        
        logger.info(f"✅ HyDE completed: {len(hypothetical_documents)}/{num_documents} documents in {generation_time:.2f}s")
        
        return HyDEResult(
            hypothetical_documents=hypothetical_documents,
            strategy_used=strategy,
            generation_time=generation_time,
            token_usage=total_token_usage,
            confidence_score=confidence_score,
            business_context=business_context
        )
    
    def _enhance_query_with_context(self, query: str, business_context: Optional[Dict]) -> str:
        """Enhance query with business context for more targeted document generation"""
        if not business_context:
            return query
        
        context_parts = []
        
        if business_context.get('vendor'):
            context_parts.append(f"vendor: {business_context['vendor']}")
        
        if business_context.get('client'):
            context_parts.append(f"client: {business_context['client']}")
        
        if business_context.get('year'):
            context_parts.append(f"year: {business_context['year']}")
        
        if business_context.get('document_type'):
            context_parts.append(f"document type: {business_context['document_type']}")
        
        if context_parts:
            context_str = " (" + ", ".join(context_parts) + ")"
            return query + context_str
        
        return query
    
    def get_strategy_info(self) -> Dict[str, str]:
        """Get information about available HyDE strategies"""
        return {
            strategy.value: {
                'name': strategy.value.replace('_', ' ').title(),
                'description': self._get_strategy_description(strategy)
            }
            for strategy in HyDEStrategy
        }
    
    def _get_strategy_description(self, strategy: HyDEStrategy) -> str:
        """Get human-readable description of a strategy"""
        descriptions = {
            HyDEStrategy.GENERIC_BUSINESS: "General business documents with professional language",
            HyDEStrategy.IT_VENDOR_CONTRACT: "IT vendor contracts with technical specs and pricing",
            HyDEStrategy.PRICING_PROPOSAL: "Detailed pricing proposals with cost breakdowns",
            HyDEStrategy.TECHNICAL_SPECIFICATION: "Technical specifications and implementation guides",
            HyDEStrategy.COMPLIANCE_SECURITY: "Compliance and security documentation",
            HyDEStrategy.VENDOR_COMPARISON: "Vendor comparison and evaluation documents"
        }
        return descriptions.get(strategy, "Unknown strategy")