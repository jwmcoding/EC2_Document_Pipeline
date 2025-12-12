"""
Abstract Base Class for LLM Document Classifiers

This module provides the interface that all LLM document classifiers must implement,
enabling support for multiple LLM providers (OpenAI, Ollama, etc.) in the document
processing pipeline.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from enum import Enum

class DocumentType(Enum):
    """Specific business document types for deal processing"""
    FMV_REPORT = "FMV Report"
    QUOTE_PROPOSAL = "Quote/Proposal"
    CONTRACT_SOW_MSA = "Contract/SoW/MSA"
    PRODUCT_LIT = "Product Lit"
    EMAIL = "Email"

@dataclass
class LLMClassificationResult:
    """Result from LLM document classification"""
    document_type: DocumentType
    confidence: float  # 0.0 - 1.0
    reasoning: str
    alternative_types: List[Tuple[DocumentType, float]]
    classification_method: str = "llm"
    tokens_used: int = 0

@dataclass
class EnhancedLLMClassificationResult:
    """Comprehensive document metadata extraction result (v3)"""
    # Core classification (required fields first)
    document_type: DocumentType
    confidence: float  # 0.0 - 1.0
    # reasoning removed to reduce output tokens
    
    # Business depth analysis (required fields)
    product_pricing_depth: str  # "low", "medium", "high"
    commercial_terms_depth: str  # "low", "medium", "high"
    
    # Optional fields with defaults
    proposed_term_start: Optional[str] = None  # "YYYY-MM-DD" or None
    proposed_term_end: Optional[str] = None    # "YYYY-MM-DD" or None
    alternative_types: List[Tuple[DocumentType, float]] = field(default_factory=list)
    classification_method: str = "llm_enhanced"
    tokens_used: int = 0
    # Pruned fields removed: content_summary, key_topics, vendor_products_mentioned, pricing_indicators

class BaseLLMClassifier(ABC):
    """Abstract base class for LLM document classifiers"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.total_tokens_used = 0
        self.classification_count = 0
    
    @abstractmethod
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
        Classify document using LLM with business context
        
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
        pass
    
    @abstractmethod
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
        Enhanced document classification with comprehensive metadata extraction
        
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
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics for cost monitoring"""
        pass
    
    def _map_string_to_enum(self, type_string: str) -> DocumentType:
        """Map LLM response string to DocumentType enum - shared implementation"""
        
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
            
            # Abbreviated forms
            "IDD": DocumentType.CONTRACT_SOW_MSA,
            "FMV": DocumentType.FMV_REPORT,
            "Email/Communication": DocumentType.EMAIL,
            "Other": DocumentType.QUOTE_PROPOSAL,
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