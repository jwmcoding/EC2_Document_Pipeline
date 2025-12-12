"""
Classification Module for Business Document Processing

Provides LLM-based document classification with support for multiple providers:
- OpenAI GPT-4.1-mini (cloud, high accuracy, API costs)
- Ollama with Phi-4/other models (local, free, private)

Main Classes:
- LLMDocumentClassifier: OpenAI-based classifier
- OllamaDocumentClassifier: Local Ollama-based classifier  
- LLMClassifierFactory: Factory for creating classifiers
"""

from .base_llm_classifier import (
    BaseLLMClassifier,
    DocumentType,
    LLMClassificationResult,
    EnhancedLLMClassificationResult
)

from .llm_classifier_factory import (
    LLMClassifierFactory,
    create_openai_classifier,
    create_ollama_classifier
)

# Import specific implementations
try:
    from .llm_document_classifier import LLMDocumentClassifier
except ImportError:
    LLMDocumentClassifier = None

try:
    from .ollama_document_classifier import OllamaDocumentClassifier
except ImportError:
    OllamaDocumentClassifier = None

__all__ = [
    # Base classes and types
    "BaseLLMClassifier",
    "DocumentType", 
    "LLMClassificationResult",
    "EnhancedLLMClassificationResult",
    
    # Factory and convenience functions
    "LLMClassifierFactory",
    "create_openai_classifier",
    "create_ollama_classifier",
    
    # Specific implementations
    "LLMDocumentClassifier",
    "OllamaDocumentClassifier"
]

