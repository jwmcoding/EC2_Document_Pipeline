"""
LLM Classifier Factory

Factory pattern for creating LLM document classifiers based on configuration.
Supports multiple providers: OpenAI (GPT-4.1-mini) and Ollama (Phi-4, etc.)
"""

import logging
from typing import Optional, Dict, Any

from .base_llm_classifier import BaseLLMClassifier

class LLMClassifierFactory:
    """Factory for creating LLM document classifiers"""
    
    @staticmethod
    def create_classifier(llm_config: Dict[str, Any]) -> BaseLLMClassifier:
        """
        Create an LLM classifier based on configuration
        
        Args:
            llm_config: Configuration dict with provider and settings
                       Expected format:
                       - OpenAI: {"provider": "openai", "api_key": "...", "model": "gpt-4.1-mini"}
                       - Ollama: {"provider": "ollama", "base_url": "...", "model": "phi4"}
        
        Returns:
            Configured LLM classifier instance
            
        Raises:
            ValueError: If provider is unsupported or configuration is invalid
            ImportError: If required dependencies are missing
        """
        provider = llm_config.get("provider", "").lower()
        logger = logging.getLogger(__name__)
        
        if provider == "openai":
            return LLMClassifierFactory._create_openai_classifier(llm_config, logger)
        elif provider == "ollama":
            return LLMClassifierFactory._create_ollama_classifier(llm_config, logger)
        else:
            raise ValueError(f"Unsupported LLM provider: {provider}")
    
    @staticmethod
    def _create_openai_classifier(config: Dict[str, Any], logger) -> BaseLLMClassifier:
        """Create OpenAI GPT-4.1-mini classifier"""
        try:
            from .llm_document_classifier import LLMDocumentClassifier
            
            api_key = config.get("api_key")
            model = config.get("model", "gpt-4.1-mini")
            
            if not api_key:
                raise ValueError("OpenAI API key is required")
            
            classifier = LLMDocumentClassifier(api_key=api_key, model=model)
            logger.info(f"✅ Created OpenAI classifier with model: {model}")
            return classifier
            
        except ImportError as e:
            raise ImportError(f"OpenAI dependencies not available: {e}")
    
    @staticmethod
    def _create_ollama_classifier(config: Dict[str, Any], logger) -> BaseLLMClassifier:
        """Create Ollama local LLM classifier"""
        try:
            from .ollama_document_classifier import OllamaDocumentClassifier
            
            base_url = config.get("base_url", "http://localhost:11434")
            model = config.get("model", "phi4")
            
            classifier = OllamaDocumentClassifier(
                model_name=model,
                ollama_base_url=base_url
            )
            logger.info(f"✅ Created Ollama classifier with model: {model} at {base_url}")
            return classifier
            
        except ImportError as e:
            raise ImportError(f"Ollama dependencies not available: {e}")
        except Exception as e:
            # Re-raise connection errors from Ollama
            raise
    
    @staticmethod
    def get_available_providers() -> Dict[str, Dict[str, Any]]:
        """
        Get information about available LLM providers
        
        Returns:
            Dict with provider info and availability status
        """
        providers = {}
        
        # Check OpenAI availability
        try:
            import openai
            providers["openai"] = {
                "available": True,
                "description": "OpenAI GPT-4.1-mini cloud inference",
                "models": ["gpt-4.1-mini", "gpt-4o-mini", "gpt-3.5-turbo"],
                "cost": "Pay per token (API charges apply)",
                "requires": ["OPENAI_API_KEY"]
            }
        except ImportError:
            providers["openai"] = {
                "available": False,
                "description": "OpenAI GPT-4.1-mini cloud inference",
                "error": "openai package not installed"
            }
        
        # Check Ollama availability
        try:
            import requests
            providers["ollama"] = {
                "available": True,
                "description": "Local LLM inference with Ollama",
                "models": ["phi4", "phi3", "llama3.2", "qwen2.5", "mistral"],
                "cost": "Free (local inference)",
                "requires": ["Ollama installed and running locally"]
            }
        except ImportError:
            providers["ollama"] = {
                "available": False,
                "description": "Local LLM inference with Ollama", 
                "error": "requests package not installed"
            }
        
        return providers
    
    @staticmethod
    def validate_provider_config(provider: str, config: Dict[str, Any]) -> bool:
        """
        Validate configuration for a specific provider
        
        Args:
            provider: Provider name ("openai" or "ollama")
            config: Configuration dictionary
            
        Returns:
            True if valid, raises ValueError if invalid
        """
        if provider.lower() == "openai":
            if not config.get("api_key"):
                raise ValueError("OpenAI API key is required")
            return True
            
        elif provider.lower() == "ollama":
            base_url = config.get("base_url", "http://localhost:11434")
            if not base_url.startswith(('http://', 'https://')):
                raise ValueError("Ollama base_url must be a valid URL")
            return True
            
        else:
            raise ValueError(f"Unknown provider: {provider}")

# Convenience functions for common use cases
def create_openai_classifier(api_key: str, model: str = "gpt-4.1-mini") -> BaseLLMClassifier:
    """Convenience function to create OpenAI classifier"""
    config = {"provider": "openai", "api_key": api_key, "model": model}
    return LLMClassifierFactory.create_classifier(config)

def create_ollama_classifier(model: str = "phi4", base_url: str = "http://localhost:11434") -> BaseLLMClassifier:
    """Convenience function to create Ollama classifier"""
    config = {"provider": "ollama", "model": model, "base_url": base_url}
    return LLMClassifierFactory.create_classifier(config) 