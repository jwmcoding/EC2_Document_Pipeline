"""
Chunker Factory

This module provides a factory for creating different types of text chunkers.
It allows the pipeline to dynamically select a chunking strategy at runtime.
"""

from typing import Literal
from src.chunking.semantic_chunker import SemanticChunker as BusinessAwareChunker
import logging

class ChunkerFactory:
    """Factory for creating text chunker instances."""

    def __init__(self, pinecone_client):
        """
        Initializes the factory with the Pinecone client.
        
        Args:
            pinecone_client: The Pinecone client instance to be used by chunkers.
        """
        self.pinecone_client = pinecone_client
        self.logger = logging.getLogger(__name__)

    def create_chunker(self, strategy: Literal['business_aware', 'semantic']):
        """
        Creates a chunker based on the specified strategy.
        
        Args:
            strategy: The chunking strategy to use.
            
        Returns:
            An instance of a configured text chunker.
            
        Raises:
            ValueError: If an unknown strategy is provided.
        """
        self.logger.info(f"Creating chunker with strategy: '{strategy}'")
        
        if strategy == 'business_aware':
            # Our existing, highly customized business-aware chunker
            return BusinessAwareChunker()
            
        elif strategy == 'semantic':
            # The LangChain Semantic Chunker is optional; import lazily
            try:
                from src.chunking.langchain_chunker_adapter import LangchainChunkerAdapter  # type: ignore
                return LangchainChunkerAdapter(self.pinecone_client)
            except Exception as e:
                self.logger.warning(
                    f"LangChain semantic chunker unavailable ({e}); falling back to business_aware."
                )
                return BusinessAwareChunker()
            
        else:
            self.logger.error(f"Unknown chunking strategy: {strategy}")
            raise ValueError(f"Unknown chunking strategy: {strategy}") 