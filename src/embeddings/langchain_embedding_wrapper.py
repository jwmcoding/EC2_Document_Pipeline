"""
LangChain Embedding Wrapper

This module provides a wrapper to make our internal EmbeddingService compatible
with the standard LangChain Embeddings interface. This allows us to use
our existing, Pinecone-optimized embedding logic with any LangChain component
that requires an embedding model, such as the SemanticChunker.
"""

from typing import List
from langchain_core.embeddings import Embeddings
import logging

class LangchainEmbeddingWrapper(Embeddings):
    """
    A wrapper to make our internal Pinecone embedding system conform to the
    LangChain Embeddings interface.
    """
    
    def __init__(self, pinecone_client):
        """
        Initializes the wrapper with our internal Pinecone client.
        
        Args:
            pinecone_client: An instance of our project's PineconeDocumentClient.
        """
        self.pinecone_client = pinecone_client
        self.logger = logging.getLogger(__name__)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Embed a list of documents using our internal Pinecone service.
        
        This method is required by the LangChain Embeddings interface.
        
        Args:
            texts: A list of document texts to embed.
            
        Returns:
            A list of embedding vectors.
        """
        self.logger.info(f"Embedding {len(texts)} documents for LangChain component...")
        try:
            # Use Pinecone's _generate_embeddings method to get dense embeddings
            embeddings_result = self.pinecone_client._generate_embeddings(texts)
            dense_embeddings = embeddings_result['dense_embeddings']
            self.logger.info("Successfully generated embeddings.")
            return dense_embeddings
        except Exception as e:
            self.logger.error(f"Error during LangChain document embedding: {e}")
            # Return empty embeddings for each text in case of failure
            return [[] for _ in texts]

    def embed_query(self, text: str) -> List[float]:
        """
        Embed a single query using our internal Pinecone service.
        
        This method is required by the LangChain Embeddings interface.
        
        Args:
            text: A single query text to embed.
            
        Returns:
            An embedding vector for the query.
        """
        self.logger.info("Embedding a single query for LangChain component...")
        try:
            # Use Pinecone's _generate_embeddings for single query (in a list)
            embeddings_result = self.pinecone_client._generate_embeddings([text])
            dense_embeddings = embeddings_result['dense_embeddings']
            if dense_embeddings:
                self.logger.info("Successfully generated query embedding.")
                return dense_embeddings[0]
            else:
                self.logger.error("Failed to generate query embedding.")
                return []
        except Exception as e:
            self.logger.error(f"Error during LangChain query embedding: {e}")
            return [] 