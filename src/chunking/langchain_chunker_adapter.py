"""
LangChain Chunker Adapter

This module provides an adapter to make the LangChain SemanticChunker
compatible with our existing document processing pipeline.
"""

from typing import List, Dict, Any
from langchain_experimental.text_splitter import SemanticChunker as LangchainSemanticChunker
from src.chunking.semantic_chunker import Chunk
from src.embeddings.langchain_embedding_wrapper import LangchainEmbeddingWrapper
import logging

class LangchainChunkerAdapter:
    """
    An adapter for the LangChain SemanticChunker to make it compatible
    with our pipeline's expected Chunk structure.
    """

    def __init__(self, pinecone_client):
        """
        Initializes the adapter.
        
        Args:
            pinecone_client: Our internal Pinecone client.
        """
        self.logger = logging.getLogger(__name__)
        
        # Wrap our Pinecone client to be compatible with LangChain
        langchain_embeddings = LangchainEmbeddingWrapper(pinecone_client)
        
        # Instantiate the LangChain SemanticChunker
        # Using recommended defaults for our document types.
        self.langchain_chunker = LangchainSemanticChunker(
            embeddings=langchain_embeddings,
            breakpoint_threshold_type="percentile" # A good starting point
        )

    def chunk_document(self, content: str, metadata: Dict[str, Any]) -> List[Chunk]:
        """
        Chunks the document using the LangChain SemanticChunker and adapts
        the output to our pipeline's Chunk format.
        
        Args:
            content: The text content of the document to chunk.
            metadata: The metadata associated with the document.
            
        Returns:
            A list of structured Chunk objects.
        """
        self.logger.info("Chunking document using LangChain's SemanticChunker...")
        
        if not content or len(content.strip()) < 50:
            self.logger.warning("Content too short for semantic chunking, returning empty list.")
            return []

        try:
            # Use the LangChain chunker to split the text
            texts = self.langchain_chunker.split_text(content)
            
            chunks = []
            for i, text_chunk in enumerate(texts):
                # Adapt the simple text chunk to our structured Chunk object
                chunk_metadata = {
                    **metadata,
                    "chunk_index": i,
                    "chunk_length": len(text_chunk),
                    "word_count": len(text_chunk.split()),
                    "section_name": "semantic",  # Sectioning is handled by the chunker itself
                    "chunk_type": "semantic",
                }
                
                chunk = Chunk(
                    text=text_chunk,
                    metadata=chunk_metadata,
                    start_index=0,  # Start index isn't critical for this method
                    end_index=len(text_chunk)
                )
                chunks.append(chunk)
            
            self.logger.info(f"Successfully created {len(chunks)} chunks using semantic strategy.")
            return chunks
            
        except Exception as e:
            self.logger.error(f"Error using LangChain SemanticChunker: {e}")
            # In case of an error, return an empty list to prevent pipeline failure
            return [] 