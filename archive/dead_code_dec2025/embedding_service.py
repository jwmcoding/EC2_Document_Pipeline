"""
Embedding Service with OpenAI Integration
Handles text embedding generation with batch processing and retry logic
"""

from openai import OpenAI
from typing import List, Dict, Any
import numpy as np
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import time
from dataclasses import dataclass


@dataclass
class EmbeddingResult:
    """Result container for embedding operations"""
    embeddings: List[List[float]]
    token_count: int
    processing_time: float
    success_count: int
    error_count: int
    errors: List[str]


class EmbeddingService:
    """OpenAI embedding service with batch processing and error handling"""
    
    def __init__(self, api_key: str, model: str = "text-embedding-3-large"):
        """Initialize the embedding service
        
        Args:
            api_key: OpenAI API key
            model: Embedding model to use (text-embedding-3-large or text-embedding-3-small)
        """
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.logger = logging.getLogger(__name__)
        
        # Model configurations
        self.model_configs = {
            "text-embedding-3-large": {"dimension": 3072, "max_tokens": 8191},
            "text-embedding-3-small": {"dimension": 1536, "max_tokens": 8191},
            "text-embedding-ada-002": {"dimension": 1536, "max_tokens": 8191}
        }
        
        self.dimension = self.model_configs.get(model, {"dimension": 1536})["dimension"]
        self.max_tokens = self.model_configs.get(model, {"max_tokens": 8191})["max_tokens"]
        
        # Rate limiting and batch settings
        self.max_batch_size = 100  # OpenAI's current limit
        self.requests_per_minute = 1000  # Conservative rate limiting
        self.tokens_per_minute = 150000  # Conservative token rate limiting
        
        self.logger.info(f"Initialized EmbeddingService with model {model} (dimension: {self.dimension})")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for a single text"""
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            self.logger.error(f"Error generating embedding: {str(e)}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def embed_batch(self, texts: List[str], batch_size: int = None) -> EmbeddingResult:
        """Generate embeddings for multiple texts with intelligent batching"""
        
        if batch_size is None:
            batch_size = min(self.max_batch_size, 50)  # Conservative default
        
        start_time = time.time()
        all_embeddings = []
        total_tokens = 0
        success_count = 0
        error_count = 0
        errors = []
        
        # Process in batches to respect API limits
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            try:
                # Estimate tokens for rate limiting
                estimated_tokens = sum(len(text.split()) * 1.3 for text in batch)  # Rough estimate
                
                self.logger.debug(f"Processing batch {i//batch_size + 1} with {len(batch)} texts "
                                f"(~{estimated_tokens:.0f} tokens)")
                
                response = self.client.embeddings.create(
                    model=self.model,
                    input=batch
                )
                
                # Extract embeddings
                batch_embeddings = [item.embedding for item in response.data]
                all_embeddings.extend(batch_embeddings)
                
                # Track usage
                total_tokens += response.usage.total_tokens if response.usage else estimated_tokens
                success_count += len(batch)
                
                # Rate limiting delay if processing many batches
                if i + batch_size < len(texts):
                    delay = self._calculate_rate_limit_delay(len(batch), estimated_tokens)
                    if delay > 0:
                        self.logger.debug(f"Rate limiting delay: {delay:.2f}s")
                        time.sleep(delay)
                
            except Exception as e:
                error_msg = f"Error processing batch {i//batch_size + 1}: {str(e)}"
                self.logger.error(error_msg)
                errors.append(error_msg)
                error_count += len(batch)
                
                # Add zero embeddings for failed items to maintain indexing
                zero_embeddings = [[0.0] * self.dimension for _ in batch]
                all_embeddings.extend(zero_embeddings)
        
        processing_time = time.time() - start_time
        
        result = EmbeddingResult(
            embeddings=all_embeddings,
            token_count=total_tokens,
            processing_time=processing_time,
            success_count=success_count,
            error_count=error_count,
            errors=errors
        )
        
        self.logger.info(f"Batch embedding completed: {success_count} success, {error_count} errors, "
                        f"{processing_time:.2f}s, {total_tokens} tokens")
        
        return result
    
    def embed_chunks(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add embeddings to document chunks"""
        
        if not chunks:
            self.logger.warning("No chunks provided for embedding")
            return chunks
        
        # Extract texts and prepare for batch processing
        texts = []
        chunk_indices = []
        
        for i, chunk in enumerate(chunks):
            text = chunk.get('text', '')
            if text.strip():
                # Truncate text if too long
                text = self._truncate_text(text)
                texts.append(text)
                chunk_indices.append(i)
            else:
                self.logger.warning(f"Chunk {i} has no text content")
        
        if not texts:
            self.logger.error("No valid text found in chunks")
            return chunks
        
        # Generate embeddings
        embedding_result = self.embed_batch(texts)
        
        # Add embeddings back to chunks
        for i, chunk_idx in enumerate(chunk_indices):
            if i < len(embedding_result.embeddings):
                chunks[chunk_idx]['embedding'] = embedding_result.embeddings[i]
                
                # Add embedding metadata
                chunks[chunk_idx]['metadata']['embedding_model'] = self.model
                chunks[chunk_idx]['metadata']['embedding_dimension'] = self.dimension
                chunks[chunk_idx]['metadata']['embedding_success'] = i < embedding_result.success_count
            else:
                self.logger.error(f"Missing embedding for chunk {chunk_idx}")
                chunks[chunk_idx]['embedding'] = [0.0] * self.dimension
                chunks[chunk_idx]['metadata']['embedding_success'] = False
        
        # Log summary
        self.logger.info(f"Embedded {len(texts)} chunks using {embedding_result.token_count} tokens "
                        f"in {embedding_result.processing_time:.2f}s")
        
        return chunks
    
    def _truncate_text(self, text: str, max_tokens: int = None) -> str:
        """Truncate text to fit within token limits"""
        
        if max_tokens is None:
            max_tokens = self.max_tokens
        
        # Rough approximation: 1 token ≈ 4 characters for English text
        max_chars = max_tokens * 4
        
        if len(text) <= max_chars:
            return text
        
        # Truncate and try to end at a sentence boundary
        truncated = text[:max_chars]
        
        # Find last sentence boundary
        for delimiter in ['. ', '! ', '? ', '\n\n', '\n']:
            last_delimiter = truncated.rfind(delimiter)
            if last_delimiter > max_chars * 0.8:  # Only if we don't lose too much
                truncated = truncated[:last_delimiter + len(delimiter)]
                break
        
        self.logger.warning(f"Truncated text from {len(text)} to {len(truncated)} characters")
        return truncated
    
    def _calculate_rate_limit_delay(self, batch_size: int, estimated_tokens: float) -> float:
        """Calculate delay needed for rate limiting"""
        
        # Simple rate limiting based on requests per minute
        requests_delay = 60.0 / self.requests_per_minute
        
        # Token-based rate limiting
        tokens_delay = (estimated_tokens / self.tokens_per_minute) * 60.0
        
        # Use the more restrictive limit
        return max(requests_delay, tokens_delay)
    
    def get_embedding_stats(self) -> Dict[str, Any]:
        """Get information about the embedding service configuration"""
        
        return {
            "model": self.model,
            "dimension": self.dimension,
            "max_tokens": self.max_tokens,
            "max_batch_size": self.max_batch_size,
            "rate_limits": {
                "requests_per_minute": self.requests_per_minute,
                "tokens_per_minute": self.tokens_per_minute
            }
        }
    
    def validate_embeddings(self, embeddings: List[List[float]]) -> Dict[str, Any]:
        """Validate a list of embeddings for consistency"""
        
        if not embeddings:
            return {"valid": False, "error": "No embeddings provided"}
        
        # Check dimensions
        dimensions = [len(emb) for emb in embeddings]
        expected_dim = self.dimension
        
        invalid_dimensions = [i for i, dim in enumerate(dimensions) if dim != expected_dim]
        
        # Check for zero vectors (often indicates failed embedding)
        zero_vectors = []
        for i, emb in enumerate(embeddings):
            if all(val == 0.0 for val in emb):
                zero_vectors.append(i)
        
        # Check for valid numeric values
        invalid_values = []
        for i, emb in enumerate(embeddings):
            if any(not isinstance(val, (int, float)) or np.isnan(val) or np.isinf(val) for val in emb):
                invalid_values.append(i)
        
        validation_result = {
            "valid": len(invalid_dimensions) == 0 and len(invalid_values) == 0,
            "total_embeddings": len(embeddings),
            "expected_dimension": expected_dim,
            "issues": {
                "invalid_dimensions": invalid_dimensions,
                "zero_vectors": zero_vectors,
                "invalid_values": invalid_values
            }
        }
        
        if zero_vectors:
            self.logger.warning(f"Found {len(zero_vectors)} zero vectors in embeddings")
        
        if invalid_dimensions:
            self.logger.error(f"Found {len(invalid_dimensions)} embeddings with wrong dimensions")
        
        if invalid_values:
            self.logger.error(f"Found {len(invalid_values)} embeddings with invalid values")
        
        return validation_result
    
    def estimate_cost(self, texts: List[str]) -> Dict[str, Any]:
        """Estimate the cost for embedding a list of texts"""
        
        # Rough token estimation
        total_chars = sum(len(text) for text in texts)
        estimated_tokens = total_chars / 4  # Rough approximation
        
        # OpenAI pricing (as of late 2023/early 2024 - should be updated)
        pricing = {
            "text-embedding-3-large": 0.00013 / 1000,  # per 1K tokens
            "text-embedding-3-small": 0.00002 / 1000,  # per 1K tokens
            "text-embedding-ada-002": 0.0001 / 1000    # per 1K tokens
        }
        
        cost_per_token = pricing.get(self.model, pricing["text-embedding-ada-002"])
        estimated_cost = estimated_tokens * cost_per_token
        
        return {
            "model": self.model,
            "estimated_tokens": int(estimated_tokens),
            "estimated_cost_usd": round(estimated_cost, 4),
            "text_count": len(texts),
            "total_characters": total_chars,
            "note": "Cost estimate based on approximate token calculation"
        } 