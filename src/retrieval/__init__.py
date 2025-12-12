"""
Retrieval components for advanced document search and query transformation.

Includes HyDE (Hypothetical Document Embeddings) for improved retrieval performance
in business document scenarios.
"""

from .hyde_query_transformer import (
    BusinessDocumentHyDETransformer,
    HyDEStrategy,
    HyDEResult
)

__all__ = [
    'BusinessDocumentHyDETransformer',
    'HyDEStrategy', 
    'HyDEResult'
]