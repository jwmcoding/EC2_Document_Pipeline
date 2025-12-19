"""
PII Redaction Module

Provides redaction services to remove client names, people names, emails, 
phone numbers, and addresses from document text before chunking and embedding.
"""

from .redaction_context import RedactionContext, RedactionResult
from .redaction_service import RedactionService
from .client_registry import ClientRegistry

__all__ = [
    'RedactionContext',
    'RedactionResult', 
    'RedactionService',
    'ClientRegistry',
]

