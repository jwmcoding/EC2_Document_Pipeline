"""
Redaction context and result data structures
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class RedactionContext:
    """Context information for redaction operations"""
    
    # Client information (required for client name redaction)
    salesforce_client_id: Optional[str] = None
    client_name: Optional[str] = None
    industry_label: Optional[str] = None
    
    # Vendor information (used to avoid redacting vendor names)
    vendor_name: Optional[str] = None
    
    # Document metadata (optional, for context)
    file_type: Optional[str] = None
    document_type: Optional[str] = None
    
    def has_client_info(self) -> bool:
        """Check if we have enough info to redact client names"""
        # We only need the Salesforce client id; the ClientRegistry is the
        # source of truth for industry label + aliases.
        return bool(self.salesforce_client_id)


@dataclass
class RedactionResult:
    """Result of a redaction operation"""
    
    # Core result
    redacted_text: str
    success: bool = True
    
    # Statistics
    client_replacements: int = 0
    email_replacements: int = 0
    phone_replacements: int = 0
    address_replacements: int = 0
    person_replacements: int = 0
    
    # Metadata
    model_used: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    
    # Validation results (for strict mode)
    validation_passed: bool = True
    validation_failures: List[str] = field(default_factory=list)
    
    def total_replacements(self) -> int:
        """Total number of replacements made"""
        return (
            self.client_replacements +
            self.email_replacements +
            self.phone_replacements +
            self.address_replacements +
            self.person_replacements
        )
    
    def has_errors(self) -> bool:
        """Check if redaction had errors"""
        return len(self.errors) > 0 or not self.success
    
    def has_validation_failures(self) -> bool:
        """Check if validation failed (strict mode)"""
        return len(self.validation_failures) > 0 or not self.validation_passed

