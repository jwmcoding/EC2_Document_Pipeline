"""
Strict mode validators for redaction quality assurance

Validates that redaction was successful and no PII remains.
"""

import logging
import re
from typing import List, Optional
from .pii_patterns import PIIPatterns
from .client_registry import ClientRegistry


class RedactionValidators:
    """Validators for strict mode redaction quality checks"""
    
    def __init__(self, client_registry: ClientRegistry):
        """
        Initialize validators.
        
        Args:
            client_registry: Client registry for client name validation
        """
        self.client_registry = client_registry
        self.logger = logging.getLogger(__name__)
    
    def validate(
        self,
        redacted_text: str,
        salesforce_client_id: Optional[str] = None,
        file_type: Optional[str] = None,
    ) -> List[str]:
        """
        Run all validators and return list of failures.
        
        Args:
            redacted_text: Text after redaction
            salesforce_client_id: Salesforce client ID (for client name validation)
            
        Returns:
            List of validation failure messages (empty if all pass)
        """
        failures = []
        
        # Check for remaining emails
        if PIIPatterns.has_email(redacted_text):
            failures.append("Email addresses still detected in redacted text")
        
        # Strict-mode Option A:
        # - Spreadsheets are extremely number-dense; "phone-like" digit sequences can be
        #   common and lead to false failures even after redaction.
        # - We still redact phones in spreadsheets, but we do not strict-fail the document
        #   on remaining phone matches for spreadsheet file types.
        spreadsheet_types = {".xlsx", ".xls", ".csv"}
        if (file_type or "").lower() not in spreadsheet_types:
            # Check for remaining phone numbers
            if PIIPatterns.has_phone(redacted_text):
                failures.append("Phone numbers still detected in redacted text")
        
        # Check for remaining client names (if client ID provided)
        if salesforce_client_id:
            client_info = self.client_registry.get_client_info(salesforce_client_id)
            if client_info:
                client_name = client_info['client_name']
                # Check if client name still appears (case-insensitive), using the same
                # "alnum boundary" concept as ClientRegistry matching. This avoids both:
                # - false positives from substring matching
                # - false negatives around underscores/hyphens/etc.
                escaped = re.escape(client_name)
                pat = re.compile(r'(?<![A-Za-z0-9])' + escaped + r'(?![A-Za-z0-9])', re.IGNORECASE)
                if pat.search(redacted_text):
                    failures.append(f"Client name '{client_name}' still detected in redacted text")
        
        return failures

