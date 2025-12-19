"""
Centralized PII detection patterns (regex)

These patterns are used for deterministic detection of emails, phone numbers,
and addresses. Keep them centralized for easy testing and maintenance.
"""

import re
from typing import List, Tuple, Pattern


class PIIPatterns:
    """Centralized regex patterns for PII detection"""
    
    # Email pattern (conservative - requires @ and domain)
    EMAIL_PATTERN: Pattern = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        re.IGNORECASE
    )
    
    # Phone pattern (US formats, incl. dot-separated: 404.123.4567)
    #
    # IMPORTANT: We avoid matching phone-like substrings inside decimal numbers,
    # which can appear frequently in spreadsheet-derived text (e.g., "0.5900001234").
    # The negative lookbehinds prevent matches that begin:
    # - immediately after a digit (part of a longer number), or
    # - immediately after a "<digit>." decimal prefix.
    PHONE_PATTERN: Pattern = re.compile(
        r'(?<!\d)(?<!\d\.)'                       # not inside a longer number / decimal
        r'(?:\+?1[-.\s]?)?'                       # optional country code
        r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'    # main 10-digit phone
        r'(?:\s*(?:x|ext\.?)\s*\d{1,5})?'         # optional extension
        r'\b'
    )
    
    # Address pattern (conservative - looks for street number + street name + suffix)
    # Matches patterns like "123 Main St", "456 Oak Avenue", "789 Park Blvd Suite 100"
    ADDRESS_PATTERN: Pattern = re.compile(
        r'\b\d+\s+[A-Za-z0-9\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Place|Pl|Way|Circle|Cir|Parkway|Pkwy)(?:\s+(?:Suite|Ste|Unit|Apt|Apartment|Room|Rm|Floor|Fl|#)\s*\d+)?\b',
        re.IGNORECASE
    )
    
    @classmethod
    def find_emails(cls, text: str) -> List[Tuple[int, int]]:
        """
        Find all email addresses in text.
        
        Returns:
            List of (start, end) tuples for each match
        """
        matches = []
        for match in cls.EMAIL_PATTERN.finditer(text):
            matches.append((match.start(), match.end()))
        return matches
    
    @classmethod
    def find_phones(cls, text: str) -> List[Tuple[int, int]]:
        """
        Find all phone numbers in text.
        
        Returns:
            List of (start, end) tuples for each match
        """
        matches = []
        for match in cls.PHONE_PATTERN.finditer(text):
            matches.append((match.start(), match.end()))
        return matches
    
    @classmethod
    def find_addresses(cls, text: str) -> List[Tuple[int, int]]:
        """
        Find all addresses in text.
        
        Returns:
            List of (start, end) tuples for each match
        """
        matches = []
        for match in cls.ADDRESS_PATTERN.finditer(text):
            matches.append((match.start(), match.end()))
        return matches
    
    @classmethod
    def has_email(cls, text: str) -> bool:
        """Check if text contains any email addresses"""
        return bool(cls.EMAIL_PATTERN.search(text))
    
    @classmethod
    def has_phone(cls, text: str) -> bool:
        """Check if text contains any phone numbers"""
        return bool(cls.PHONE_PATTERN.search(text))
    
    @classmethod
    def has_address(cls, text: str) -> bool:
        """Check if text contains any addresses"""
        return bool(cls.ADDRESS_PATTERN.search(text))

