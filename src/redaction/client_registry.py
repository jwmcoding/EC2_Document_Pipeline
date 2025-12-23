"""
Client registry loader and deterministic alias generation

Loads client CSV and generates aliases for client name matching.
"""

import csv
import re
import logging
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path


class ClientRegistry:
    """Manages client information and alias generation for redaction"""
    
    # Placeholder token for client replacement
    CLIENT_PLACEHOLDER_TEMPLATE = "<<CLIENT: {industry_label}>>"
    
    def __init__(self, csv_path: Optional[str] = None):
        """
        Initialize client registry.
        
        Args:
            csv_path: Path to CSV file with columns: salesforce_client_id, client_name, industry_label, aliases (optional)
        """
        self.logger = logging.getLogger(__name__)
        self.clients: Dict[str, Dict[str, str]] = {}  # salesforce_client_id -> {client_name, industry_label, aliases}
        self.alias_patterns: Dict[str, List[re.Pattern]] = {}  # salesforce_client_id -> list of compiled regex patterns
        self.generated_variants: Dict[str, List[str]] = {}  # salesforce_client_id -> list of generated variant strings
        
        if csv_path:
            self.load_from_csv(csv_path)
    
    def load_from_csv(self, csv_path: str) -> None:
        """
        Load client registry from CSV file.
        
        Supports two CSV formats:
        
        1. Standard format (with industry_label):
           - salesforce_client_id: unique Salesforce ID
           - client_name: primary client name
           - industry_label: industry name (SIC code name) for replacement
           - aliases: (optional) pipe-delimited list of aliases
        
        2. Salesforce export format (SF-Cust-Mapping.csv):
           - 18 Digit ID: Salesforce client ID (used as key)
           - Account Name: client name
           - industry_label: generated dummy value if not present
           - aliases: optional column (if present)
        
        Example (standard):
        salesforce_client_id,client_name,industry_label,aliases
        001XX000003Url7MAC,Morgan Stanley,Investment Banking,"Morgan Stanley|MS|MorganStanley"
        
        Example (Salesforce export):
        Account Name,Website,Account ID,18 Digit ID
        Morgan Stanley,www.morganstanley.com,001XX000003Url7MAC,001XX000003Url7MAAAD
        """
        csv_path_obj = Path(csv_path)
        if not csv_path_obj.exists():
            self.logger.warning(f"Client CSV not found: {csv_path}")
            return
        
        self.logger.info(f"Loading client registry from: {csv_path}")
        
        try:
            # Use utf-8-sig to transparently handle BOM-prefixed headers (common in Excel exports)
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                count = 0
                
                # Detect CSV format by checking column names
                raw_fieldnames = reader.fieldnames or []
                fieldnames = [str(fn).lstrip("\ufeff").strip() for fn in raw_fieldnames if fn]
                is_salesforce_format = '18 Digit ID' in fieldnames and 'Account Name' in fieldnames
                
                for row in reader:
                    if is_salesforce_format:
                        # Salesforce export format
                        # Some exports may still include BOM in the first header key depending on how the CSV was written.
                        client_id = (row.get('18 Digit ID') or '').strip()
                        client_name = (row.get('Account Name') or row.get('\ufeffAccount Name') or '').strip()
                        aliases_str = (row.get('aliases') or '').strip()
                        
                        # Use Industry column if present, otherwise fallback to dummy
                        industry_label = (row.get('Industry') or '').strip()
                        if not industry_label:
                            industry_label = f"Client Organization"  # Fallback dummy value
                    else:
                        # Standard format
                        client_id = row.get('salesforce_client_id', '').strip()
                        client_name = row.get('client_name', '').strip()
                        industry_label = row.get('industry_label', '').strip()
                        aliases_str = row.get('aliases', '').strip()
                    
                    if not client_id or not client_name:
                        self.logger.warning(f"Skipping incomplete row: {row}")
                        continue
                    
                    # If industry_label is missing in standard format, use dummy
                    if not industry_label:
                        industry_label = f"Client Organization"  # Dummy value
                    
                    # Parse aliases (pipe-delimited)
                    aliases_list = []
                    if aliases_str:
                        aliases_list = [a.strip() for a in aliases_str.split('|') if a.strip()]
                    
                    self.clients[client_id] = {
                        'client_name': client_name,
                        'industry_label': industry_label,
                        'aliases': aliases_list
                    }
                    
                    # Generate deterministic aliases and compile patterns
                    self._generate_alias_patterns(client_id, client_name, aliases_list)
                    count += 1
                
                self.logger.info(f"Loaded {count} clients from CSV")
                if is_salesforce_format:
                    industry_count = sum(1 for c in self.clients.values() if c.get('industry_label') != 'Client Organization')
                    if industry_count > 0:
                        self.logger.info(f"Using Industry column for {industry_count} clients (fallback to 'Client Organization' for {count - industry_count} clients)")
                    else:
                        self.logger.info("Using dummy industry_label='Client Organization' (Industry column not found or empty)")
        
        except Exception as e:
            self.logger.error(f"Error loading client CSV: {e}")
            raise
    
    def _generate_alias_patterns(self, client_id: str, client_name: str, explicit_aliases: List[str]) -> None:
        """
        Generate deterministic aliases and compile regex patterns for matching.
        
        Args:
            client_id: Salesforce client ID
            client_name: Primary client name
            explicit_aliases: Explicit aliases from CSV
        """
        all_names: Set[str] = {client_name}
        all_names.update(explicit_aliases)
        
        # Generate deterministic variants
        variants = self._generate_variants(client_name)
        all_names.update(variants)
        
        # Store generated variants for LLM prompt examples and filtering
        self.generated_variants[client_id] = list(variants)
        
        # Compile regex patterns (with word boundaries for safety)
        patterns = []
        for name in all_names:
            if not name:
                continue
            
            # Escape special regex characters
            escaped = re.escape(name)
            # Use alnum-boundaries instead of \b so we still match tokens like:
            #   "Aramark_DC_Upgrade" (underscore is NOT alphanumeric, but IS a \w char)
            # This prevents false negatives that would otherwise cause strict-mode
            # validation failures.
            pattern = re.compile(r'(?<![A-Za-z0-9])' + escaped + r'(?![A-Za-z0-9])', re.IGNORECASE)
            patterns.append(pattern)
        
        # Sort by length (longest first) to avoid partial matches
        patterns.sort(key=lambda p: len(p.pattern), reverse=True)
        
        self.alias_patterns[client_id] = patterns
    
    def _generate_variants(self, name: str) -> List[str]:
        """
        Generate deterministic alias variants from a client name.
        
        Variants include:
        - Original name
        - Normalized versions (case/punctuation/whitespace)
        - Legal suffix stripping (Inc, LLC, Ltd, Corp, etc.)
        - Ampersand/and swap
        - Without common tokens (The, Group, Holdings)
        - No-space version
        
        NOTE: Acronyms and abbreviations (e.g., "AFI", "AmFam") are NOT generated
        programmatically. These are handled by:
        1. Explicit aliases in the CSV (human-curated)
        2. LLM span detection (contextual understanding)
        
        Programmatic acronym generation was removed because cultural/business
        nicknames cannot be reliably derived algorithmically, and short acronyms
        risk false positives (e.g., "MS" matches Morgan Stanley, Microsoft, Mississippi).
        """
        variants = []
        
        # Original name
        variants.append(name)
        
        # Normalized (lowercase, collapsed whitespace)
        normalized = re.sub(r'\s+', ' ', name.lower().strip())
        variants.append(normalized)
        
        # Without legal suffixes
        legal_suffixes = [
            r'\s+Inc\.?$', r'\s+LLC\.?$', r'\s+Ltd\.?$', r'\s+Limited$',
            r'\s+Corp\.?$', r'\s+Corporation$', r'\s+Co\.?$', r'\s+Company$',
            r'\s+PLC\.?$', r'\s+LP\.?$', r'\s+LLP\.?$', r'\s+PC\.?$'
        ]
        for suffix_pattern in legal_suffixes:
            variant = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
            if variant and variant != name:
                variants.append(variant)
        
        # Ampersand/and swap
        if '&' in name:
            variants.append(name.replace('&', 'and'))
        if 'and' in name.lower():
            variants.append(name.replace('and', '&'))
        
        # Without common tokens (The, Group, Holdings)
        tokens_to_drop = ['The', 'Group', 'Holdings']
        for token in tokens_to_drop:
            pattern = r'\b' + re.escape(token) + r'\b\s*'
            variant = re.sub(pattern, '', name, flags=re.IGNORECASE).strip()
            if variant and variant != name:
                variants.append(variant)
        
        # No-space version (for cases like "BankofAmerica")
        no_space = re.sub(r'\s+', '', name)
        if no_space != name:
            variants.append(no_space)
        
        # Deduplicate while preserving order
        seen = set()
        unique_variants = []
        for v in variants:
            v_lower = v.lower()
            if v_lower not in seen:
                seen.add(v_lower)
                unique_variants.append(v)
        
        return unique_variants
    
    def get_client_info(self, salesforce_client_id: str) -> Optional[Dict[str, str]]:
        """Get client information by Salesforce ID"""
        return self.clients.get(salesforce_client_id)
    
    def get_replacement_token(self, salesforce_client_id: str) -> Optional[str]:
        """
        Get the replacement token for a client.
        
        Returns:
            Replacement token like "<<CLIENT: Investment Banking>>" or None if not found
        """
        client_info = self.get_client_info(salesforce_client_id)
        if not client_info:
            return None
        
        industry_label = client_info['industry_label']
        return self.CLIENT_PLACEHOLDER_TEMPLATE.format(industry_label=industry_label)
    
    def get_generated_variants(self, salesforce_client_id: str) -> List[str]:
        """
        Get generated variant aliases for a client (for LLM prompt examples and filtering).
        
        Returns:
            List of generated variant strings (e.g., ["AFI", "AmFam", "AmericanFamilyInsurance"])
        """
        return self.generated_variants.get(salesforce_client_id, [])
    
    def replace_client_names(self, text: str, salesforce_client_id: str) -> Tuple[str, int]:
        """
        Replace all occurrences of client name and aliases with replacement token.
        
        Args:
            text: Text to process
            salesforce_client_id: Salesforce client ID
            
        Returns:
            Tuple of (redacted_text, replacement_count)
        """
        if salesforce_client_id not in self.alias_patterns:
            return text, 0
        
        replacement_token = self.get_replacement_token(salesforce_client_id)
        if not replacement_token:
            return text, 0
        
        patterns = self.alias_patterns[salesforce_client_id]
        redacted_text = text
        replacement_count = 0
        
        # Apply patterns (already sorted longest-first)
        for pattern in patterns:
            matches = list(pattern.finditer(redacted_text))
            if matches:
                # Replace from end to start to preserve offsets
                for match in reversed(matches):
                    redacted_text = (
                        redacted_text[:match.start()] +
                        replacement_token +
                        redacted_text[match.end():]
                    )
                    replacement_count += 1
        
        return redacted_text, replacement_count

