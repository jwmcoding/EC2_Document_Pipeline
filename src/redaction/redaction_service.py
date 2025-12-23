"""
Main redaction service that orchestrates all redaction stages

Coordinates regex patterns, client registry, LLM span detection, and validators.
"""

import logging
import re
from typing import Optional, List, Tuple
from .redaction_context import RedactionContext, RedactionResult
from .pii_patterns import PIIPatterns
from .client_registry import ClientRegistry
from .llm_span_detector import LLMSpanDetector
from .validators import RedactionValidators


class RedactionService:
    """Main service for PII redaction"""
    
    # Placeholder tokens
    EMAIL_PLACEHOLDER = "<<EMAIL>>"
    PHONE_PLACEHOLDER = "<<PHONE>>"
    ADDRESS_PLACEHOLDER = "<<ADDRESS>>"
    
    def __init__(
        self,
        client_registry: ClientRegistry,
        llm_span_detector: Optional[LLMSpanDetector] = None,
        strict_mode: bool = True
    ):
        """
        Initialize redaction service.
        
        Args:
            client_registry: Client registry for client name redaction
            llm_span_detector: LLM span detector for PERSON entities (optional)
            strict_mode: If True, fail documents when validation fails
        """
        self.client_registry = client_registry
        self.llm_span_detector = llm_span_detector
        self.strict_mode = strict_mode
        self.validators = RedactionValidators(client_registry)
        self.logger = logging.getLogger(__name__)
    
    def redact(self, text: str, context: RedactionContext) -> RedactionResult:
        """
        Perform complete redaction on text.
        
        Args:
            text: Original text to redact
            context: Redaction context with client/vendor info
            
        Returns:
            RedactionResult with redacted text and statistics
        """
        result = RedactionResult(
            redacted_text=text,
            success=True,
            model_used=None
        )
        
        if not text or len(text.strip()) == 0:
            result.warnings.append("Empty text provided for redaction")
            return result
        
        try:
            # Stage 1: Regex-based PII removal (email, phone, address)
            result.redacted_text, email_count = self._redact_emails(result.redacted_text)
            result.email_replacements = email_count
            
            result.redacted_text, phone_count = self._redact_phones(result.redacted_text)
            result.phone_replacements = phone_count
            
            result.redacted_text, address_count = self._redact_addresses(result.redacted_text)
            result.address_replacements = address_count

            # Stage 2: LLM-based PERSON and ORG detection
            # NOTE: This must run BEFORE deterministic client replacement. Otherwise we can
            # partially replace the client name (e.g., "Denver Health") and leave a tail
            # ("and Hospitals Authority Inc"), preventing the model from seeing the full ORG span.
            
            # Build client-specific replacement token and get client info (if we know the client id)
            replacement_token = None
            client_name = None
            client_aliases = []
            generated_variants = []
            if context.has_client_info():
                replacement_token = self.client_registry.get_replacement_token(
                    context.salesforce_client_id
                )
                client_info = self.client_registry.get_client_info(context.salesforce_client_id)
                if client_info:
                    client_name = client_info.get("client_name")
                    explicit_aliases = client_info.get("aliases", []) or []
                    # Include generated variants for LLM filtering and prompt examples
                    generated_variants = self.client_registry.get_generated_variants(context.salesforce_client_id)
                    client_aliases = explicit_aliases + generated_variants
            
            if self.llm_span_detector:
                try:
                    placeholder_ranges = self._find_placeholder_ranges(result.redacted_text)
                    # Pass client context AND vendor context to LLM for better detection
                    all_spans = self.llm_span_detector.detect_spans(
                        result.redacted_text,
                        client_name=client_name,
                        client_variants=generated_variants,
                        vendor_name=context.vendor_name  # Pass primary vendor from deal metadata
                    )

                    # Filter ORG spans to only match the current client (when configured)
                    org_spans = [
                        (start, end, entity_type, span_text)
                        for start, end, entity_type, span_text in all_spans
                        if entity_type == "ORG"
                    ]
                    if replacement_token and client_name and org_spans:
                        org_spans = self.llm_span_detector.filter_org_spans_for_client(
                            org_spans, client_name, client_aliases
                        )
                    else:
                        # Don't redact ORGs globally; we only redact client ORGs.
                        org_spans = []

                    # PERSON spans always get redacted
                    person_spans = [
                        (start, end, entity_type, span_text)
                        for start, end, entity_type, span_text in all_spans
                        if entity_type == "PERSON"
                    ]

                    # Apply replacements from end->start to preserve offsets and avoid overlap issues
                    replacements = []
                    for start, end, _, span_text in org_spans:
                        if replacement_token:
                            if self._overlaps_any(start, end, placeholder_ranges):
                                continue
                            replacements.append((start, end, replacement_token, "client"))
                    for start, end, _, span_text in person_spans:
                        if self._overlaps_any(start, end, placeholder_ranges):
                            continue
                        replacements.append((start, end, self.llm_span_detector.PERSON_PLACEHOLDER, "person"))
                    replacements.sort(key=lambda x: x[0], reverse=True)

                    redacted_text = result.redacted_text
                    last_start = None
                    for start, end, token, kind in replacements:
                        if start < 0 or end <= start or end > len(redacted_text):
                            continue
                        # Skip overlaps (since earlier replacements shift ranges)
                        if last_start is not None and end > last_start:
                            continue
                        redacted_text = redacted_text[:start] + token + redacted_text[end:]
                        last_start = start
                        if kind == "client":
                            result.client_replacements += 1
                        elif kind == "person":
                            result.person_replacements += 1

                    result.redacted_text = redacted_text
                    result.model_used = self.llm_span_detector.model
                except Exception as e:
                    error_msg = f"LLM span detection failed: {str(e)}"
                    result.errors.append(error_msg)
                    self.logger.error(error_msg)
                    # Important: LLM span detection is an optional enhancement layer.
                    # Even in strict_mode, we still proceed with deterministic client replacement
                    # and strict validators so we don't fail the entire document due to transient
                    # LLM issues (e.g., empty content responses).
            else:
                result.warnings.append("LLM span detector not available - skipping PERSON/ORG redaction")

            # Stage 3: Client name replacement (deterministic, post-LLM)
            # This catches any remaining client aliases (e.g., acronyms like "VSP") that
            # the model didn't tag as ORG, and serves as a backstop for missed spans.
            if context.has_client_info():
                replacement_token = self.client_registry.get_replacement_token(context.salesforce_client_id)
                if replacement_token:
                    result.redacted_text, client_count = self.client_registry.replace_client_names(
                        result.redacted_text,
                        context.salesforce_client_id
                    )
                    result.client_replacements += client_count

                    # If the client was replaced inside a longer legal entity name (common in
                    # "ClientName and Something Inc" patterns), collapse the remaining tail so
                    # reviewers don't see partial legal names post-redaction.
                    result.redacted_text, tail_count = self._collapse_client_placeholder_tails(
                        result.redacted_text,
                        replacement_token,
                    )
                    result.client_replacements += tail_count
                else:
                    result.warnings.append(f"Client ID {context.salesforce_client_id} not found in registry")
            else:
                result.warnings.append("No client information provided - skipping client name redaction")
            
            # Stage 4: Validation (strict mode)
            if self.strict_mode:
                validation_failures = self.validators.validate(
                    result.redacted_text,
                    context.salesforce_client_id,
                    file_type=context.file_type,
                )
                
                if validation_failures:
                    result.validation_passed = False
                    result.validation_failures = validation_failures
                    result.success = False
                    result.errors.extend([f"Validation failed: {f}" for f in validation_failures])
                    self.logger.error(f"Redaction validation failed: {validation_failures}")
            
        except Exception as e:
            error_msg = f"Redaction error: {str(e)}"
            result.errors.append(error_msg)
            result.success = False
            self.logger.error(error_msg)
        
        return result

    def _find_placeholder_ranges(self, text: str) -> List[Tuple[int, int]]:
        """
        Find spans of existing redaction placeholders like <<EMAIL>> or <<CLIENT: ...>>.
        We avoid applying LLM span replacements inside these ranges to prevent
        corrupting placeholder tokens (e.g., "<<ADDRE<<PERSON>>").
        """
        ranges: List[Tuple[int, int]] = []
        if not text:
            return ranges
        for m in re.finditer(r"<<[^>]{1,80}>>", text):
            ranges.append((m.start(), m.end()))
        return ranges

    def _overlaps_any(self, start: int, end: int, ranges: List[Tuple[int, int]]) -> bool:
        """Return True if [start,end) overlaps any of the provided ranges."""
        for rs, re_ in ranges:
            if start < re_ and end > rs:
                return True
        return False

    def _collapse_client_placeholder_tails(self, text: str, replacement_token: str) -> Tuple[str, int]:
        """
        Collapse common legal-entity tails that remain after a partial client replacement.

        Example:
          "<<CLIENT: X>> and Hospitals Authority Inc" -> "<<CLIENT: X>>"

        This is intentionally conservative: it only fires when the text already
        contains the client placeholder token.
        """
        if not text or not replacement_token:
            return text, 0

        # Tail must end with a legal-ish suffix to avoid over-redaction.
        suffixes = (
            "Inc", "Incorporated", "LLC", "Ltd", "Limited", "Corp", "Corporation", "Company", "Authority"
        )
        suffix_re = r"(?:%s)" % "|".join(suffixes)

        # Allow up to 10 TitleCase-ish tokens in the tail.
        tail_words_re = r"(?:[A-Z][A-Za-z0-9&'.-]*\s+){0,10}"

        patterns = [
            re.compile(
                rf"{re.escape(replacement_token)}\s+and\s+{tail_words_re}{suffix_re}\b(?:\s+Inc\b)?",
                flags=re.MULTILINE,
            ),
            re.compile(
                rf"{re.escape(replacement_token)}\s+{tail_words_re}{suffix_re}\b(?:\s+Inc\b)?",
                flags=re.MULTILINE,
            ),
        ]

        replaced = 0
        out = text
        for pat in patterns:
            out, n = pat.subn(replacement_token, out)
            replaced += n

        return out, replaced
    
    def _redact_emails(self, text: str) -> tuple[str, int]:
        """Replace email addresses with placeholder"""
        matches = PIIPatterns.find_emails(text)
        if not matches:
            return text, 0
        
        # Replace from end to start
        redacted = text
        for start, end in reversed(matches):
            redacted = redacted[:start] + self.EMAIL_PLACEHOLDER + redacted[end:]
        
        return redacted, len(matches)
    
    def _redact_phones(self, text: str) -> tuple[str, int]:
        """Replace phone numbers with placeholder"""
        matches = PIIPatterns.find_phones(text)
        if not matches:
            return text, 0
        
        # Replace from end to start
        redacted = text
        for start, end in reversed(matches):
            redacted = redacted[:start] + self.PHONE_PLACEHOLDER + redacted[end:]
        
        return redacted, len(matches)
    
    def _redact_addresses(self, text: str) -> tuple[str, int]:
        """Replace addresses with placeholder"""
        matches = PIIPatterns.find_addresses(text)
        if not matches:
            return text, 0
        
        # Replace from end to start
        redacted = text
        for start, end in reversed(matches):
            redacted = redacted[:start] + self.ADDRESS_PLACEHOLDER + redacted[end:]
        
        return redacted, len(matches)

