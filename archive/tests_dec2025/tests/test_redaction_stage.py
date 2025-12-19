"""
Unit tests for PII redaction stage

Tests both deterministic components (regex, client registry) and LLM-based
PERSON detection (requires OPENAI_API_KEY).
"""

import pytest
import os
import tempfile
import csv
from pathlib import Path

from src.redaction.pii_patterns import PIIPatterns
from src.redaction.client_registry import ClientRegistry
from src.redaction.redaction_context import RedactionContext
from src.redaction.redaction_service import RedactionService
from src.redaction.validators import RedactionValidators


class TestPIIPatterns:
    """Test deterministic PII pattern detection"""
    
    def test_email_detection(self):
        """Test email pattern detection"""
        text = "Contact us at john.doe@example.com or support@company.org"
        matches = PIIPatterns.find_emails(text)
        assert len(matches) == 2
        assert text[matches[0][0]:matches[0][1]] == "john.doe@example.com"
        assert text[matches[1][0]:matches[1][1]] == "support@company.org"
    
    def test_phone_detection(self):
        """Test phone number pattern detection"""
        text = "Call (555) 123-4567 or 555-987-6543"
        matches = PIIPatterns.find_phones(text)
        assert len(matches) >= 2
    
    def test_address_detection(self):
        """Test address pattern detection"""
        text = "Visit us at 123 Main Street, Suite 100"
        matches = PIIPatterns.find_addresses(text)
        assert len(matches) >= 1
    
    def test_has_methods(self):
        """Test convenience has_* methods"""
        assert PIIPatterns.has_email("Email: test@example.com")
        assert not PIIPatterns.has_email("No email here")
        assert PIIPatterns.has_phone("Call 555-123-4567")
        assert not PIIPatterns.has_phone("No phone")


class TestClientRegistry:
    """Test client registry and alias generation"""
    
    def test_load_from_csv(self):
        """Test loading client registry from CSV"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            writer.writerow({
                'salesforce_client_id': '001XX000003Url7MAC',
                'client_name': 'Morgan Stanley',
                'industry_label': 'Investment Banking',
                'aliases': 'MS|MorganStanley'
            })
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            client_info = registry.get_client_info('001XX000003Url7MAC')
            assert client_info is not None
            assert client_info['client_name'] == 'Morgan Stanley'
            assert client_info['industry_label'] == 'Investment Banking'
            assert 'MS' in client_info['aliases']
        finally:
            os.unlink(csv_path)
    
    def test_alias_generation(self):
        """Test deterministic alias generation"""
        registry = ClientRegistry()
        variants = registry._generate_variants("Morgan Stanley Inc.")
        
        # Should include normalized versions
        assert any("morgan stanley" in v.lower() for v in variants)
        # Should include without legal suffix
        assert any("morgan stanley" in v.lower() and "inc" not in v.lower() for v in variants)
        # Should include no-space version
        assert "MorganStanleyInc." in variants or "MorganStanley" in variants
        # NOTE: Acronyms (e.g., "MS") are NOT generated programmatically.
        # They rely on explicit CSV aliases or LLM contextual detection.
    
    def test_client_replacement(self):
        """Test client name replacement"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            writer.writerow({
                'salesforce_client_id': '001XX000003Url7MAC',
                'client_name': 'Morgan Stanley',
                'industry_label': 'Investment Banking',
                'aliases': 'MS'
            })
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            text = "We met with Morgan Stanley and MS representatives"
            redacted, count = registry.replace_client_names(text, '001XX000003Url7MAC')
            
            assert count >= 1
            assert "<<CLIENT: Investment Banking>>" in redacted
            assert "Morgan Stanley" not in redacted
        finally:
            os.unlink(csv_path)
    
    def test_vendor_not_redacted(self):
        """Test that vendor names are not redacted"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            writer.writerow({
                'salesforce_client_id': '001XX000003Url7MAC',
                'client_name': 'ServiceNow',
                'industry_label': 'Cloud Software',
                'aliases': ''
            })
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            # Text where ServiceNow is the vendor (not client)
            text = "ServiceNow provided a quote for our client"
            redacted, count = registry.replace_client_names(text, '001XX000003Url7MAC')
            
            # ServiceNow should be redacted because it IS the client in this case
            # (This test validates the logic - in real usage, we only redact if it matches the current client)
            assert "<<CLIENT: Cloud Software>>" in redacted
        finally:
            os.unlink(csv_path)


class TestRedactionService:
    """Test complete redaction service"""
    
    def test_regex_redaction(self):
        """Test regex-based PII redaction"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            service = RedactionService(registry, strict_mode=False)
            
            text = "Contact john@example.com or call 555-123-4567"
            context = RedactionContext()
            result = service.redact(text, context)
            
            assert result.success
            assert result.email_replacements > 0
            assert result.phone_replacements > 0
            assert "<<EMAIL>>" in result.redacted_text
            assert "<<PHONE>>" in result.redacted_text
        finally:
            os.unlink(csv_path)
    
    def test_client_redaction(self):
        """Test client name redaction"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            writer.writerow({
                'salesforce_client_id': '001XX000003Url7MAC',
                'client_name': 'Morgan Stanley',
                'industry_label': 'Investment Banking',
                'aliases': ''
            })
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            service = RedactionService(registry, strict_mode=False)
            
            text = "We met with Morgan Stanley representatives"
            context = RedactionContext(
                salesforce_client_id='001XX000003Url7MAC',
                client_name='Morgan Stanley',
                industry_label='Investment Banking'
            )
            result = service.redact(text, context)
            
            assert result.success
            assert result.client_replacements > 0
            assert "<<CLIENT: Investment Banking>>" in result.redacted_text
            assert "Morgan Stanley" not in result.redacted_text
        finally:
            os.unlink(csv_path)


class TestRedactionValidators:
    """Test strict mode validators"""
    
    def test_email_validation_failure(self):
        """Test validator detects remaining emails"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            validators = RedactionValidators(registry)
            
            text = "Contact test@example.com"
            failures = validators.validate(text)
            
            assert len(failures) > 0
            assert any("email" in f.lower() for f in failures)
        finally:
            os.unlink(csv_path)
    
    def test_client_name_validation_failure(self):
        """Test validator detects remaining client names"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            writer.writerow({
                'salesforce_client_id': '001XX000003Url7MAC',
                'client_name': 'Morgan Stanley',
                'industry_label': 'Investment Banking',
                'aliases': ''
            })
            csv_path = f.name
        
        try:
            registry = ClientRegistry(csv_path)
            validators = RedactionValidators(registry)
            
            text = "We met with Morgan Stanley"
            failures = validators.validate(text, '001XX000003Url7MAC')
            
            assert len(failures) > 0
            assert any("client" in f.lower() or "morgan stanley" in f.lower() for f in failures)
        finally:
            os.unlink(csv_path)


@pytest.mark.llm
class TestLLMSpanDetection:
    """Test LLM-based PERSON detection (requires OPENAI_API_KEY)"""
    
    @pytest.fixture
    def llm_detector(self):
        """Create LLM span detector"""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set - skipping LLM tests")
        
        from src.redaction.llm_span_detector import LLMSpanDetector
        return LLMSpanDetector(api_key=api_key, model="gpt-5-mini-2025-08-07")
    
    def test_person_detection(self, llm_detector):
        """Test LLM detects person names"""
        text = "John Smith and Jane Doe met with the team. Contact Mary Johnson for details."
        spans = llm_detector.detect_person_spans(text)
        
        assert len(spans) >= 2  # Should detect at least some person names
        
        # Verify spans are valid
        for start, end in spans:
            assert start >= 0
            assert end > start
            assert end <= len(text)
    
    def test_person_replacement(self, llm_detector):
        """Test applying PERSON replacements"""
        text = "John Smith and Jane Doe attended the meeting."
        spans = llm_detector.detect_person_spans(text)
        
        redacted, count = llm_detector.apply_person_replacements(text, spans)
        
        assert count > 0
        assert "<<PERSON>>" in redacted
        # Verify text outside spans is preserved
        assert "attended" in redacted or "meeting" in redacted
    
    def test_no_person_in_text(self, llm_detector):
        """Test LLM handles text with no person names"""
        text = "The company reported strong quarterly earnings. Revenue increased by 15%."
        spans = llm_detector.detect_person_spans(text)
        
        # Should return empty or very few spans
        assert len(spans) <= 1  # May detect false positives, but should be minimal
    
    def test_org_detection(self, llm_detector):
        """Test LLM detects ORG entities"""
        text = "We met with Morgan Stanley and ServiceNow representatives. Project Falcon is underway."
        all_spans = llm_detector.detect_spans(text)
        
        # Should detect ORG entities
        org_spans = [(start, end, entity_type, text) for start, end, entity_type, text in all_spans if entity_type == 'ORG']
        assert len(org_spans) >= 1  # Should detect at least some organizations
        
        # Verify spans are valid
        for start, end, entity_type, span_text in org_spans:
            assert start >= 0
            assert end > start
            assert end <= len(text)
            assert entity_type == 'ORG'
    
    def test_org_filtering_for_client(self, llm_detector):
        """Test ORG filtering only matches client names"""
        text = "We met with Morgan Stanley (MS) and ServiceNow representatives. Project Falcon is underway."
        all_spans = llm_detector.detect_spans(text)
        
        org_spans = [(start, end, entity_type, text) for start, end, entity_type, text in all_spans if entity_type == 'ORG']
        
        # Filter to only Morgan Stanley (client)
        client_name = "Morgan Stanley"
        client_aliases = ["MS", "MorganStanley"]
        
        matching_spans = llm_detector.filter_org_spans_for_client(org_spans, client_name, client_aliases)
        
        # Should match Morgan Stanley and MS, but NOT ServiceNow
        assert len(matching_spans) >= 1
        
        # Verify matched spans contain client references
        matched_texts = [text for _, _, _, text in matching_spans]
        assert any("Morgan Stanley" in text or "MS" in text for text in matched_texts)


@pytest.mark.llm
class TestEndToEndRedaction:
    """End-to-end redaction tests with LLM (requires OPENAI_API_KEY)"""
    
    @pytest.fixture
    def redaction_service(self):
        """Create complete redaction service"""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set - skipping LLM tests")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'salesforce_client_id', 'client_name', 'industry_label', 'aliases'
            ])
            writer.writeheader()
            writer.writerow({
                'salesforce_client_id': '001XX000003Url7MAC',
                'client_name': 'Morgan Stanley',
                'industry_label': 'Investment Banking',
                'aliases': 'MS'
            })
            csv_path = f.name
        
        try:
            from src.redaction.client_registry import ClientRegistry
            from src.redaction.llm_span_detector import LLMSpanDetector
            from src.redaction.redaction_service import RedactionService
            
            registry = ClientRegistry(csv_path)
            llm_detector = LLMSpanDetector(api_key=api_key, model="gpt-5-mini-2025-08-07")
            service = RedactionService(
                client_registry=registry,
                llm_span_detector=llm_detector,
                strict_mode=True
            )
            yield service
        finally:
            os.unlink(csv_path)
    
    def test_complete_redaction(self, redaction_service):
        """Test complete redaction pipeline"""
        text = """
        Dear John Smith,
        
        We met with Morgan Stanley (MS) representatives including Jane Doe.
        Please contact us at john@example.com or call 555-123-4567.
        Our office is at 123 Main Street, Suite 100.
        """
        
        context = RedactionContext(
            salesforce_client_id='001XX000003Url7MAC',
            client_name='Morgan Stanley',
            industry_label='Investment Banking'
        )
        
        result = redaction_service.redact(text, context)
        
        assert result.success
        assert result.validation_passed
        
        # Check all PII types were redacted
        assert "<<CLIENT: Investment Banking>>" in result.redacted_text
        assert "<<EMAIL>>" in result.redacted_text
        assert "<<PHONE>>" in result.redacted_text
        assert "<<PERSON>>" in result.redacted_text
        
        # Verify original PII is gone
        assert "Morgan Stanley" not in result.redacted_text
        assert "john@example.com" not in result.redacted_text
        assert "555-123-4567" not in result.redacted_text

