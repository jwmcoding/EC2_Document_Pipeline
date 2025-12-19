# PII Redaction Stage - Unit Test Plan

**Created**: December 14, 2025  
**Purpose**: Comprehensive test plan for the PII redaction stage implementation  
**Test File**: `tests/test_redaction_stage.py`

---

## Overview

This test plan covers all components of the PII redaction stage:
- Deterministic PII pattern detection (email, phone, address)
- Client registry loading and alias generation
- Client name redaction (deterministic + LLM catch-all)
- Person name redaction (LLM-based)
- Organization redaction with client filtering
- Strict mode validators
- End-to-end redaction pipeline

---

## Test Categories

### 1. Deterministic Tests (No API Required)
These tests run without `OPENAI_API_KEY` and verify:
- Regex pattern matching
- CSV loading
- Alias generation
- Client name replacement logic
- Validator behavior

### 2. LLM Tests (Requires OPENAI_API_KEY)
These tests require a valid OpenAI API key and verify:
- PERSON entity detection
- ORG entity detection
- Span filtering logic
- End-to-end redaction with LLM

**Marking**: All LLM tests are marked with `@pytest.mark.llm` and will skip gracefully if API key is missing.

---

## Test Cases

### TestPIIPatterns

#### Test: `test_email_detection`
**Purpose**: Verify email regex pattern correctly identifies email addresses  
**Input**: Text with multiple email formats  
**Expected**: Returns correct (start, end) offsets for all emails  
**Assertions**:
- Number of matches equals expected count
- Extracted text matches expected email addresses

#### Test: `test_phone_detection`
**Purpose**: Verify phone regex pattern identifies US phone formats  
**Input**: Text with various phone formats ((555) 123-4567, 555-987-6543, etc.)  
**Expected**: Returns offsets for all phone numbers  
**Assertions**: At least 2 matches found

#### Test: `test_address_detection`
**Purpose**: Verify address regex pattern identifies street addresses  
**Input**: Text with address patterns (123 Main Street, Suite 100)  
**Expected**: Returns offsets for addresses  
**Assertions**: At least 1 match found

#### Test: `test_has_methods`
**Purpose**: Verify convenience `has_*` methods work correctly  
**Input**: Text with and without PII  
**Expected**: Boolean results match presence of PII  
**Assertions**: 
- `has_email()` returns True when email present, False otherwise
- `has_phone()` returns True when phone present, False otherwise

---

### TestClientRegistry

#### Test: `test_load_from_csv`
**Purpose**: Verify CSV loading works with Salesforce export format  
**Input**: Temporary CSV file with Salesforce format (`18 Digit ID`, `Account Name`)  
**Expected**: Client registry loads successfully  
**Assertions**:
- Client info retrieved correctly by Salesforce ID
- Client name matches CSV value
- Industry label set to dummy value "Client Organization"
- Aliases parsed correctly (if present)

**CSV Format Tested**:
```csv
Account Name,Website,Account ID,18 Digit ID
Morgan Stanley,www.morganstanley.com,001XX000003Url7MAC,001XX000003Url7MAAAD
```

#### Test: `test_alias_generation`
**Purpose**: Verify deterministic alias generation creates expected variants  
**Input**: Client name "Morgan Stanley Inc."  
**Expected**: Generates variants including:
- Normalized versions (lowercase, whitespace collapsed)
- Without legal suffix ("Morgan Stanley")
- Acronym ("MS")
- Ampersand/and swaps
- No-space version ("MorganStanley")

**Assertions**:
- Variants list contains expected patterns
- No duplicates
- Original name included

#### Test: `test_client_replacement`
**Purpose**: Verify client name replacement works correctly  
**Input**: Text containing client name and aliases  
**Expected**: All occurrences replaced with `<<CLIENT: {industry_label}>>`  
**Assertions**:
- Replacement count > 0
- Placeholder token appears in redacted text
- Original client name removed from text

#### Test: `test_vendor_not_redacted`
**Purpose**: Verify vendor names are NOT redacted when they're not the client  
**Input**: Text where ServiceNow is the vendor (not client)  
**Expected**: ServiceNow preserved in text  
**Note**: This test validates the logic - in practice, we only redact if the org matches the current document's client

---

### TestRedactionService

#### Test: `test_regex_redaction`
**Purpose**: Verify regex-based PII removal (email, phone)  
**Input**: Text with emails and phone numbers  
**Expected**: PII replaced with placeholders  
**Assertions**:
- `<<EMAIL>>` appears in redacted text
- `<<PHONE>>` appears in redacted text
- Replacement counts > 0
- Original PII removed

#### Test: `test_client_redaction`
**Purpose**: Verify client name redaction via registry  
**Input**: Text with client name, valid RedactionContext  
**Expected**: Client name replaced with industry label placeholder  
**Assertions**:
- `<<CLIENT: {industry_label}>>` appears
- Client name removed
- Replacement count > 0

---

### TestRedactionValidators

#### Test: `test_email_validation_failure`
**Purpose**: Verify validator detects remaining emails after redaction  
**Input**: Text with email address  
**Expected**: Validation failures list includes email detection message  
**Assertions**:
- Failures list not empty
- Failure message mentions "email"

#### Test: `test_client_name_validation_failure`
**Purpose**: Verify validator detects remaining client names  
**Input**: Text with client name, client ID in registry  
**Expected**: Validation failures include client name detection  
**Assertions**:
- Failures list not empty
- Failure message mentions client name or "client"

---

### TestLLMSpanDetection (Requires OPENAI_API_KEY)

#### Test: `test_person_detection`
**Purpose**: Verify LLM detects person names correctly  
**Input**: Text with 2-3 person names  
**Expected**: Returns span offsets for person names  
**Assertions**:
- At least 2 spans returned
- All spans have valid offsets (start >= 0, end > start, end <= text length)
- Entity type is "PERSON"

#### Test: `test_person_replacement`
**Purpose**: Verify PERSON replacements applied correctly  
**Input**: Text with person names, detected spans  
**Expected**: Person names replaced with `<<PERSON>>`  
**Assertions**:
- Replacement count > 0
- `<<PERSON>>` appears in redacted text
- Text outside spans preserved (e.g., "attended", "meeting")

#### Test: `test_no_person_in_text`
**Purpose**: Verify LLM handles text with no person names gracefully  
**Input**: Text about company earnings (no person names)  
**Expected**: Returns empty or minimal spans  
**Assertions**: Span count <= 1 (allowing for false positives)

#### Test: `test_org_detection`
**Purpose**: Verify LLM detects ORG entities  
**Input**: Text with organization names  
**Expected**: Returns ORG entity spans  
**Assertions**:
- At least 1 ORG span found
- Spans have valid offsets
- Entity type is "ORG"
- Span text field populated

#### Test: `test_org_filtering_for_client`
**Purpose**: Verify ORG filtering only matches client names  
**Input**: Text with client name and vendor name  
**Expected**: Only client-matching ORG spans returned  
**Assertions**:
- Filtered spans contain client references
- Vendor names NOT in filtered spans
- Matching logic works correctly

---

### TestEndToEndRedaction (Requires OPENAI_API_KEY)

#### Test: `test_complete_redaction`
**Purpose**: Verify complete redaction pipeline end-to-end  
**Input**: Text with all PII types (client name, person names, email, phone, address)  
**Expected**: All PII redacted, validation passes  
**Assertions**:
- All placeholder tokens present (`<<CLIENT: ...>>`, `<<EMAIL>>`, `<<PHONE>>`, `<<PERSON>>`)
- Original PII removed
- Validation passes (`validation_passed = True`)
- Success = True
- Replacement counts > 0 for each type

---

## Test Data Requirements

### CSV Test Files

**Salesforce Format CSV** (for `test_load_from_csv`):
```csv
Account Name,Website,Account ID,18 Digit ID
Morgan Stanley,www.morganstanley.com,001XX000003Url7MAC,001XX000003Url7MAAAD
ServiceNow,www.servicenow.com,001XX000004Abc8DEF,001XX000004Abc8DEFAAX
```

**Standard Format CSV** (for future SIC code support):
```csv
salesforce_client_id,client_name,industry_label,aliases
001XX000003Url7MAC,Morgan Stanley,Investment Banking,"MS|MorganStanley"
```

### Sample Test Text

**Person Detection**:
```
John Smith and Jane Doe met with the team. Contact Mary Johnson for details.
```

**ORG Detection**:
```
We met with Morgan Stanley and ServiceNow representatives. Project Falcon is underway.
```

**Complete Redaction**:
```
Dear John Smith,

We met with Morgan Stanley (MS) representatives including Jane Doe.
Please contact us at john@example.com or call 555-123-4567.
Our office is at 123 Main Street, Suite 100.
```

---

## Running Tests

### Run All Tests
```bash
pytest tests/test_redaction_stage.py -v
```

### Run Only Deterministic Tests (No API Key Required)
```bash
pytest tests/test_redaction_stage.py -v -m "not llm"
```

### Run Only LLM Tests (Requires OPENAI_API_KEY)
```bash
pytest tests/test_redaction_stage.py -v -m llm
```

### Run Specific Test Class
```bash
pytest tests/test_redaction_stage.py::TestPIIPatterns -v
pytest tests/test_redaction_stage.py::TestClientRegistry -v
pytest tests/test_redaction_stage.py::TestLLMSpanDetection -v
```

### Run with Coverage
```bash
pytest tests/test_redaction_stage.py --cov=src/redaction --cov-report=html
```

---

## Expected Test Results

### Deterministic Tests
- **All should pass** without API key
- Fast execution (< 1 second total)
- No external dependencies

### LLM Tests
- **May skip** if `OPENAI_API_KEY` not set (graceful skip message)
- **Require API key** to run
- Slower execution (~2-5 seconds per test due to API calls)
- **Cost**: ~$0.001-0.01 per test run (GPT-5 mini pricing)

---

## Test Coverage Goals

### Component Coverage
- ✅ `PIIPatterns`: All regex patterns tested
- ✅ `ClientRegistry`: CSV loading, alias generation, replacement logic
- ✅ `RedactionService`: Complete redaction pipeline
- ✅ `RedactionValidators`: All validation checks
- ✅ `LLMSpanDetector`: PERSON and ORG detection, filtering

### Edge Cases Covered
- ✅ Empty text handling
- ✅ Missing CSV columns
- ✅ Invalid span offsets
- ✅ Overlapping spans
- ✅ Long documents (windowing)
- ✅ No PII in text
- ✅ Vendor/competitor protection

---

## Known Limitations / Future Tests

### Not Yet Tested (Out of Scope for Unit Tests)
- **End-to-end pipeline integration**: Full document processing with redaction
- **Performance testing**: Large document processing times
- **Cost analysis**: LLM API costs at scale
- **False positive/negative rates**: LLM accuracy metrics
- **Concurrent processing**: Parallel worker redaction behavior

### Future Test Additions
- **SIC code integration**: Test with real industry labels (when available)
- **Alias CSV column**: Test explicit alias loading from CSV
- **Error recovery**: Test behavior when LLM API fails
- **Strict mode toggle**: Test permissive vs strict mode differences

---

## Troubleshooting

### Tests Skip with "OPENAI_API_KEY not set"
**Solution**: Set environment variable:
```bash
export OPENAI_API_KEY=your_key_here
pytest tests/test_redaction_stage.py -v -m llm
```

### CSV Loading Fails
**Check**:
- CSV file path is correct
- CSV has required columns (`18 Digit ID`, `Account Name` for Salesforce format)
- CSV encoding is UTF-8

### LLM Tests Fail
**Check**:
- API key is valid
- Model name is correct (`gpt-5-mini-2025-08-07`)
- Network connectivity
- API rate limits

### Span Offset Errors
**Check**:
- Text length matches expected
- Offsets are within text bounds
- Window overlap logic working correctly

---

## Test Maintenance

### When Adding New Features
1. Add test cases to appropriate test class
2. Update this document with new test descriptions
3. Ensure both deterministic and LLM tests cover new functionality
4. Update coverage goals if new components added

### When CSV Format Changes
1. Update `test_load_from_csv` to handle new format
2. Update CSV format documentation in this plan
3. Test backward compatibility if needed

---

## Success Criteria

### All Tests Passing
- ✅ Deterministic tests: 100% pass rate
- ✅ LLM tests: 100% pass rate (when API key available)
- ✅ No linting errors
- ✅ Coverage > 80% for redaction module

### Test Quality
- ✅ Tests are isolated (no shared state)
- ✅ Tests use temporary files (no cleanup needed)
- ✅ Tests have clear assertions
- ✅ Tests document expected behavior

---

**Last Updated**: December 14, 2025  
**Test File Location**: `tests/test_redaction_stage.py`  
**Related Documentation**: `src/redaction/` module docstrings

