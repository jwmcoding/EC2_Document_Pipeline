# Salesforce Integration Tests

This document describes how to run the integration tests for the `RawSalesforceExportConnector`.

## Test Structure

The test file `test_salesforce_integration.py` contains two test suites:

1. **Unit Tests** (always run): Tests with synthetic fixtures
   - Test basic connector functionality
   - Test CSV parsing logic
   - Test file path resolution with mock data

2. **Integration Tests** (requires actual export): Tests with real export directory
   - Test connector initialization with actual CSV files
   - Test ContentVersion → Deal mapping coverage
   - Test file path resolution with actual directory structure
   - Test document listing and metadata enrichment
   - Test file downloading

## Running Tests

### Run All Tests (Unit + Integration)

```bash
# From project root
pytest tests/test_salesforce_integration.py -v
```

### Run Only Unit Tests (No Export Required)

```bash
pytest tests/test_salesforce_integration.py -v -m "not integration"
```

### Run Only Integration Tests (Requires Export Directory)

```bash
pytest tests/test_salesforce_integration.py -v -m integration
```

## Prerequisites for Integration Tests

The integration tests require the actual export directory:
- **Path**: `/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771`
- **Required Files**:
  - `content_versions.csv`
  - `content_documents.csv`
  - `content_document_links.csv`
  - `deal__cs.csv`
  - `Deal__cs/` directory structure
  - `Attachments/Body/` directory (optional)

If the export directory is not available, integration tests will be skipped automatically.

## Test Output

Integration tests provide detailed output including:
- Number of ContentVersions loaded
- Number of Deal records loaded
- Mapping coverage statistics
- Sample document listings
- File resolution success rates

Example output:
```
✅ Loaded 179,832 ContentVersions
✅ Loaded 687,656 Deal records
✅ Built 45,234 CV→Deal mappings

📊 Mapping Coverage:
   Total ContentVersions: 179,832
   Mapped to Deals: 45,234 (25.1%)
   Unmapped: 134,598
```

## Customizing Export Directory

To test with a different export directory, modify the `ACTUAL_EXPORT_DIR` constant in `test_salesforce_integration.py`:

```python
ACTUAL_EXPORT_DIR = "/path/to/your/export/directory"
```

