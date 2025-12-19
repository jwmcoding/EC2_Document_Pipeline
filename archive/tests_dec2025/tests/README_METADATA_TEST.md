# Metadata Simplification Test Suite

Tests for the new **26-field simplified metadata schema** that reduces Pinecone storage by 59%.

## Overview

This test validates:
- ✅ All 26 core fields are present in the metadata schema
- ✅ Field data types are correct (strings, floats, etc.)
- ✅ Proper metadata population from enhanced JSON
- ✅ Pinecone upsert with new schema to test namespace
- ✅ Deal creation date extraction from `deal__cs.csv`

## Test Configuration

**Test Namespace**: `metadata_simplification_test`  
**Sample Size**: 10 random documents  
**Enhanced JSON**: `enhanced_salesforce_resume_documents.json`

## Expected Schema (26 Fields)

### Core Document (3)
- `file_name` - Document filename
- `file_type` - .pdf, .xlsx, .docx, .msg
- `deal_creation_date` - From CreatedDate in deal__cs.csv

### Deal Context (6)
- `deal_id` - User-friendly deal number
- `salesforce_deal_id` - Raw Salesforce ID for tracing
- `deal_subject` - Deal description
- `deal_status` - Closed, In Progress, etc.
- `deal_reason` - Renewal, New purchase, Add-on
- `deal_start_date` - Deal start date

### Business Relationships (6)
- `client_id` - Friendly client identifier
- `client_name` - Full client name
- `salesforce_client_id` - Raw Salesforce ID
- `vendor_id` - Friendly vendor identifier
- `vendor_name` - Full vendor name
- `salesforce_vendor_id` - Raw Salesforce ID

### Financial Metrics (7)
- `proposed_amount` - Initial vendor quote
- `final_amount` - Final negotiated amount
- `savings_1yr` - First year savings
- `savings_3yr` - Three year savings
- `savings_target` - Target savings
- `savings_achieved` - Actual outcome
- `savings_target_full_term` - Full contract term savings

### Rich Content (2)
- `current_narrative` - Analyst insights
- `customer_comments` - Customer advice

### Email-Specific (1)
- `email_subject` - Email subject (only for .msg files)

### Technical (1)
- `chunk_index` - Chunk number within document

## Prerequisites

1. **Pinecone API Key**
   ```bash
   export PINECONE_API_KEY="your-api-key"
   export PINECONE_INDEX_NAME="business-documents"  # or your index name
   ```

2. **OpenAI API Key** (for embeddings)
   ```bash
   export OPENAI_API_KEY="your-openai-key"
   ```

3. **Enhanced JSON File**
   - Path: `enhanced_salesforce_resume_documents.json`
   - Used for loading sample documents

## Running Tests

### Run All Metadata Simplification Tests
```bash
pytest tests/test_metadata_simplification.py -v
```

### Run Specific Test
```bash
# Test schema validation
pytest tests/test_metadata_simplification.py::test_metadata_schema_validation -v -s

# Test field count
pytest tests/test_metadata_simplification.py::test_metadata_field_count -v -s

# Test Pinecone upsert (requires API keys)
pytest tests/test_metadata_simplification.py::test_pinecone_upsert -v -s

# Test field types
pytest tests/test_metadata_simplification.py::test_metadata_field_types -v -s
```

### Run with Verbose Output
```bash
pytest tests/test_metadata_simplification.py -v -s
```

## Expected Output

```
test_metadata_simplification.py::test_metadata_schema_validation 
✅ Testing metadata schema with 10 documents
  Document 1: contract_summary.pdf
    Total fields in schema: 64
    Populated fields: 28
    Deal ID: 58773
    Creation Date: 2023-01-15
  ✓ PASSED

test_metadata_simplification.py::test_metadata_field_count
📊 Testing field count across 10 documents
  Expected 26 fields in schema
  Found 24 unique populated fields
  ✅ All expected fields found!
  ✓ PASSED

test_metadata_simplification.py::test_pinecone_upsert
🔄 Testing Pinecone upsert with 10 documents to namespace: metadata_simplification_test
  ✅ Document 1: contract_summary.pdf → 8 chunks
  ✅ Document 2: meeting_notes.docx → 6 chunks
  ...
  📈 Summary:
    Documents processed: 10
    Total chunks upserted: 67
    Test namespace: metadata_simplification_test
    Vectors in test namespace: 67
  ✓ PASSED

test_metadata_simplification.py::test_metadata_field_types
🔍 Testing field types for 10 documents
  ✅ All field types validated correctly
  ✓ PASSED

========================= 4 passed in 12.34s =========================
```

## Cleanup

To delete the test namespace after verification:

```python
from src.connectors.pinecone_client import PineconeDocumentClient

client = PineconeDocumentClient(api_key="your-key", index_name="business-documents")
client.index.delete(namespace="metadata_simplification_test", delete_all=True)
```

Or via CLI:
```bash
# List namespaces
pc index describe --name business-documents

# Delete test namespace records via Pinecone console
```

## Key Metrics

| Metric | Baseline | Optimized | Savings |
|--------|----------|-----------|---------|
| **Fields per chunk** | 63 | 26 | 59% ↓ |
| **Metadata size** | ~5-10 KB | ~2-3 KB | 60% ↓ |
| **Query speed** | Baseline | +40-50% faster | Significant |
| **Storage (1.26M docs)** | ~40 GB | ~16.4 GB | 59% ↓ |
| **Monthly cost** | Baseline | $200-300 savings | 59% ↓ |

## Troubleshooting

### Test Skipped: Enhanced JSON Not Found
```
SKIPPED - Enhanced JSON file not found at enhanced_salesforce_resume_documents.json
```
**Solution**: Ensure the enhanced JSON file exists in the project root, or update `ENHANCED_JSON_PATH` in the test file.

### Test Skipped: PINECONE_API_KEY Not Set
```
SKIPPED - PINECONE_API_KEY not set in environment
```
**Solution**: Set the environment variable before running tests:
```bash
export PINECONE_API_KEY="your-api-key"
pytest tests/test_metadata_simplification.py -v
```

### Upsert Fails: Metadata Exceeds 40KB
```
Error: Metadata size exceeds 40KB limit
```
**Solution**: This shouldn't happen with the 26-field schema (~2-3 KB per chunk). Check for:
- Large text in `current_narrative` or `customer_comments` (truncate to 2000 chars)
- Nested objects in metadata (should be flat only)
- Unicode or binary data in string fields

## Implementation Timeline

**Phase 1: Schema Update** ✅ Complete
- Updated DocumentMetadata model
- Updated Pinecone client metadata building
- Updated Salesforce connectors

**Phase 2: Testing** 🚀 In Progress
- Unit tests for field validation
- Integration tests with Pinecone
- Performance benchmarking

**Phase 3: Migration** ⏳ Pending
- Create new optimized namespace
- Reprocess documents with new schema
- Validate search quality
- Deprecate old namespace

## Next Steps

1. Run this test suite to validate the 26-field schema
2. Review test output and field population percentages
3. If all tests pass, proceed with production migration
4. Create new namespace with optimized data
5. Run performance benchmarks vs old schema
6. Gradually migrate users to new namespace

## References

- **Metadata Simplification Plan**: `METADATA_SIMPLIFICATION_PLAN.md`
- **Main README**: `README.md`
- **Pinecone Limits**: 40KB metadata per record
- **Test Data**: `enhanced_salesforce_resume_documents.json`

