# Metadata Test - Quick Start (2 minutes)

## Step 1: Set Environment Variables
```bash
export PINECONE_API_KEY="your-pinecone-api-key"
export PINECONE_INDEX_NAME="business-documents"
export OPENAI_API_KEY="your-openai-key"
```

## Step 2: Run Tests

### Option A: Quick Validation (No Pinecone Required)
```bash
# Tests schema, field count, and types
pytest tests/test_metadata_simplification.py::test_metadata_schema_validation -v -s
pytest tests/test_metadata_simplification.py::test_metadata_field_count -v -s
pytest tests/test_metadata_simplification.py::test_metadata_field_types -v -s
```

### Option B: Full Test with Pinecone (5-10 minutes)
```bash
# Runs all tests including Pinecone upsert
pytest tests/test_metadata_simplification.py -v -s
```

### Option C: Just Check Upsert
```bash
# Tests upserting 10 documents to Pinecone
pytest tests/test_metadata_simplification.py::test_pinecone_upsert -v -s
```

## Step 3: Verify Results

✅ **All tests pass** = Schema is valid and working
- 10 random documents processed
- All 26 fields validated
- ~60-70 chunks upserted to `metadata_simplification_test` namespace
- No errors

## What Gets Tested

| Test | What | Time |
|------|------|------|
| `test_metadata_schema_validation` | Field population | <1s |
| `test_metadata_field_count` | Field completeness | <1s |
| `test_metadata_field_types` | Data type correctness | <1s |
| `test_pinecone_upsert` | Pinecone integration | 3-5m |

## Sample Output
```
✅ Document 1: contract_summary.pdf → 8 chunks
✅ Document 2: meeting_notes.docx → 6 chunks
✅ Document 3: proposal.xlsx → 5 chunks
...
📈 Summary:
  Documents processed: 10
  Total chunks upserted: 67
  Test namespace: metadata_simplification_test
```

## Cleanup (Optional)
```bash
# After testing, delete test namespace to save storage
# (Pinecone console or CLI)
```

## Troubleshooting

**Test Skipped?**
```bash
# Make sure files exist:
ls enhanced_salesforce_resume_documents.json
# Make sure env vars set:
echo $PINECONE_API_KEY
```

**No Data in Pinecone?**
- Check API key is correct
- Check index name exists
- Check namespace "metadata_simplification_test" created

**Metadata Too Large?**
- Shouldn't happen with 26-field schema (2-3KB per chunk)
- Check for large text fields (truncate to limits in test)

## Next: Migration Plan

Once all tests pass, see `METADATA_SIMPLIFICATION_PLAN.md` for:
- Creating production namespace
- Reprocessing all documents
- Performance benchmarks

