# Quick-Win Pre-Production Tests

Four critical tests to validate pipeline readiness before processing 143,000 documents.

## Test Files Created

1. **`test_text_field_accessibility.py`** - Verifies text is accessible at top level
2. **`test_metadata_size_limits.py`** - Ensures no vectors exceed 40KB metadata limit
3. **`test_resume_capability.py`** - Tests interrupt/resume functionality
4. **`test_performance_benchmark.py`** - Measures processing time and cost estimates

## Prerequisites

- Pinecone API key: `PINECONE_API_KEY`
- OpenAI API key: `OPENAI_API_KEY` (for embeddings)
- Test namespace: `benchmark-250docs-2025-12-11` (for Tests 1 & 2)
- Test discovery files: `test_resume_discovery.json` and `test_perf_discovery.json` (for Tests 3 & 4)

## Running Tests

### Test 1: Text Field Accessibility (2 hours)

```bash
# Run all text accessibility tests
pytest tests/test_text_field_accessibility.py -v -s

# Run specific test
pytest tests/test_text_field_accessibility.py::test_text_accessible_at_top_level -v -s
```

**What it tests:**
- Text accessible via `result.text` (not None)
- Text NOT in metadata dictionary
- Search queries return text correctly
- Filter-only queries return text

**Success criteria:**
- 100% of sampled vectors have accessible text
- 0% have text in metadata
- All search queries return text

---

### Test 2: Metadata Size Validation (1 hour)

```bash
# Run metadata size tests
pytest tests/test_metadata_size_limits.py -v -s

# Or use enhanced check script
python check_test_progress.py benchmark-250docs-2025-12-11
```

**What it tests:**
- All vectors have metadata < 40KB limit
- Filename truncation working (< 200 chars)
- Metadata size statistics

**Success criteria:**
- 100% of vectors under 40KB limit
- All filenames < 200 chars

---

### Test 3: Resume Capability (1 hour)

```bash
# Run resume capability test
pytest tests/test_resume_capability.py -v -s
```

**What it tests:**
- Processing can be interrupted
- Progress file created correctly
- Resume processes remaining documents
- No duplicate vectors

**Requirements:**
- Test discovery file: `test_resume_discovery.json` with 100 documents

**Success criteria:**
- Progress file created on interrupt
- Resume processes remaining docs
- Zero duplicate vectors

---

### Test 4: Performance Benchmark (4 hours)

```bash
# Run performance benchmark
pytest tests/test_performance_benchmark.py -v -s
```

**What it tests:**
- Processing time per document
- Memory usage
- Optimal worker count (tests 4, 6, 8 workers)
- Cost estimates for 143K documents

**Requirements:**
- Test discovery file: `test_perf_discovery.json` with 1,000 documents

**Success criteria:**
- Processing time < 5 seconds/document
- Memory stable (no leaks)
- Cost estimate calculated

---

## Quick Test Execution Order

1. **Tests 1 & 2** (can run immediately, use existing namespace):
   ```bash
   pytest tests/test_text_field_accessibility.py tests/test_metadata_size_limits.py -v -s
   ```

2. **Test 3** (requires test discovery file):
   ```bash
   pytest tests/test_resume_capability.py -v -s
   ```

3. **Test 4** (requires test discovery file, longest test):
   ```bash
   pytest tests/test_performance_benchmark.py -v -s
   ```

## Expected Output

### Test 1 Output Example
```
🔍 Testing text accessibility at top level...
   Namespace: benchmark-250docs-2025-12-11
   Sample size: 50
   Found 50 vectors
📊 Results:
   Text accessible: 50/50 (100.0%)
   Empty text: 0/50
✅ PASSED
```

### Test 2 Output Example
```
🔍 Testing metadata size limits...
📊 Metadata Size Statistics:
   Average: 2,345 bytes (2.29 KB)
   Max: 3,456 bytes (3.37 KB)
   Limit: 40,960 bytes (40.00 KB)
✅ All vectors under limit!
```

### Test 3 Output Example
```
🔍 Testing resume capability...
📋 Step 1: Clearing test namespace...
📋 Step 2: Starting processing...
📋 Step 5: Resuming processing...
📊 Duplicate Check:
   Sample checked: 100
   Duplicates found: 0
✅ Resume capability test passed!
```

### Test 4 Output Example
```
PERFORMANCE BENCHMARK SUMMARY
Workers    Time/Doc     Vectors/Sec     Peak Memory    
------------------------------------------------------------
4          3.45         12.3            2048          
6          2.89         15.6            2560          
8          2.67         18.2            3072          

📈 Estimates for 143,000 documents:
   Using 8 workers (best performance):
   Estimated time: 106.2 hours (4.4 days)
```

## Troubleshooting

### Test Skipped: No Vectors Found
```
SKIPPED - No vectors found in namespace benchmark-250docs-2025-12-11
```
**Solution**: Run the 250-file benchmark first to populate the namespace.

### Test Skipped: Discovery File Not Found
```
SKIPPED - Test discovery file not found: test_resume_discovery.json
```
**Solution**: Create test discovery files using `discover_documents.py`:
```bash
python discover_documents.py --source salesforce_raw \
  --export-root-dir /path/to/export \
  --max-docs 100 \
  --output test_resume_discovery.json
```

### Import Errors
```
ModuleNotFoundError: No module named 'psutil'
```
**Solution**: Install missing dependencies:
```bash
pip install psutil pytest
```

## Next Steps After Tests Pass

1. ✅ All tests pass → Proceed with production run
2. ⚠️ Test failures → Fix issues before production
3. 📊 Review performance metrics → Optimize worker count if needed
4. 💰 Review cost estimates → Get approval if needed

## Files Modified/Created

- ✅ `tests/test_text_field_accessibility.py` (new)
- ✅ `tests/test_metadata_size_limits.py` (new)
- ✅ `tests/test_resume_capability.py` (new)
- ✅ `tests/test_performance_benchmark.py` (new)
- ✅ `check_test_progress.py` (enhanced with metadata size check)

