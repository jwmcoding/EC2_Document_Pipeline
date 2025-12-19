# Production Cleanup Plan - December 2025

**Purpose**: Archive test files, development scripts, and one-off utilities that are not needed for production operations.

**Date**: December 18, 2025

---

## Production Scripts (KEEP - Root Level)

These are core production entrypoints:

- `discover_documents.py` - Document discovery (production)
- `process_discovered_documents.py` - Document processing (production)
- `batch_processing_updater.py` - Batch job monitoring (production)
- `batch_results_checker.py` - Batch results retrieval (production)

---

## Scripts Directory - Keep Utilities, Archive Testing Tools

### Keep (Production Utilities)
- `scripts/audit_metadata_quality.py` - Metadata quality auditing
- `scripts/backfill_date_timestamps.py` - Date timestamp backfill utility
- `scripts/monitor_progress.py` - Progress monitoring utility
- `scripts/utilities/update_vendor_client_names.py` - Vendor/client name updates

### Archive (Testing/Development Tools)
- `scripts/redaction_harness_local.py` → `archive/scripts/redaction_harness_local.py`
  - **Reason**: Testing tool for redaction review artifacts, not production
- `scripts/select_table_test_documents.py` → `archive/scripts/select_table_test_documents.py`
  - **Reason**: Test document selection utility
- `scripts/compare_pinecone_targets/` → `archive/scripts/compare_pinecone_targets/`
  - **Reason**: Comparison tool for parser evaluation, not production workflow

---

## Root-Level Test Files (ARCHIVE)

Move to `archive/test_files_dec2025/`:

- `test_10_files_metadata.py`
- `test_kofax_table_extraction.py`
- `check_test_progress.py`
- `quick_e2e_test_pure_streaming.py`
- `compare_parsers_from_discovery.py`
- `retrieve_chunk_text_from_pinecone.py`
- `test_local_dec10/` (entire directory)

---

## Root-Level One-Off Scripts (ARCHIVE)

Move to `archive/one_off_scripts_dec2025/`:

- `merge_deal_financial_data.py` - One-time data merge script
- `delete_namespace_records.py` - One-time cleanup script
- `upload_marketing_resources.py` - One-time upload script
- `process_enhanced_salesforce_direct.py` - Alternative processing path (superseded)
- `process_curated_salesforce_files.py` - Alternative processing path (superseded)
- `pipeline_wrapper.py` - Wrapper script (not used)

---

## Tests Directory (ARCHIVE)

Move entire `tests/` directory to `archive/tests_dec2025/`:

**Rationale**: All test files are for development/QA, not production operations. Keep structure intact.

**Contents**:
- `tests/conftest.py`
- `tests/test_*.py` (all test files)
- `tests/chunking_evaluation/` (entire subdirectory)
- `tests/*.md` (test documentation files)
- `tests/*.lock` (test state files)
- `tests/*.progress.json` (test progress files)

**Note**: These are valuable for development but clutter the production codebase.

---

## Streamlit UI Test Files (ARCHIVE)

- `streamlit-ui/test_init.py` → `archive/streamlit-ui/test_init.py`
  - **Reason**: Test file, not production code

---

## OpenAI Batch Code Directory (ARCHIVE)

Move to `archive/openai_batch_code_dec2025/`:

- `openai_batch_code/enhanced_discovery_persistence.py`
- `openai_batch_code/batch_results_checker.py`
- `openai_batch_code/batch_api_integration.py`

**Note**: These may be superseded by `batch_processing_updater.py` and `batch_results_checker.py` in root.

---

## Implementation Steps

1. **Create archive directories**:
   ```bash
   mkdir -p archive/scripts
   mkdir -p archive/test_files_dec2025
   mkdir -p archive/one_off_scripts_dec2025
   mkdir -p archive/tests_dec2025
   mkdir -p archive/streamlit-ui
   mkdir -p archive/openai_batch_code_dec2025
   ```

2. **Move test files**:
   ```bash
   mv test_*.py check_test_progress.py quick_e2e_test_pure_streaming.py \
      compare_parsers_from_discovery.py retrieve_chunk_text_from_pinecone.py \
      archive/test_files_dec2025/
   mv test_local_dec10 archive/test_files_dec2025/
   ```

3. **Move one-off scripts**:
   ```bash
   mv merge_deal_financial_data.py delete_namespace_records.py \
      upload_marketing_resources.py process_enhanced_salesforce_direct.py \
      process_curated_salesforce_files.py pipeline_wrapper.py \
      archive/one_off_scripts_dec2025/
   ```

4. **Move testing scripts**:
   ```bash
   mv scripts/redaction_harness_local.py archive/scripts/
   mv scripts/select_table_test_documents.py archive/scripts/
   mv scripts/compare_pinecone_targets archive/scripts/
   ```

5. **Move tests directory**:
   ```bash
   mv tests archive/tests_dec2025/
   ```

6. **Move streamlit test**:
   ```bash
   mv streamlit-ui/test_init.py archive/streamlit-ui/
   ```

7. **Move OpenAI batch code**:
   ```bash
   mv openai_batch_code archive/openai_batch_code_dec2025/
   ```

8. **Create README in archive**:
   - Document what was moved and why
   - Include date and reference to this plan

---

## Verification

After cleanup, verify:

- [ ] Production scripts (`discover_documents.py`, `process_discovered_documents.py`) still work
- [ ] No imports broken (check for `from tests import` or `import tests`)
- [ ] Scripts directory only contains production utilities
- [ ] Root directory is clean (only production entrypoints)
- [ ] Archive structure is organized and documented

---

## Notes

- **Tests are valuable** - Keep them in archive for future reference
- **Scripts may be reusable** - Archive preserves them for future needs
- **Production focus** - Clean root directory improves clarity
- **Documentation preserved** - All test docs move with test files

---

## Files Summary

| Category | Count | Destination |
|----------|-------|-------------|
| Root test files | 7 files + 1 dir | `archive/test_files_dec2025/` |
| One-off scripts | 6 files | `archive/one_off_scripts_dec2025/` |
| Testing scripts | 3 items | `archive/scripts/` |
| Tests directory | ~30 files | `archive/tests_dec2025/` |
| Streamlit test | 1 file | `archive/streamlit-ui/` |
| OpenAI batch code | 3 files | `archive/openai_batch_code_dec2025/` |

**Total**: ~50 files/directories to archive

