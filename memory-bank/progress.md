# Progress Log

## 2025-12-18 (Late Morning - NaN Sanitization Fix)
- **Fixed critical Pinecone upsert bug**: Added `_sanitize_numeric()` function to handle NaN values in numeric metadata fields
- **Root cause**: `float(metadata.get("field") or 0.0)` failed because pandas NaN is truthy, so `or 0.0` never triggered
- **Solution**: Created `_sanitize_numeric()` that explicitly checks for:
  - Python None
  - pandas NaN (via `pd.isna()`)
  - Python float NaN (via `math.isnan()`)
  - NumPy NaN
  - Invalid conversions (ValueError, TypeError)
- **Applied to 7 numeric fields**: `final_amount`, `savings_1yr`, `savings_3yr`, `fixed_savings`, `savings_target_full_term`, `chunk_index`
- **Testing**: Verified all NaN types (Python, pandas, NumPy) convert to 0.0 default
- **Impact**: Fixes ~45% failure rate during benchmark run where documents with empty financial fields were failing at upsert stage

## 2025-12-18 (Morning)
- **Mistral API error handling enhancements**:
  - Added `_validate_api_key()` method that tests API key during `MistralParser.__init__()` - fails fast with clear error instead of hanging 12+ minutes into processing
  - Added `_classify_api_error()` to detect specific failure types: 401 Unauthorized, 429 Rate Limit, 402 Spending Limit, 503 Service Unavailable, 504 Gateway Timeout, 500 Server Error
  - Added `_create_error_message()` with actionable guidance for each error type (what went wrong, how to fix it, alternative solutions)
  - Errors log prominently with 🚨 emoji for critical issues (rate limit, spending limit, service down)
  - Each error message includes: clear explanation, action required steps, links to Mistral console, fallback parser options
  - **Result**: Users immediately know when API limits are hit, spending caps are reached, or keys are invalid - no more silent hangs
- **Constants ordering fix**: Moved `METADATA_TEXT_MAX_BYTES` and related constants to top of `parallel_processor.py` (line 24) to fix `NameError` when used in function default parameters
- **Discovery schema fix**: Updated benchmark discovery file from old `"metadata"` format to schema v2.1 `"discovery_metadata"` format to prevent schema mismatch errors during processing

## 2025-12-17 (Late Evening)
- **Default PDF parser changed to Mistral OCR**: Updated `process_discovered_documents.py` default from `pdfplumber` to `mistral`.
  - Mistral provides better production quality, especially for scanned/image PDFs
  - ~12.56s/PDF (vs pdfplumber ~1.37s, Docling ~50.77s)
  - Requires `MISTRAL_API_KEY` in `.env`
  - Override with `--parser-backend pdfplumber` (fast, local) or `--parser-backend docling` (best tables)
- **Redaction pipeline integration confirmed**: Trial run with 14 documents validated redaction stage works end-to-end. LLM span detection calls Responses API before chunking. 51 chunks upserted to `redaction-trial-dec2025` namespace.
- **Documentation updates**: `AGENTS.md`, `PIPELINE_TECHNICAL_SUMMARY.md`, `activeContext.md` updated with Mistral as default.

## 2025-12-17 (Evening)
- **Pipeline code quality improvements**: Comprehensive standardization across key pipeline scripts (`discover_documents.py`, `process_discovered_documents.py`, `parallel_processor.py`, `redaction_harness_local.py`):
  - **Security**: Removed API key from log output (use environment variable references)
  - **Type hints**: Added return type annotations to all public methods
  - **Docstrings**: Standardized to Google style with Args/Returns/Raises sections
  - **Constants**: Extracted magic numbers (batch sizes, timeouts, model names) to named constants
  - **Refactoring**: Split `_convert_to_document_metadata()` into 4 focused helper methods (`_extract_file_info`, `_extract_business_metadata`, `_extract_deal_metadata`, `_extract_llm_classification`)
  - **Module exports**: Added explicit `__all__` declarations
  - **Tests**: Created `tests/test_document_processor_helpers.py` for new helper methods
  - **Result**: Zero breaking changes, improved maintainability, better IDE support, all linting passes

## 2025-12-17 (Afternoon)
- **Redaction Responses API migration**: Migrated LLM span detection from Chat Completions to Responses API (`client.responses.create()`) with strict JSON Schema output, `reasoning.effort="minimal"`, and default model `gpt-5-mini` (rolling alias). Preserves existing prompt text and span cap; improves observability and reduces empty-content failures.
- **Simplified deterministic variant generation** (late afternoon):
  - **Removed** programmatic acronym/abbreviation generation from `ClientRegistry._generate_variants()`
  - **Kept** reliable patterns: full names, normalizations, legal suffix stripping, &/and swaps, no-space versions
  - **Rationale**: Cultural/business nicknames (e.g., "AmFam", "BofA", "Citi") cannot be derived algorithmically. Short acronyms (e.g., "MS", "AF") risk false positives across unrelated terms.
  - **Abbreviation detection now relies on**: (1) LLM contextual understanding, (2) explicit CSV aliases (human-curated)
- **Documentation**: Updated `src/redaction/README.md` to reflect simplified approach.
- **Future work identified**: Curate explicit aliases in `SF-Cust-Mapping.csv` for top 50-100 high-volume clients to provide guaranteed deterministic redaction of known nicknames.

## 2025-12-17 (Morning)
- **Redaction LLM span detection diagnostics**: Added structured error logging in `src/redaction/llm_span_detector.py` when OpenAI returns HTTP 200 but `message.content` is empty, and when JSON parsing fails.
- **What gets logged**: `finish_reason`, `has_refusal`, `tool_calls_present`, `response_id/model`, `usage` (if present), and safe correlation fields (`content_len`, `content_sha256_16`) plus window/batch sizing context (no document text).
- **Docs updated**: `src/redaction/troubleshooting.md` and `memory-bank/activeContext.md` now describe the new log events and how to use them.
- **Redaction harness Word-doc parsing fix**: Updated `scripts/redaction_harness_local.py` so non-PDF files (e.g. `.docx`) are not reported as “docling”; outputs now include `content_parser` (e.g. `python_docx`) and `original.<ext>` artifacts (e.g. `original.docx`).
- **Re-run (Word docs)**: `output/redaction_harness_20251217_104938/` generated 3 `.docx` artifacts from `tmp_redaction_discovery_sample_10_deals.json` with `content_parser=python_docx`.

### Next step
- Re-run the redaction harness run that intermittently failed and grep logs for `llm_span_detection_empty_content` / `llm_span_detection_json_decode_error` to confirm whether the issue is refusal/tool-calls vs true empty payload.

