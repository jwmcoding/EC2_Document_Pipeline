## PII / NER Redaction Stage (`src/redaction/`)

### Purpose
This module implements a **PII + client-name redaction stage** for extracted document text and metadata fields, intended to run **before chunking/embedding** so downstream RAG indexing does not store sensitive data.

It supports:
- **Deterministic PII removal**: emails, phones, addresses (regex-based)
- **Deterministic client-name replacement** (based on Salesforce client id + alias patterns)
- **LLM span detection** (OpenAI, e.g. GPT‑5 mini) for:
  - **PERSON** entities (always redacted)
  - **ORG** entities (only redacted when they match the *current client*)
- **Metadata field redaction** (December 2025): Redacts client names from Pinecone metadata fields (`client_name` and `file_name`)
- **Strict-mode validation** to fail fast when sensitive content remains

---

### Key files
- `redaction_service.py`: orchestrates the full redaction pipeline
- `client_registry.py`: loads client registry CSV, generates normalizations (suffix stripping, &/and swaps), compiles regex patterns. Acronyms/abbreviations rely on explicit CSV aliases or LLM detection.
- `llm_span_detector.py`: calls OpenAI Responses API to return span offsets for PERSON/ORG entities; accepts client context for enhanced prompt examples
- `validators.py`: strict-mode validators (post-redaction checks)
- `redaction_context.py`: carries client/vendor identifiers + metadata for the redaction run

---

### Inputs
At runtime, the redaction stage needs:
- **Text**: extracted text (from pdfplumber/Docling/Mistral/etc.)
- **RedactionContext**:
  - `salesforce_client_id` (**required** to enable client redaction)
  - Optional: `client_name`, `vendor_name`, `file_type`, `document_type`
- **Client registry CSV**:
  - Recommended: `src/redaction/SF-Cust-Mapping.csv` (Salesforce export format)
  - Also supported: a “standard” CSV with columns:
    `salesforce_client_id, client_name, industry_label, aliases`

Important:
- “Runs locally” means local files + local outputs; **LLM PERSON/ORG detection still calls OpenAI** when enabled.

---

### Outputs
The redaction stage returns a `RedactionResult` containing:
- `redacted_text`
- `counts`: client/email/phone/address/person replacements
- `model_used` (LLM model name when enabled)
- `validation_passed` / `validation_failures` (strict mode)
- `warnings` / `errors`

**Metadata Redaction** (December 2025):
When redaction is enabled, the pipeline also redacts client names from metadata fields:
- `client_name` → `<<CLIENT>>`
- `file_name` → client name occurrences replaced with `<<CLIENT>>` (e.g., `"Nasdaq Report.pdf"` → `"<<CLIENT>> Report.pdf"`)

This ensures no human-readable client identifiers are stored in Pinecone metadata when redaction is enabled.

For reviewer workflows, use the test harness `scripts/redaction_harness_local.py` which emits per-doc artifacts:
- `original.pdf` (exact bytes used)
- `original.md` (optional pre-redaction markdown)
- `redacted.md` (post-redaction markdown)
- `redaction.json` (counts/model/validation + extraction stats)
- `REDACTION_REVIEW_INDEX.md` (run-level index)

---

### Redaction pipeline logic (current order)
Implemented in `RedactionService.redact()`:

1) **Regex PII** (always):
- Emails → `<<EMAIL>>`
- Phones → `<<PHONE>>`
- Addresses → `<<ADDRESS>>`

2) **LLM spans (optional; OpenAI)**:
- Detect spans of `PERSON` and `ORG` using GPT‑5 mini via Responses API (strict JSON Schema output).
- **PERSON** spans are always replaced with `<<PERSON>>`.
- **ORG** spans are only replaced when they match the **current client** (derived from `salesforce_client_id` and the registry aliases, including **generated variants**). This prevents redacting vendors/competitors.
- **Client context enhancement** (Dec 2025): LLM receives client name in prompt to help detect contextual abbreviations.
- **Filtering includes explicit aliases**: CSV aliases are included in ORG span filtering to ensure detected abbreviations match known client references.
- The implementation avoids applying LLM replacements inside existing `<<...>>` placeholder ranges to prevent corrupting tokens.

3) **Deterministic client-name replacement** (requires `salesforce_client_id`):
- Replace any remaining client mentions/aliases using registry regex patterns.
- **Generated variants**: Full name, normalized, legal suffix stripping, ampersand/and swaps, common token drops, no-space versions.
- **Explicit aliases**: Human-curated nicknames/abbreviations from CSV `aliases` column (e.g., "AmFam" for American Family Insurance).
- **NOTE**: Acronyms/abbreviations are NOT generated programmatically — they're handled by LLM span detection (contextual) or explicit CSV aliases (human-curated). Programmatic acronym generation was removed because cultural nicknames cannot be derived algorithmically.

4) **“Tail collapse” (deterministic cleanup)**
If the deterministic client replacement partially replaces a longer legal entity name, we collapse common leftover tails.

Example:
- Before: `Denver Health and Hospitals Authority Inc`
- After client replacement: `<<CLIENT: …>> and Hospitals Authority Inc`
- After tail collapse: `<<CLIENT: …>>`

This is intentionally conservative and triggers only when:
- the text contains the **client placeholder**, and
- the tail ends with a legal-ish suffix (Inc/LLC/Corp/etc.)

5) **Strict-mode validation**
`validators.py` checks for:
- remaining email/phone patterns
- remaining client name (by registry lookup for the given client id)

If strict mode is on and validation fails, the document is failed.

---

### LLM prompts (what the model is asked)
In `llm_span_detector.py`:
- **API**: Uses OpenAI Responses API (`client.responses.create()`) with strict JSON Schema output
- **Model**: Default `gpt-5-mini` (rolling alias) with `reasoning.effort="minimal"` for deterministic extraction
- **System instruction**: Expert at detecting entities that need anonymization; focus on CLIENT company names/abbreviations; preserve vendor/competitor information
- **User prompt** (`_build_prompt` / `_build_batch_prompt`) includes:
  - **Client-specific examples**: Top 3-4 generated variants (e.g., "AFI", "AmFam" for "American Family Insurance") shown in prompt
  - **PERSON**: ALL human names (client and vendor employees)
  - **ORG**: Client company names, abbreviations, acronyms, nicknames
  - **Abbreviation detection**: Explicit instructions to detect standard acronyms (first letters), common abbreviations (first 2-3 letters), and logical variants
  - Exact character offsets + entity text
  - Max 40 spans per window

The model is **not** asked to perform replacements itself—only to return spans. Client context (name + generated variants) is passed to improve detection accuracy.

---

### Known gotchas / debugging checklist
- **No `salesforce_client_id`** ⇒ client redaction is intentionally skipped (we can’t know which org is “the client”).
- If you see **partial legal names** after redaction (e.g., “and … Inc”), check tail-collapse behavior.
- If you see `<<ADDRE<<PERSON>>`-style corruption, check placeholder-overlap protection in the LLM replacement step.
- If you need a clean before/after diff, run the harness with `--emit-original-md`.

---

### Test harness quick start (recommended)
Run against a discovery JSON that includes Salesforce client ids:

```bash
python scripts/redaction_harness_local.py \
  --input "/Users/jeffmuscarella/2025_Python/Dropbox/tmp_redaction_discovery_sample_10_deals.json" \
  --client-redaction-csv "/Users/jeffmuscarella/2025_Python/Dropbox/src/redaction/SF-Cust-Mapping.csv" \
  --parser-backend docling \
  --docling-ocr-mode on \
  --strict-mode \
  --enable-llm-person-redaction \
  --redaction-model "gpt-5-mini-2025-08-07" \
  --emit-original-md
```

Then open the run index:
- `output/redaction_harness_<timestamp>/REDACTION_REVIEW_INDEX.md`

---

## Code Quality (December 2025)

### Pipeline Script Improvements
The redaction harness (`scripts/redaction_harness_local.py`) and related pipeline scripts have been updated with:

- **Type hints**: All public methods now have return type annotations for better IDE support and type checking
- **Google-style docstrings**: Standardized documentation format with Args/Returns/Raises sections
- **Constants extraction**: Magic numbers (batch sizes, timeouts, model names) extracted to named constants
- **Module exports**: Explicit `__all__` declarations to control public API surface
- **Security**: API keys no longer logged in command examples (use environment variable references)

These improvements enhance maintainability and follow Python best practices (PEP 8) without changing redaction functionality.


