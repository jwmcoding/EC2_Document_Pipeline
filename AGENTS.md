# AGENTS.md - Business Document Processing Pipeline

> Cross-agent guidance for working with this repository. For Pinecone-specific patterns (API usage, batch limits, common mistakes), see the workspace rules which are automatically applied.

---

## 📋 Project Overview

**Business Document Processing Pipeline** - Transforms business documents into searchable intelligence using LLM classification and vector embeddings. See `memory-bank/projectbrief.md` for full details.

### Key Directory Structure

```
src/                     # Source code modules
memory-bank/             # Project documentation (READ THESE FIRST)
streamlit-ui/            # Business intelligence interface
logs/                    # Structured logging output
tests/                   # Test files
config/                  # YAML configuration files
```

---

## 🔧 Setup & Development

### Environment Setup

```bash
# 1. Clone and create virtual environment
cd <project-directory>
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure API keys in .env
DROPBOX_ACCESS_TOKEN=your_token
OPENAI_API_KEY=your_key
PINECONE_API_KEY=your_key
```

### Running the Application

```bash
# Start Streamlit business intelligence interface
cd streamlit-ui
streamlit run app_standalone.py --server.port 8501

# Run document discovery
python discover_documents.py --source local --path "/your/docs" --output discovery.json

# Process discovered documents (parallel)
python process_discovered_documents.py --input discovery.json --parallel --workers 8
```

### Running Tests

```bash
pytest tests/ -v
```

---

## 📝 Code Style Guidelines

### Python Standards

- **Style**: PEP 8 compliance
- **Module size**: Keep modules < 600 LOC
- **Functions**: Prefer small, cohesive functions
- **Type hints**: Required for function definitions, public methods, and non-obvious variables

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `DocumentProcessor`, `PineconeDocumentClient`)
- **Functions/methods**: `snake_case` (e.g., `process_document`, `extract_metadata`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAX_BATCH_SIZE`, `DEFAULT_NAMESPACE`)
- **Private methods**: `_leading_underscore` (e.g., `_parse_content`)

### Import Order

```python
# Standard library
import os
import json
from typing import Dict, List, Optional

# Third-party
from openai import OpenAI
from pinecone import Pinecone

# Local imports
from src.config.settings import Settings
from src.connectors.pinecone_client import PineconeDocumentClient
```

### Documentation

- Docstrings for all public functions and classes
- **Do NOT create .md files for small fixes** - only for major features/systems
- Update `memory-bank/activeContext.md` after significant changes

---

## 🔄 Typical Workflows

### Discovery → Processing Workflow

```bash
# 1. Discovery (fast, free)
python discover_documents.py --source local --path "/docs" --output discovery.json

# 2. Processing (parallel for speed)
python process_discovered_documents.py --input discovery.json --workers 8

# 3. Optional: Batch enhancement (50% savings)
python process_discovered_documents.py --input discovery.json --use-batch --batch-only
python batch_processing_updater.py --job-id batch_xxx --monitor --update
```

### Processing with PII Redaction (December 2025)

Enable PII redaction to remove client names, person names, emails, phones, and addresses **before** chunking:

```bash
# Parallel processing with redaction enabled (Mistral OCR is default)
python process_discovered_documents.py \
  --input discovery.json \
  --namespace production \
  --workers 6 \
  --enable-redaction \
  --client-redaction-csv src/redaction/SF-Cust-Mapping.csv \
  --redaction-model gpt-5-mini-2025-08-07
```

**PDF Parser Options:**
- `--parser-backend mistral` (default) - Mistral OCR API, best for production
- `--parser-backend docling` - Best for structured tables/pricing grids
- `--parser-backend pdfplumber` - Fast local baseline, no API costs

**Redaction flags:**
- `--enable-redaction` - Enables the PII redaction stage
- `--client-redaction-csv` - Path to client registry CSV (required for client name redaction)
- `--redaction-model` - OpenAI model for PERSON entity detection (default: `gpt-5-mini-2025-08-07`)

**What gets redacted:**
- Client names → `<<CLIENT: ClientName>>`
- Person names → `<<PERSON>>`
- Emails → `<<EMAIL>>`
- Phones → `<<PHONE>>`
- Addresses → `<<ADDRESS>>`

**Requirements:**
- `salesforce_client_id` must be present in document metadata (from Salesforce discovery)
- Client registry CSV must include the client ID for redaction to apply

### Salesforce Export Processing

```bash
# ⚠️ CRITICAL: Use --source salesforce_raw (NOT --source local) for Salesforce exports!
# ⚠️ CRITICAL: Include vendor/client mapping CSVs or names will be NULL!
python discover_documents.py --source salesforce_raw \
  --export-root-dir /path/to/export \
  --content-versions-csv .../ContentVersion.csv \
  --content-document-links-csv .../ContentDocumentLink.csv \
  --deal-metadata-csv .../Deal__c.csv \
  --vendor-mapping-csv .../SF-Vendor_mapping.csv \
  --client-mapping-csv .../SF-Cust-Mapping.csv \
  --require-deal-association \
  --output salesforce_discovery.json
```

**⚠️ Mapping CSV Files (REQUIRED for vendor/client names):**
- `SF-Vendor_mapping.csv` - Maps vendor IDs to names (6,259 vendors)
- `SF-Cust-Mapping.csv` - Maps client IDs to names (955 clients)
- These files are **NOT auto-generated** - copy from previous exports or export manually from Salesforce

### Monitoring Long-Running Operations

```bash
tail -f logs/progress/*_progress_*.log           # Real-time progress
cat logs/progress/latest_completion_summary.txt  # Instant results
```

---

## ⚠️ Critical Anti-Patterns

> Full patterns documented in `memory-bank/systemPatterns.md`. These are the most critical:

### Source Selection

**⚠️ Using `--source local` on Salesforce exports = NULL metadata!** Always use `--source salesforce_raw`.

### Missing Mapping CSVs

**⚠️ Omitting `--vendor-mapping-csv` or `--client-mapping-csv` = NULL names!** Always include both:
```bash
--vendor-mapping-csv /path/to/SF-Vendor_mapping.csv \
--client-mapping-csv /path/to/SF-Cust-Mapping.csv
```

### String Value Detection

```python
# ❌ WRONG: "None".strip() is truthy
if vendor_id and str(vendor_id).strip(): ...

# ✅ CORRECT: Explicit null detection
vendor_id_clean = str(vendor_id).strip() if vendor_id else ''
if vendor_id_clean and vendor_id_clean.lower() not in ['none', 'nan', '']: ...
```

### Pinecone Pagination

```python
# ❌ WRONG: Same vector = infinite loop
while True: results = index.query(vector=[0.0] * 1024, top_k=100)

# ✅ CORRECT: Use list() method
for vector_ids in index.list(namespace=namespace, limit=100): ...
```

---

## 📚 Memory Bank Protocol

When starting a task, **read these files** (in order):

1. `memory-bank/projectbrief.md` - Mission, users, requirements
2. `memory-bank/systemPatterns.md` - Critical anti-patterns and architecture
3. `memory-bank/techContext.md` - Technical stack details
4. `memory-bank/activeContext.md` - Current focus and quick reference

**After meaningful work**, update `memory-bank/activeContext.md` with concise bullets on current state.

### Archive Exclusion Rule (Context Hygiene)

- **Default behavior**: Do **NOT** search, read, or use any documents under `/archive` (including `memory-bank/archive/` and top-level `archive/`) for context, planning, or answers.
- **Exception**: Only consult `/archive` when the user **explicitly asks** to use it or requests historical details that require it.
- **Why**: Archive content is intentionally verbose/historical and can waste context or cause outdated guidance to leak into current work.

**For historical details (only when explicitly requested)**, see `memory-bank/archive/` (detailed patterns, progress history, etc.)

### Dead Code Archive (December 2025)

The `archive/dead_code_dec2025/` directory contains **unused code** that was removed during a December 2025 cleanup:

| File | Original Location | Reason Archived |
|------|-------------------|-----------------|
| `parallel_document_processor.py` | `src/pipeline/` | Superseded by `parallel_processor.py` |
| `enhanced_batch_processor.py` | `src/pipeline/` | Not imported anywhere |
| `raw_salesforce_export_connector_streaming.py` | `src/connectors/` | Superseded, never used |
| `optimized_document_converter.py` | `src/parsers/` | Only used in archive |
| `optimized_pdf_processor.py` | `src/parsers/` | Only used by dead code |
| `optimized_excel_processor.py` | `src/parsers/` | Only used by dead code |
| `discovery_cache.py` | `src/utils/` | Old cache system, replaced |
| `discovery_cache_migrator.py` | `src/utils/` | Not imported anywhere |
| `production_logging.py` | `src/config/` | Not imported anywhere |
| `embedding_service.py` | `src/embeddings/` | Replaced by Pinecone embeddings |
| `langchain_embedding_wrapper.py` | `src/embeddings/` | Dead LangChain adapter |
| `test_performance_simple.py` | `src/` | Misplaced test file |
| `manage_pipeline.py` | root | July 2025, obsolete |
| `config.py` | root | July 2025, obsolete |
| `Dropbox_api.py` | root | July 2025, obsolete |

**⚠️ Do NOT include this directory in searches or code exploration unless explicitly requested.**
