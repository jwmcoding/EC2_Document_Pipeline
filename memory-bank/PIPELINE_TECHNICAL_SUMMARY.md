# Business Document Processing Pipeline — Technical Summary (for Colleagues)

**Audience**: Technical teammates onboarding to the pipeline architecture and ops  
**Scope**: Current repo (not historical `/archive` docs)  
**Last updated**: 2025-12-17

---

## Executive Summary

This repository implements a **two-phase document processing pipeline** that turns large sets of business documents (PDFs, Office files, emails) into a **hybrid-searchable** Pinecone index (dense + sparse vectors) with **business metadata** (deal/vendor/client context) for filtering and retrieval.

- **Phase 1 (Discovery)**: Enumerate files and produce a **discovery JSON** (fast, resumable, minimal compute).
- **Phase 2 (Processing)**: Download/parse content, chunk text, embed chunks, and **upsert vectors to Pinecone** (parallelizable, resumable, production-logged).

For PDFs, the pipeline supports multiple extraction backends:
- **pdfplumber**: very fast; weaker on complex tables.
- **Docling**: slower; best table extraction (TableFormer) and stronger structure.
- **Mistral OCR**: external OCR service; can help on scanned PDFs; mid-speed; output quality varies by doc.

**Text storage policy (current)**: chunk text **is stored in Pinecone metadata** under the `text` field, truncated (~37KB) to stay under Pinecone’s per-record metadata limit. This enables lightweight keyword/metadata inspection in downstream tooling without an external text store.

---

## High-Level Architecture

### Inputs (document sources)

The pipeline can discover documents from multiple sources using a common “file source” abstraction:
- **Dropbox (`dropbox`)**: live listing and download via Dropbox API.
- **Local filesystem (`local`)**: process files on disk.
- **Salesforce organized exports (`salesforce`)**: enriched metadata via mapping files.
- **Raw Salesforce exports (`salesforce_raw`)**: enriched metadata via ContentVersion/Link CSVs + Deal CSVs (strongly recommended for production Salesforce exports).

### Outputs

- **Discovery JSON**: an inventory of files + metadata + processing status (resumable).
- **Pinecone records**: one record per chunk, containing:
  - Dense vector embedding (semantic)
  - Sparse vector embedding (keyword)
  - Metadata (filtering + provenance)

### Core components (conceptual)

1. **Discovery**
   - List files, capture metadata, write JSON incrementally.
2. **Document conversion**
   - Normalize file types (e.g., extract text from DOCX/XLSX/MSG; obtain PDF bytes; etc.).
3. **PDF parsing**
   - Extract text + tables from PDF (backend-specific).
4. **Chunking**
   - Convert parsed text into semantically coherent chunks, preserving table blocks.
5. **Embedding**
   - Generate dense + sparse embeddings via Pinecone Inference API.
6. **Upsert**
   - Batch upsert chunk vectors to Pinecone with size-limit safeguards.

---

## Phase 1 — Discovery (fast + resumable)

**Entrypoint**: `discover_documents.py`

Responsibilities:
- **Enumerate files** from a chosen source.
- Extract **business context** (vendor/client/deal info where available).
- Write an **appendable discovery file** via `DiscoveryPersistence`.
- Support **resume** (`--resume`) for interrupted runs.

Key operational guidance:
- For Salesforce raw exports: use `--source salesforce_raw` with the required CSVs.
- For non-Salesforce local folders: use `--source local`.

**Why discovery is separate**: it lets you cheaply (and deterministically) define the processing set once, then re-run Phase 2 with different parsing settings or different limits without re-enumerating everything.

---

## Phase 2 — Processing (parse → chunk → embed → upsert)

**Entrypoint**: `process_discovered_documents.py`

Responsibilities:
- Load discovery JSON and select documents (supports filters and limits).
- Initialize the source client (download bytes) and the Pinecone client (embeddings + upsert).
- For each document:
  - Convert to processable form (`DocumentConverter`)
  - Parse (PDF backends or Office/email parsers)
  - (Optional) **Redaction**: remove client names + PII before chunking
  - Chunk (semantic chunker + table preservation)
  - Embed (dense + sparse)
  - Upsert to Pinecone
  - Persist per-document **processing_status** back into the discovery JSON

Scaling/ops:
- Parallel processing is supported in the pipeline (workers; resumable runs).
- Progress logging is designed for long runs (rate + ETA style monitoring).

### Processing Status Schema (per-document)

Each document's `processing_status` is stored in the discovery JSON:

```json
{
  "processed": true,
  "processing_date": "2025-12-13T...",
  "processor_version": "2.0",
  "parser_backend": "mistral",       // PDF parser selection (mistral, docling, pdfplumber)
  "content_parser": "extract_msg",   // Actual parser used for file type
  "chunks_created": 4,
  "vectors_created": 4,
  "pinecone_namespace": "...",
  "processing_errors": [],
  "processing_time_seconds": 0.68
}
```

**`content_parser` values by file type:**
| File Type | `content_parser` |
|-----------|------------------|
| `.pdf` | `mistral`, `docling`, or `pdfplumber` |
| `.xlsx`, `.xls`, `.csv` | `pandas_openpyxl` |
| `.docx` | `python_docx` |
| `.doc` | `docx2txt` |
| `.msg` | `extract_msg` |
| `.pptx` | `python_pptx` |
| `.txt` | `direct_text` |

---

## Optional Stage — PII / NER Redaction (pre-chunking)

**Purpose**: remove sensitive information from extracted text **before** chunking/embedding so downstream RAG indexing does not store PII/client identifiers.

**Implementation**: `src/redaction/` (see `src/redaction/README.md` for full detail)

### Production Usage (December 2025)

Enable redaction in the processing pipeline with CLI flags:

```bash
python process_discovered_documents.py \
  --input discovery.json \
  --namespace production \
  --workers 6 \
  --enable-redaction \
  --client-redaction-csv src/redaction/SF-Cust-Mapping.csv \
  --redaction-model gpt-5-mini-2025-08-07
```

| Flag | Description |
|------|-------------|
| `--enable-redaction` | Enables the PII redaction stage |
| `--client-redaction-csv` | Path to client registry CSV (required for client redaction) |
| `--redaction-model` | OpenAI model for PERSON detection (default: `gpt-5-mini-2025-08-07`) |

### What it redacts
- **Client references**: replaced with a placeholder token `<<CLIENT: …>>` (requires `salesforce_client_id`)
- **PERSON names**: replaced with `<<PERSON>>` (LLM-driven via OpenAI Responses API)
- **Emails**: `<<EMAIL>>` (regex)
- **Phones**: `<<PHONE>>` (regex)
- **Addresses**: `<<ADDRESS>>` (regex)

### How it works (high-level)
The redaction stage is orchestrated by `RedactionService` and runs:
1) Regex PII (email/phone/address)
2) OpenAI LLM span detection via Responses API (PERSON always; ORG only when it matches the current client)
3) Deterministic client alias replacement (backstop for acronyms like "VSP")
4) Conservative cleanup of partial legal entity name tails (when a client name is part of a longer legal entity string)
5) Strict-mode validators (fail fast if patterns remain)

### Requirements
- **`salesforce_client_id`** must be present in document metadata (from Salesforce discovery)
- Client registry CSV must contain the client ID for redaction to apply
- Documents without `salesforce_client_id` skip client redaction (regex PII patterns still apply)

### Testing artifacts (review workflow)
Use `scripts/redaction_harness_local.py` to generate:
- `original.pdf` (exact bytes used)
- `original.md` (optional pre-redaction markdown)
- `redacted.md`
- `redaction.json`
- `REDACTION_REVIEW_INDEX.md`

This is testing-only output under `output/redaction_harness_<timestamp>/`.

## PDF Parsing Backends (Mistral vs Docling vs pdfplumber)

**Default**: `--parser-backend mistral` (as of December 2025)

### Mistral OCR (`src/parsers/mistral_parser.py`) — **RECOMMENDED**

- **Strengths**: Best balance of quality and speed for production; handles scanned PDFs well; provides page-level markdown.
- **Performance**: ~12.56s/PDF average (API-based)
- **Trade-offs**: Network latency + per-document API cost; tables are best-effort markdown (not structured).
- **Requires**: `MISTRAL_API_KEY` in `.env`

#### Reliability notes (December 18, 2025)
- **Request-level timeouts**: the Mistral SDK client is configured with `timeout_ms` so workers cannot wedge indefinitely on a blocking HTTP call.
- **Large PDF handling**: PDFs **> 20MB** are automatically split into **page-range chunks** and OCR’d chunk-by-chunk (default 10 pages per chunk), then stitched back together. This allows “include large PDFs” runs to complete reliably.

### Docling (`src/parsers/docling_parser.py`)

- **Strengths**: Best structured extraction, especially tables (TableFormer) and more "document-like" layout understanding.
- **Performance**: ~50.77s/PDF average (slowest)
- **Configuration**:
  - OCR enabled (for scanned PDFs)
  - TableFormer enabled (`ACCURATE` mode) for table structure
  - Uses a hard per-PDF timeout guard (Unix signals)
- **Best for**: BOMs, pricing grids, contracts with structured exhibits.
- **Weaknesses**: Much slower and can fail on specific PDFs (pipeline failures / complexity).

### pdfplumber (`src/parsers/pdfplumber_parser.py`)

- **Strengths**: Very fast (~1.37s/PDF), local-only, no API costs.
- **Tables**: Uses pdfplumber's table extraction with tuned line-based settings; tables are formatted inline for chunker detection.
- **Weaknesses**: Table extraction is heuristic and can struggle with complex layouts / dense pricing grids.
- **Best for**: Quick local testing or when API costs are a concern.

---

## Chunking and Table Preservation

The pipeline uses a semantic chunking strategy designed to:
- Keep sections coherent for retrieval quality
- **Preserve tables** as single units (avoid splitting rows/columns across chunks)

Tables extracted (from pdfplumber or Docling) are normalized and formatted through a unified table formatter, so that downstream chunking can recognize table blocks and keep them intact.

---

## Embeddings + Hybrid Search (Pinecone)

The Pinecone integration is built around **hybrid retrieval**:
- **Dense embedding**: `multilingual-e5-large`
- **Sparse embedding**: `pinecone-sparse-english-v0`
- Optional reranking (Cohere) is supported via Pinecone inference APIs.

Operational constraints the code is designed around:
- **Metadata size limit** (per-record): ~40KB
- **Request size limits**: conservative batching for embedding and upsert

### Text storage policy (important)

This repo treats Pinecone as a **vector + metadata index**, not a text store:
- We do **not** store chunk text in Pinecone metadata (to avoid size blowups).
- We do **not** add a nonstandard “top-level text field” to Pinecone records.

If the product later requires showing the chunk text in a UI, we should:
- Store text in an **external system** (S3/DB) and reference it from metadata, **or**
- Re-parse the source document on-demand (slower but simplest).

---

## Benchmark Results — Docling vs Mistral vs pdfplumber (local sample)

The repo includes a local benchmark harness:
- **Runner**: `test_local_dec10/run_local_test.py --parser all`
- **Summary output**: `test_local_dec10/LOCAL_RESULTS_SUMMARY.md`

### Timing headline (local Apple Silicon; small PDF set)

Average PDF processing times observed:
- **pdfplumber**: **~1.37s / PDF**
- **Mistral OCR**: **~12.56s / PDF**
- **Docling**: **~50.77s / PDF**

Notable nuance:
- At least one test PDF (`GSA_Pricing.pdf`) produced a Docling “pipeline failed” message while still returning a textual error payload (i.e., the harness recorded it as a “completed run,” but the extraction was not successful).

### Interpretation / what these results imply

- **pdfplumber** is a strong “throughput baseline” and can be the fastest path when tables are not critical.
- **Docling** is slower but is the best fit when **table fidelity is the primary goal** (quotes, BOMs, pricing schedules, contracts with structured exhibits).
- **Mistral OCR** sits between them on speed, with value primarily for **scan-heavy PDFs**; however, its output is less “structured-table native” than Docling.

### Caveats

These timings are:
- A **small, local sample** (not a statistically rigorous benchmark).
- Hardware- and document-dependent (Docling cost varies widely with layout complexity; OCR adds cost).
- Mistral includes network + API latency, so EC2 region, congestion, and request patterns matter.

---

## Production Notes and “What to Watch”

- **Choose the right source**: Salesforce exports must use `salesforce_raw` (raw) or `salesforce` (organized) to avoid NULL business metadata.
- **Timeouts matter**: Docling and OCR can hang on complex docs; timeouts prevent worker starvation.
- **Batch sizing**: keep embedding and upsert payloads under service limits.
- **Resume-first workflow**: discovery + processing status in JSON is the backbone of safe long runs.
- **Logging**: use progress logs to monitor sustained rate and identify hotspots (slow PDFs, failing parsers, retry storms).

---

## Appendix — Key Repo Entrypoints

- `discover_documents.py`: Phase 1 discovery → discovery JSON
- `process_discovered_documents.py`: Phase 2 processing from discovery JSON → Pinecone upserts
- `src/pipeline/document_processor.py`: end-to-end “one document” orchestration
- `src/parsers/`:
  - `pdfplumber_parser.py`
  - `docling_parser.py`
  - `mistral_parser.py`
- `src/connectors/pinecone_client.py`: embeddings, upsert, search
- `tests/` and `test_local_dec10/`: test + benchmark harnesses


