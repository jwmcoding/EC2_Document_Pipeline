# Pinecone Index/Namespace Document Comparator

Compare documents between two different Pinecone indexes and namespaces to evaluate parsing/extraction quality and embedding differences.

## Overview

This tool locates the same document in two Pinecone targets (index + namespace), reconstitutes the full document text from chunk records, and compares:
- **Text quality diagnostics** for LLM usefulness (no single score, side-by-side metrics)
- **Embedding comparison** (centroid similarity when dimensions match)

## Features

- **Document Resolution**: Find documents by `deal_id`, `file_name`, or `document_name` with exact filter matching + bounded scan fallback
- **Chunk Reconstruction**: Reassembles full document text from chunks, preserving section markers and table formatting
- **Text Diagnostics**: Comprehensive metrics including:
  - Text length, chunk counts, empty chunk percentage
  - Repeated lines ratio, non-ASCII density (OCR artifacts)
  - Table marker detection, section header counting
  - OCR artifact scoring (broken hyphens, excessive whitespace, mixed case)
- **Embedding Comparison**: Computes centroid similarity between document embeddings when dimensions match
- **Dual Output**: Machine-readable JSON + human-readable Markdown reports

## Installation

No additional dependencies beyond the main project requirements. Uses existing:
- `src/connectors/pinecone_client.py` (PineconeDocumentClient)
- `src/config/settings.py` (Settings)

## Usage

### Basic Example

```bash
python scripts/compare_pinecone_targets/compare_pinecone_targets.py \
  --left-index npi-deal-data \
  --left-namespace test-namespace \
  --right-index npi-deal-data \
  --right-namespace production-namespace \
  --deal-id "58773" \
  --file-name "contract.pdf"
```

## Test harness: sample 25 deals with PDFs (NEW)

This harness scans the **source** index/namespace to find deals that actually have PDFs, samples 25 deal_ids, then reconstructs the corresponding PDFs from the **target** namespace.

Defaults are set to your current targets:
- **Source**: `business-documents` / `SF-Files-2020-8-15-25`
- **Target**: `npi-deal-data` / `sf-export-aug15-2025`

```bash
python scripts/compare_pinecone_targets/sample_25_deals_harness.py
```

Outputs (under `output/harness_sample_25_deals_<timestamp>/`):
- `manifest.json`
- `summary.md`
- `reconstructed/<deal_id>/<file_name>.source.txt`
- `reconstructed/<deal_id>/<file_name>.target.txt`

### LLM-as-judge mode (GPT-5.2)

Add `--llm-judge` to have the harness ask an OpenAI model to judge which reconstruction is better **for LLM use** (no numeric scores; JSON verdict stored in the manifest).

```bash
python scripts/compare_pinecone_targets/sample_25_deals_harness.py \
  --llm-judge \
  --llm-model gpt-5.2
```

Notes:
- Requires `OPENAI_API_KEY` in your environment.
- You can override the default model via `OPENAI_JUDGE_MODEL` or `--llm-model`.

### Pricing-focused eval / judge (NEW)

If your goal is **prices, pricing tables, price trends, and comparisons**, the harness can:
- compute deterministic **pricing signals** (currency/percent/year density, numeric token density, pricing/trend keyword hits)
- optionally run a **pricing-focused GPT judge** rubric

```bash
python scripts/compare_pinecone_targets/sample_25_deals_harness.py \
  --pricing-eval \
  --llm-judge --pricing-judge \
  --llm-model gpt-5.2
```

Where to look:
- `manifest.json` → per PDF: `source.pricing_signals`, `target.pricing_signals`, and `pricing_judgment`
- `summary.md` → aggregate pricing deltas + pricing judge winner counts

### Search by File Name Only

```bash
python scripts/compare_pinecone_targets/compare_pinecone_targets.py \
  --left-index index1 --left-namespace ns1 \
  --right-index index2 --right-namespace ns2 \
  --file-name "document.pdf" \
  --match-mode scan
```

### Advanced Options

```bash
python scripts/compare_pinecone_targets/compare_pinecone_targets.py \
  --left-index npi-deal-data --left-namespace test \
  --right-index npi-deal-data --right-namespace prod \
  --deal-id "58773" --file-name "contract.pdf" \
  --match-mode auto \
  --scan-max-ids 5000 \
  --scan-batch-size 100 \
  --output-dir output/my_comparison \
  --no-reconstructed-files
```

## Arguments

### Required

- `--left-index`: Left Pinecone index name
- `--left-namespace`: Left Pinecone namespace
- `--right-index`: Right Pinecone index name
- `--right-namespace`: Right Pinecone namespace

### Search Criteria (at least one required)

- `--deal-id`: Deal ID to search for
- `--file-name`: File name to search for
- `--document-name`: Alias for `--file-name`

### Optional

- `--match-mode`: `exact` (filter only), `scan` (enumerate), or `auto` (try exact then scan) [default: `auto`]
- `--scan-max-ids`: Maximum IDs to scan in scan mode [default: 10000]
- `--scan-batch-size`: Batch size for scanning [default: 100]
- `--output-dir`: Output directory [default: `output/compare_<timestamp>`]
- `--no-reconstructed-files`: Skip saving reconstructed text files

## Output

The tool generates three files in the output directory:

1. **`comparison.json`**: Machine-readable comparison data
   - Document resolution details
   - Chunk metadata and IDs
   - Full reconstructed text
   - Text diagnostics
   - Embedding comparison results

2. **`comparison.md`**: Human-readable Markdown report
   - Resolution summary
   - Side-by-side diagnostics table
   - Embedding comparison results
   - Text excerpts (head/middle/tail)
   - Chunk ID tables for traceability

3. **`left_reconstructed.txt`** / **`right_reconstructed.txt`** (optional): Full reconstructed text files

## Document Resolution Strategy

The tool uses a hierarchical document key strategy:

1. **Primary**: `(deal_id, file_name)` when both exist (best disambiguation)
2. **Fallback**: `file_name` only
3. **Optional**: Other stable identifiers (`salesforce_deal_id`, `salesforce_content_version_id`)

This avoids dependency on `document_path` which may not exist in all schemas.

## Text Diagnostics Explained

### Basic Metrics
- **Total Length**: Character count of reconstructed text
- **Chunk Count**: Number of chunks comprising the document
- **Empty Chunks %**: Percentage of chunks with no text content

### Quality Indicators
- **Repeated Lines Ratio**: Proportion of duplicate lines (indicates extraction issues)
- **Non-ASCII Density %**: Percentage of non-ASCII characters (OCR artifact indicator)
- **Table Markers**: Count of `=== TableName ===` markers (table preservation)
- **Section Headers**: Count of section boundary markers

### OCR Artifact Score
Composite score (0-100) based on:
- Broken hyphenation patterns (`word- \n word`)
- Excessive whitespace (3+ consecutive spaces)
- Mixed case artifacts (e.g., "ThIs Is OcR")

Lower scores indicate cleaner extraction.

## Embedding Comparison

When both documents have retrievable vectors with matching dimensions:

- **Centroid Similarity**: Cosine similarity between document centroid vectors
- **Vector Statistics**: Average norms, duplicate vector ratios
- **Comparability**: Clear reporting when dimensions differ or vectors unavailable

## Limitations

1. **Chunk Limit**: Pinecone query has a `top_k` limit of 200 chunks. Documents with >200 chunks will only show the first 200. Use scan mode for complete coverage.

2. **Vector Retrieval**: Vectors are fetched separately via `fetch()`. If vectors aren't stored or accessible, embedding comparison will be marked as "not comparable".

3. **Schema Differences**: The tool adapts to different metadata schemas, but some fields may be missing in older indexes.

## Examples

### Compare Mistral vs Docling Parsing

```bash
python scripts/compare_pinecone_targets/compare_pinecone_targets.py \
  --left-index npi-deal-data --left-namespace benchmark-mistral \
  --right-index npi-deal-data --right-namespace benchmark-docling \
  --file-name "GSA_Pricing.pdf"
```

### Compare Production vs Test Namespace

```bash
python scripts/compare_pinecone_targets/compare_pinecone_targets.py \
  --left-index npi-deal-data --left-namespace production \
  --right-index npi-deal-data --right-namespace test-full-pipeline-20docs-20251213 \
  --deal-id "58773" --file-name "contract.pdf"
```

## Integration with Existing Tools

This tool complements:
- `retrieve_chunk_text_from_pinecone.py`: Single-namespace chunk retrieval
- `parser_comparison_results/`: Parser output comparisons
- Streamlit UI: Interactive document search

## Error Handling

- **Document Not Found**: Clear error messages with suggestions
- **Ambiguous Matches**: Lists all candidates for user selection
- **Missing Vectors**: Gracefully handles unavailable embeddings
- **Schema Differences**: Adapts to missing metadata fields

## Performance

- **Exact Mode**: Fast (filtered query, typically <1 second)
- **Scan Mode**: Slower (enumerates IDs, ~1-10 seconds per 1000 IDs depending on network)
- **Vector Fetching**: Batched in groups of 100 for efficiency

## Related Documentation

- `memory-bank/systemPatterns.md`: Safe pagination patterns (no dummy vector pagination)
- `memory-bank/activeContext.md`: Current project status
- `PINECONE_METADATA_DICTIONARY.md`: Metadata field reference
- `src/chunking/semantic_chunker.py`: Table marker conventions

