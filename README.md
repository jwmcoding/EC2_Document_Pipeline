# Business Document Processing Pipeline

An intelligent document processing pipeline that transforms business documents into searchable intelligence using **GPT-4.1-mini** with **50% cost savings** through OpenAI's Batch API.

## 🎯 Key Features

- **Multi-Source Discovery**: Support for Dropbox, Salesforce exports, and local filesystem
- **Business Intelligence**: Automated content summaries, pricing analysis, and contract term extraction
- **Parallel Processing**: 8x faster processing with configurable workers
- **Batch API Integration**: 50% cost savings on LLM classification
- **Advanced Search**: Filter by pricing complexity, contract dates, vendor products
- **Streamlit Business Intelligence**: Professional web interface for document search and analysis
- **LangSmith User Analytics**: Optional user interaction tracing for search behavior insights

## 🧠 Two-Phase Classification System

The pipeline provides **two types** of metadata classification:

### **Phase 1: Fast Discovery (Free)**
- **Source**: Filename/path analysis  
- **Cost**: $0.00 (no LLM calls)
- **Example**: `45492-FMV-Adobe.pdf` → `{vendor: "Adobe", doc_type: "FMV", deal_number: "45492"}`

### **Phase 2: Enhanced Intelligence (Batch API)**
- **Source**: Full document content analysis by GPT-4.1-mini
- **Cost**: 50% savings with batch processing
- **Example**: Rich business intelligence with content summaries, pricing analysis, contract terms

```json
{
  "document_type": "Quote/Proposal",
  "confidence": 0.94,
  "product_pricing_depth": "medium",
  "commercial_terms_depth": "low"
}
```

## 📂 Document Source Configuration

The pipeline supports three different document sources with **different metadata capabilities**:

### 🔧 **Source Selection Guide**

| Source Type | Use When | Metadata Fields | vendor_id Populated |
|-------------|----------|-----------------|-------------------|
| `--source salesforce` | ✅ **Organized Salesforce files** | **37+ rich fields** | ✅ **YES** |
| `--source salesforce_raw` | ✅ **Raw Salesforce export bundles** | **37+ rich fields** | ✅ **YES** |
| `--source dropbox` | Dropbox folders | **6 basic fields** | ❌ No |
| `--source local` | Local directories | **6 basic fields** | ❌ No |

### ✅ **Salesforce Source (Organized Files)**

For pre-organized Salesforce files with mapping CSVs:

```bash
# ✅ CORRECT - Full business intelligence with vendor/client data
# Example using August 2025 Salesforce export (organized structure)
AUGUST_CSV="/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1"

python discover_documents.py --source salesforce \
  --salesforce-files-dir /Volumes/Jeff_2TB/organized_salesforce_v2 \
  --file-mapping-csv organized_files_to_deal_mapping_enhanced.csv \
  --deal-metadata-csv $AUGUST_CSV/Deal__c.csv \
  --client-mapping-csv $AUGUST_CSV/SF-Cust-Mapping.csv \
  --vendor-mapping-csv $AUGUST_CSV/SF-Vendor_mapping.csv \
  --require-deal-association \
  --output production_august_discovery.json
```

**Key Arguments for Salesforce source:**
- `--salesforce-files-dir`: Path to organized files directory (with emails/, documents/, etc. subfolders)
- `--file-mapping-csv`: Maps organized files to deal IDs (in workspace, not on external drive)
- `--deal-metadata-csv`: Salesforce Deal__c.csv with all deal metadata
- `--client-mapping-csv`: Maps client IDs to names (SF-Cust-Mapping.csv)
- `--vendor-mapping-csv`: Maps vendor IDs to names (SF-Vendor_mapping.csv)
- `--require-deal-association`: Only include files with valid deal associations (recommended)

### ✅ **Raw Salesforce Export Source (Differential Exports)**

For raw Salesforce differential export bundles (new format with **merged financial data**):

```bash
# ✅ CORRECT - Use salesforce_raw source with CSV files for full metadata enrichment
python discover_documents.py --source salesforce_raw \
  --export-root-dir "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771" \
  --content-versions-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/content_versions.csv" \
  --content-documents-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/content_documents.csv" \
  --content-document-links-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/content_document_links.csv" \
  --deal-metadata-csv "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771/deal__cs.csv" \
  --client-mapping-csv "/path/to/SF-Cust-Mapping.csv" \
  --vendor-mapping-csv "/path/to/SF-Vendor_mapping.csv" \
  --deal-mapping-csv "organized_files_to_deal_mapping.csv" \
  --require-deal-association \
  --output raw_salesforce_discovery.json
```

**⚠️ CRITICAL: Do NOT use `--source local` for Salesforce exports!**

```bash
# ❌ WRONG - Will find files but NO metadata enrichment
python discover_documents.py --source local \
  --path "/Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771" \
  --output discovery.json

# Result: All metadata fields will be NULL (deal_id, vendor_id, client_id, etc.)
# Even though files physically exist in Salesforce export structure!
```

**Why?** The `local` source only scans the filesystem - it has **no knowledge of Salesforce relationships**. You MUST use `salesforce_raw` with CSV files to get deal associations and metadata enrichment.

**Key Arguments**:
- `--export-root-dir` - **REQUIRED** Path to Salesforce export root (contains CSV files and ContentVersions/ directory)
- `--content-versions-csv` - **REQUIRED** Maps ContentVersion IDs to ContentDocument IDs and filenames
- `--content-documents-csv` - **REQUIRED** Document metadata
- `--content-document-links-csv` - **REQUIRED** Links ContentDocuments to Deal__c records (the mapping!)
- `--deal-metadata-csv` - **REQUIRED** Deal business metadata (deal__cs.csv from export)
- `--require-deal-association` - **(RECOMMENDED)** Only include files with valid deal associations
  - ✅ With flag: Only files linked to deals (all metadata populated)
  - ⚠️ Without flag: All files included (some will have null metadata)

**📊 Understanding `mapping_status` Values**:
When you review discovery output, check the `mapping_status` field:
- `"mapped"` - ✅ File linked to deal, all metadata enriched
- `"mapped_no_metadata"` - ⚠️ File found but no deal association (all metadata NULL)
- Use `--require-deal-association` to exclude `mapped_no_metadata` files

**📊 Merged Financial Data**:
The `deal_merged_financial_data.csv` file contains 41,371 deals with complete financial metrics merged from:
- **Newer export** (Dec 2, 2025): 41,371 deals, including 4,568 new deals
- **Older export** (Aug 21, 2025): Financial data for 36,803 overlapping deals

This ensures all 8 financial fields are populated (newer exports lack these fields).

**What You'll See**:
```
📊 SALESFORCE EXPORT STATISTICS
==================================================================
📁 Files:
   Total files in export: 179,505
   Files with deal associations: 142,280 (79.3%)
   Files without deal associations: 37,225
   Files with user-friendly deal IDs: 23,670

🤝 Deals:
   Unique deals referenced: 15,234
   Deals with full metadata: 15,234
   Total deal records in CSV: 41,371

🔗 Mappings:
   Deal ID to number mappings: 23,670
   ContentVersion to Deal links: 179,505
==================================================================
```

**Raw Export Structure:**
- CSV files: `content_versions.csv`, `content_documents.csv`, `content_document_links.csv`, `deal__cs.csv` (or use merged `deal_merged_financial_data.csv`)
- File payloads (in priority order):
  1. **PRIMARY**: `ContentVersions/VersionData/<ContentVersionId>/` (97% of files)
  2. Deal-specific: `Deal__cs/<exportId>/<DealId>/` directories  
  3. Legacy attachments: `Attachments/Body/<ContentDocumentId>` files

**🚀 Memory-Safe Processing** (for large exports):
- The default `RawSalesforceExportConnector` uses pandas and may crash on very large CSV files (>100MB)
- For large exports, use `PureStreamingConnector` which uses:
  - **SQLite-backed deal indexing** (disk-based, zero memory overhead)
  - **Pure CSV streaming** (no pandas for large files)
  - Successfully processes 41,371 deals + 179,505 content versions without crashes
- See `src/connectors/raw_salesforce_export_connector_pure_streaming.py` for implementation

**Rich Metadata Populated (27 fields - 57% reduction!):**
- **Core**: `file_name`, `file_type`, `deal_creation_date` (for time-based filtering)
- **Financial**: `proposed_amount`, `final_amount`, `savings_1yr`, `savings_3yr`, `savings_target`, `savings_achieved`, `savings_target_full_term`
- **Deal Context**: `deal_id`, `salesforce_deal_id`, `deal_subject`, `deal_status`, `deal_reason`, `deal_start_date`
- **Relationships**: `client_id`, `client_name`, `salesforce_client_id`, `vendor_id`, `vendor_name`, `salesforce_vendor_id`
- **Narratives**: `current_narrative`, `customer_comments`
- **Contracts**: `contract_term`, `effort_level`, `deal_origin`
- **Email**: `email_subject`, `email_has_attachments` (only for .msg files)
- **Technical**: `chunk_index`

**Dual ID Storage for All Key Entities** (for flexibility):
- `deal_id`: User-friendly format (e.g., `"58773"`) — **for filtering/display**
- `salesforce_deal_id`: Raw Salesforce ID (e.g., `"a0WQg000001QKH3MAO"`) — **for lookups/tracing**
- `client_id`: Friendly name or identifier — **for filtering/display**
- `salesforce_client_id`: Raw Salesforce ID — **for lookups/tracing**
- `vendor_id`: Friendly name or identifier — **for filtering/display**
- `salesforce_vendor_id`: Raw Salesforce ID — **for lookups/tracing**

**Removed Fields:**
- Redundant: `namespace`, `chunk_length`, `text`
- PII: `email_sender`, `email_recipients_to`, `email_date`
- AI-generated: `document_type`, `document_type_confidence`, `classification_reasoning`, `classification_method`, `classification_tokens_used`, `product_pricing_depth`, `commercial_terms_depth`, `proposed_term_start`, `proposed_term_end`

### 📁 **Dropbox Source**

```bash
# For Dropbox business folders
python discover_documents.py --source dropbox \
  --folder "/Business Documents/FMV Reports" \
  --output dropbox_discovery.json
```

### 🗂️ **Local Filesystem Source**

**Use `--source local` when**:
- ✅ Files are NOT from Salesforce (personal documents, downloads, external sources)
- ✅ Quick exploration/testing (count files, check types, no metadata needed)
- ✅ Content-only processing (full-text search without business filtering)
- ✅ Salesforce CSVs don't exist or are incomplete

**Smart Detection**: If you accidentally use `--source local` on a Salesforce export, you'll see a warning:
```
⚠️  SALESFORCE EXPORT DETECTED!
This directory contains Salesforce CSV files and structure.
Using --source local will result in NULL METADATA.
✅ RECOMMENDED: Use --source salesforce_raw for full enrichment
```

**Example** (Non-Salesforce files):
```bash
# For local directory processing (NOT Salesforce)
python discover_documents.py --source local \
  --path "/Users/username/Documents/Business" \
  --output local_discovery.json
```

### ⚠️ **Critical Configuration Warning**

**NEVER use `--source local` for Salesforce files!** This will miss 30+ metadata fields:

```bash
# ❌ WRONG - Misses vendor_id, deal data, financial metrics
python discover_documents.py --source local --path "/organized_salesforce_v2"

# ✅ CORRECT - Gets full Salesforce metadata
python discover_documents.py --source salesforce --salesforce-files-dir "/organized_salesforce_v2" [...]
```

## 🔧 Vendor Metadata Assessment & Correction

### **Problem: Missing Vendor Metadata in Existing Namespaces**

If you have an existing namespace with missing vendor information (common when documents were processed with wrong source configuration), you can assess and correct the metadata:

#### **Step 1: Assess Current Coverage**
```bash
# Analyze metadata coverage with SQLite tracking
python src/metadata_mgmt/analyze_metadata_coverage.py \
  --namespace SF-Files-2020-8-15-25 \
  --full-scan \
  --csv output/metadata_coverage.csv

# Quick vendor status check
python src/metadata_mgmt/analyze_metadata_coverage.py \
  --namespace SF-Files-2020-8-15-25 \
  --quick

# Or vendor-focused comprehensive analysis
python src/metadata_mgmt/analyze_metadata_coverage.py \
  --namespace SF-Files-2020-8-15-25 \
  --focus vendor \
  --sample-size 5000
```

#### **Step 2: Identify String Detection Issues**
**Common Issue**: Documents may have `vendor_id: "None"` as STRING values (not null), which naive detection treats as populated.

```python
# ❌ BROKEN: Treats "None" strings as populated
if vendor_id and str(vendor_id).strip():
    # "None".strip() is truthy - skips document incorrectly

# ✅ CORRECT: Explicit string value validation  
vendor_id_clean = str(vendor_id).strip() if vendor_id else ''
if vendor_id_clean and vendor_id_clean.lower() not in ['none', 'nan', '']:
    # Properly detects "None" strings as missing data
```

#### **Step 3: Execute Vendor Population**
```bash
# Test the population logic first
python src/metadata_mgmt/populate_vendor_ids_fixed.py \
  --namespace SF-Files-2020-8-15-25 \
  --analyze-only

# Execute actual population (requires Deal__c.csv and vendor mapping files)
python src/metadata_mgmt/populate_vendor_ids_fixed.py \
  --namespace SF-Files-2020-8-15-25 \
  --batch-size 100
```

#### **Step 4: Validate Results**
```bash
# Re-run coverage analysis to confirm improvement
python src/metadata_mgmt/analyze_metadata_coverage.py \
  --namespace SF-Files-2020-8-15-25 \
  --full-scan \
  --csv output/metadata_coverage_after.csv

# Compare before/after in SQLite database
sqlite3 output/metadata_stats.sqlite "
SELECT 'BEFORE' as phase, populated_count, blank_count 
FROM field_stats 
WHERE field_name='vendor_id' AND run_id='[BEFORE_RUN_ID]'
UNION ALL
SELECT 'AFTER' as phase, populated_count, blank_count 
FROM field_stats 
WHERE field_name='vendor_id' AND run_id='[AFTER_RUN_ID]';
"
```

### **Expected Results**
- **Coverage Improvement**: 1.85% → 98%+ vendor_id/vendor_name population
- **Documents Enhanced**: ~1.24M documents receiving proper vendor metadata  
- **Streamlit UI**: Restored vendor/client filtering functionality
- **Search Quality**: Improved business document discovery and analysis

### **Required Files for Vendor Population**
- `Deal__c.csv` - Salesforce deal metadata with vendor relationships
- `SF-Vendor_mapping.csv` - Vendor ID to name mapping
- `organized_files_to_deal_mapping_enhanced.csv` - File to deal relationships

## 💰 Multi-Stage Architecture & Cost Optimization

### **Recommended Workflow: Multi-Stage Processing**

| Stage | Purpose | Cost | Speed | Result |
|-------|---------|------|-------|---------|
| **Discovery** | Fast file enumeration + basic metadata | $0.00 | Instant | Ready for processing |
| **Processing** | Content parsing + Pinecone upload | ~$0.05 | 8x faster (parallel) | ✅ Immediately searchable |
| **Enhancement** | Batch LLM intelligence | 50% savings | Within 24h | ✅ Rich business analytics |

### **Performance & Cost Example (886 Documents)**
- **Total Cost**: $0.33 (~$0.0004 per document)
- **Total Time**: ~30 minutes
- **Processing Speed**: Up to 3.2 docs/sec with 8 workers
- **System Requirements**: 8+ CPU cores recommended

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone and setup
git clone <repository>
cd <project-directory>

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.template .env
# Edit .env with your API keys:
# DROPBOX_ACCESS_TOKEN=your_dropbox_token
# OPENAI_API_KEY=your_openai_key
# PINECONE_API_KEY=your_pinecone_key
```

### 2. Business Intelligence Interface (Query Processed Documents) 🚀

If you have already processed documents, start the Streamlit interface to search and analyze them:

```bash
# Navigate to Streamlit UI directory
cd streamlit-ui

# Ensure main venv is activated (should see "(venv)" prefix)
source ../venv/bin/activate

# Start Streamlit server  
streamlit run app_standalone.py --server.port 8501

# Access at: http://localhost:8501
```

**✅ Professional Features:**
- **Business-Ready Interface**: Professional document search and analysis platform
- **Large Query Input**: Multi-line text area with example query buttons
- **Smart Filtering**: Vendor, client, document type, and year filters in sidebar
- **Source Transparency**: Expandable document cards with metadata and content previews

### 3. Multi-Stage Processing Workflow

#### Stage 1: Fast Discovery (Free)
```bash
# Interactive discovery
python discover_documents.py

# Direct discovery command
python discover_documents.py --source local --path "/docs" --output discovery.json
```

#### Stage 2: Document Processing
```bash
# Make documents immediately searchable with basic metadata
python process_discovered_documents.py --input discovery.json --parallel --workers 8

# Optional: Collect enhanced LLM metadata (batch processing for 50% savings)
python process_discovered_documents.py --input discovery.json --batch-only --use-batch
```

#### Stage 3: Monitor & Apply Batch Enhancement
```bash
# Monitor batch job completion
python batch_results_checker.py --job-id batch_abc123 --monitor --update

# Apply enhanced metadata to existing documents
python process_discovered_documents.py --input discovery.json --reprocess
```

## 📋 Usage Examples

### Discovery Options
```bash
# Dropbox source
python discover_documents.py --source dropbox --folder "/2024 Deal Docs"

# Local filesystem with testing limit
python discover_documents.py --source local --path "/docs" --max-docs 100

# View detailed summary of existing discovery file (without re-running discovery)
python discover_documents.py --source local --path . --output existing_discovery.json --show-summary
```

### Discovery Summary Output

After discovery completes (or with `--show-summary`), you'll see a comprehensive analysis:

```
======================================================================
📊 DISCOVERY SUMMARY
======================================================================
📂 Source: salesforce_raw
📁 Path: /Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1
📄 Total documents: 147,000

📅 DATE RANGE ANALYSIS:
   Earliest year: 2015
   Latest year: 2025
   Pre-2000 documents: 0
   Year 2000+: 147,000

📆 DOCUMENTS BY YEAR:
   2023:  45,234 ██████████████████████████████████████████████████
   2022:  38,123 ████████████████████████████████████████
   ...

📁 FILE TYPES:
   .pdf            58,000 ( 39.5%)
   .xlsx           35,000 ( 23.8%)
   .msg            28,000 ( 19.0%)
   .docx           22,000 ( 15.0%)

💾 SIZE STATISTICS:
   Total size: 125.4 GB
   Average size: 0.87 MB
   Files > 10MB: 2,340
======================================================================
```

### Processing Options
```bash
# Basic processing (parallel for speed)
python process_discovered_documents.py --input discovery.json --parallel --workers 8

# With filters for specific documents
python process_discovered_documents.py --input discovery.json --filter-vendor "Microsoft,Adobe"

# Resume interrupted processing
python process_discovered_documents.py --input discovery.json --resume
```

## 🔍 Batch Job Monitoring

```bash
# Check batch job status
python batch_results_checker.py --job-id batch_abc123 --monitor

# Update completed jobs automatically
python batch_results_checker.py --job-id batch_abc123 --discovery-file discovery.json --update
```

## 📊 Advanced Search Capabilities

With enhanced metadata processing, you can search using rich business intelligence:

### Search by Business Intelligence
```python
# Find documents with detailed pricing information
results = search_documents(
    query="laptop pricing", 
    filter={"product_pricing_depth": "high"}
)

# Search by contract date ranges
results = search_documents(
    query="2024 contracts", 
    filter={
        "proposed_term_start": {"$gte": "2024-01-01"},
        "proposed_term_end": {"$lte": "2024-12-31"}
    }
)

# Focus on normalized business filters (document_type, depths, dates)
results = search_documents(
    query="Cisco equipment", 
    filter={"document_type": {"$eq": "Technical Specification"}}
)
```

## 📈 Complete Workflow Examples

### Production Workflow: Multi-Stage Processing
```bash
# 1. Fast discovery (free)
python discover_documents.py --source local --path "/business_docs" --output discovery.json

# 2. Make documents searchable immediately  
python process_discovered_documents.py --input discovery.json --parallel --workers 8

# 3. Optional: Add enhanced intelligence via batch API (50% savings)
python process_discovered_documents.py --input discovery.json --batch-only --use-batch
python batch_results_checker.py --job-id batch_abc123 --monitor --update
python process_discovered_documents.py --input discovery.json --reprocess
```

### Targeted Analysis Examples
```bash
# Contract compliance monitoring
python discover_documents.py --source dropbox --folder "/contracts" --output contracts.json
python process_discovered_documents.py --input contracts.json --filter-type "Contract/SoW/MSA"

# Vendor pricing intelligence  
python discover_documents.py --source local --path "/vendor_docs" --output vendors.json
python process_discovered_documents.py --input vendors.json --filter-vendor "Microsoft,Adobe,Oracle"
```

## 🎯 Document Types & Intelligence Levels

### Supported Document Types
- **Email** - Communications with metadata extraction
- **FMV Report** - Fair Market Value with pricing intelligence  
- **Quote/Proposal** - Sales documents with pricing and terms analysis
- **Contract/SoW/MSA** - Contracts with date extraction and terms complexity
- **Product Literature** - Marketing materials with product intelligence

### Intelligence Extraction Levels
- **Pricing Depth**: LOW (basic mentions) → MEDIUM (specific prices) → HIGH (detailed tables)
- **Terms Depth**: LOW (standard terms) → MEDIUM (specific terms) → HIGH (detailed conditions)

## ⚙️ Configuration Options

### Discovery Command Line Arguments

> **Note**: Discovery phase ALWAYS extracts **simple metadata** (filename/path-based). The `--classify` flag only affects whether **enhanced LLM metadata** collection is prepared.

| Argument | Description | Metadata Type | Example |
|----------|-------------|---------------|---------|
| `--source` | Document source (dropbox/local) | Always: Simple | `--source local` |
| `--folder` | Dropbox folder path | Always: Simple | `--folder "/2024 Deal Docs"` |
| `--path` | Local filesystem path | Always: Simple | `--path "/Users/docs"` |
| `--classify` | **Prepare for enhanced LLM metadata** | Setup: Enhanced | `--classify` |
| `--use-batch` | Use batch API for LLM (50% savings) | Enhanced only | `--use-batch` |
| `--max-docs` | Limit for testing | Both types | `--max-docs 100` |
| `--output` | Output JSON file | Both types | `--output results.json` |

### Processing Command Line Arguments

> **Note**: Processing phase creates document chunks with **simple metadata** embedded. The `--use-batch` flag controls whether **enhanced LLM metadata** is collected.

| Argument | Description | Metadata Type | Example |
|----------|-------------|---------------|---------|
| `--input` | Discovery JSON file (contains simple metadata) | Both types | `--input discovery.json` |
| `--limit` | Maximum documents to process | Both types | `--limit 100` |
| `--filter-type` | Filter by document types (simple metadata) | Simple filtering | `--filter-type "FMV,IDD"` |
| `--filter-vendor` | Filter by vendor names (simple metadata) | Simple filtering | `--filter-vendor "Microsoft,Adobe"` |
| `--deal-created-after` | **Filter by deal creation date** (from Salesforce) | Date filtering | `--deal-created-after 2000-01-01` |
| `--deal-created-before` | Filter by deal creation date (upper bound) | Date filtering | `--deal-created-before 2024-12-31` |
| `--modified-after` | Filter by file modified date (less reliable) | Date filtering | `--modified-after 2020-01-01` |
| `--reprocess` | Reprocess all documents | Both types | `--reprocess` |
| `--resume` | Resume interrupted processing | Both types | `--resume` |
| `--parallel` | Enable parallel processing | Both types | `--parallel` |
| `--workers` | Number of parallel workers | Both types | `--workers 8` |
| `--use-batch` | **Collect enhanced LLM metadata** (50% savings) | Enhanced only | `--use-batch` |
| `--batch-only` | Collect batch requests without document processing | Enhanced only | `--batch-only` |
| `--chunking-strategy` | Choose chunking strategy for enhanced metadata (business_aware, semantic) | Enhanced only | `--chunking-strategy semantic` |

### Date Filtering (Important!)

**⚠️ Recommended**: Use `--deal-created-after` instead of `--modified-after` for reliable date filtering.

- **`--deal-created-after`**: Uses `deal_creation_date` from Salesforce `Deal__c.csv` (authoritative business date)
- **`--modified-after`**: Uses file `modified_time` from disk (unreliable - files may have been copied/moved)

```bash
# Filter to only process documents from deals created in 2000 or later
python process_discovered_documents.py \
  --input discovery.json \
  --deal-created-after 2000-01-01 \
  --namespace production \
  --parser-backend mistral \
  --parallel --workers 8
```

See [CHUNKING_STRATEGY_EVALUATION.md](./CHUNKING_STRATEGY_EVALUATION.md) for detailed comparison and [LANGCHAIN_CONFIGURATION_RESULTS.md](./LANGCHAIN_CONFIGURATION_RESULTS.md) for LangChain parameter optimization results.

### Environment Variables

```bash
# Required
DROPBOX_ACCESS_TOKEN=your_dropbox_token_here
OPENAI_API_KEY=your_openai_api_key_here  
PINECONE_API_KEY=your_pinecone_api_key_here

# Optional
PINECONE_INDEX_NAME=business-documents
LOG_LEVEL=INFO
```

## 📊 Enhanced Logging & Monitoring

### Structured Logging Architecture
```
logs/
├── progress/latest_completion_summary.txt    # Instant results from any operation
├── progress/*_progress_*.log                # Real-time progress updates
├── processing/*_SUMMARY.txt                # Final operation summaries
└── system/errors_*.log                     # Daily error aggregation
```

### Quick Monitoring Commands
```bash
# Get instant results from any operation
cat logs/progress/latest_completion_summary.txt

# Monitor active operations in real-time
tail -f logs/progress/*_progress_*.log

# Check today's errors
cat logs/system/errors_$(date +%Y%m%d).log
```

### Real-Time Monitoring Example
```bash
# Terminal 1: Start processing
python process_discovered_documents.py --input discovery.json --parallel --workers 8

# Terminal 2: Monitor progress
tail -f logs/progress/*_progress_*.log
# Output: 📈 Progress: 100/1000 (10.0%) | Rate: 25.3 docs/min | ETA: 35m 42s
```

### Enhanced Processing Summary

At the end of each processing run, you'll see comprehensive statistics:

```
============================================================
🎉 PROCESSING COMPLETE
============================================================
📊 Documents processed: 1,000
❌ Documents failed: 12
🧩 Total chunks created: 8,450
⏱️  Total time: 45.2 minutes (0.75 hours)
📈 Throughput: 22.1 docs/minute
📈 Average: 2.71s per document

📁 FILE TYPE BREAKDOWN:
   Type       Count  Success   Failed   Avg Time   Chunks
   ---------- ------ -------- ------- ---------- --------
   .pdf          450      445       5      4.52s     3,850
   .docx         300      298       2      0.87s     2,100
   .xlsx         150      149       1      1.23s     1,200
   .msg          100       96       4      0.65s     1,300

⚠️ ERROR BREAKDOWN:
   timeout: 5
   no_text_extracted: 4
   download_failed: 3

⏱️  PROCESSING TIME DISTRIBUTION:
   Fastest:   0.45s
   P50 (med): 2.34s
   P90:       5.67s
   P99:       12.45s
   Slowest:   18.23s
============================================================
```

## 🔧 Troubleshooting

### Common Issues

#### Discovery `source_path` Missing (Critical!)

**Error:**
```
ValueError: Discovery file missing source_path in metadata and could not be inferred.
```

**Cause:** When using `--source salesforce`, the discovery script may not populate `source_path` in the metadata.

**Fix:** Manually update the discovery JSON after discovery completes:
```python
import json
with open('your_discovery.json', 'r') as f:
    d = json.load(f)
d['discovery_metadata']['source_path'] = '/Volumes/Jeff_2TB/organized_salesforce_v2'
d['discovery_metadata']['salesforce_files_dir'] = '/Volumes/Jeff_2TB/organized_salesforce_v2'
with open('your_discovery.json', 'w') as f:
    json.dump(d, f, indent=2, default=str)
```

Or set environment variable before processing:
```bash
export SALESFORCE_EXPORT_ROOT=/Volumes/Jeff_2TB/organized_salesforce_v2
```

#### File Mapping CSV Location

**⚠️ Important:** The `organized_files_to_deal_mapping_enhanced.csv` file is in the **workspace directory**, not on the external drive!

```bash
# ✅ CORRECT - relative path (file is in workspace)
--file-mapping-csv organized_files_to_deal_mapping_enhanced.csv

# ❌ WRONG - file doesn't exist on external drive
--file-mapping-csv /Volumes/Jeff_2TB/organized_salesforce_v2/organized_files_to_deal_mapping_enhanced.csv
```

#### Processing Order (Not Alphabetical!)

**Q:** Why aren't documents processed in alphabetical folder order?

**A:** Discovery uses `os.walk()` which returns directories in **filesystem order** (order created on disk), not alphabetical. For example, you might see:
1. `images/` → 2. `emails/` → 3. `spreadsheets/` → 4. `documents/`

This means PDFs in `documents/` folder might not process until 40%+ through the run.

To check the actual order in your discovery file:
```bash
python -c "
import json
d = json.load(open('your_discovery.json'))
folders = {}
for i, doc in enumerate(d['documents']):
    folder = doc.get('file_info', {}).get('path', '').split('/')[0]
    if folder not in folders:
        folders[folder] = i + 1
        print(f'Doc #{i+1}: {folder}/')
"
```

#### Discovery Problems
```bash
# Test discovery with limits
python discover_documents.py --source local --path "/your/path" --max-docs 10

# Verify path exists
ls -la "/your/path"
```

#### Processing Issues
```bash
# Check discovery file
python -c "import json; print(len(json.load(open('discovery.json'))['documents']))"

# Test connections
python -c "from openai import OpenAI; print('OpenAI OK')"
python -c "from src.connectors.pinecone_client import PineconeDocumentClient; print('Pinecone OK')"
```

#### Batch Job Issues
```bash
# Check job status
python batch_results_checker.py --job-id your_job_id

# Verify batch results applied
python -c "
import json
doc = json.load(open('discovery.json'))['documents'][0]
print('✅ Enhanced metadata found' if 'llm_classification' in doc else '❌ No enhanced metadata')
"
```

### Performance Tips
- Use `--limit 10` for testing
- Use `--parallel --workers 8` for speed
- Use batch API for 50% cost savings on large datasets

## 🤝 Support & Next Steps

### Getting Help
1. Check logs: `cat logs/progress/latest_completion_summary.txt`
2. Verify environment variables are set correctly  
3. Test with small datasets first (`--limit 10`)
4. Review troubleshooting section above

### What You Can Do After Processing
- **Advanced Search**: Use business intelligence filters in your queries
- **Compliance Tracking**: Monitor contract dates and terms complexity  
- **Vendor Analysis**: Analyze pricing strategies and product portfolios
- **Business Intelligence Interface**: Search and analyze documents through the professional Streamlit platform
- **User Analytics**: Track search patterns and system performance with LangSmith (see setup below)

## 📊 LangSmith User Analytics (Optional)

Track user interactions and search behavior in your Streamlit interface with LangSmith integration.

### 🎯 What Gets Tracked
- **User search queries** and AI-generated responses
- **Search performance** metrics (response time, documents found)
- **Filter usage** and session patterns
- **System performance** trends and confidence scores

### 🚀 Quick Setup
```bash
# 1. Install LangSmith (already included in requirements.txt)
pip install langsmith

# 2. Get API key from https://langchain.com/langsmith

# 3. Add to your .env file:
LANGSMITH_API_KEY=your_api_key_here
LANGSMITH_PROJECT=business-document-search
LANGSMITH_ENABLED=true
```

### 📈 Dashboard Insights
Once enabled, your LangSmith dashboard shows:
- **Popular search topics** and query patterns
- **System response times** and success rates
- **User session behavior** and feature usage
- **Search quality metrics** and document relevance

### 🔒 Privacy & Performance
- ✅ **Non-intrusive**: Tracing errors never break user experience
- ✅ **Focused scope**: Only user interactions, not internal processing
- ✅ **Optional**: Completely configurable and can be disabled anytime
- ✅ **Lightweight**: Minimal performance overhead

**Note**: LangSmith tracing is entirely optional. The system works perfectly without it.

---

**Ready to get started?** Begin with the multi-stage workflow:

```bash
# 1. Fast discovery (free)
python discover_documents.py --source local --path "/your_docs"

# 2. Make documents searchable  
python process_discovered_documents.py --input discovery.json --parallel --workers 8

# 3. Start the business intelligence interface
cd streamlit-ui && streamlit run app_standalone.py --server.port 8501
``` 