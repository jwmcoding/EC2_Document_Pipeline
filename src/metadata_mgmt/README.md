# Metadata Management Module

Safe and efficient tools for enhancing Pinecone metadata while concurrent operations are running.

## 🎯 Purpose

This module provides tools to enhance existing Pinecone vectors with additional metadata (specifically client and vendor names) without interfering with ongoing upsert operations. All scripts are designed with Pinecone's thread-safety and eventual consistency in mind.

## 📁 Scripts

### 1. `analyze_metadata_coverage.py` (Unified Script)
**Purpose**: Unified metadata coverage analyzer with multiple analysis modes

**✅ Consolidated Script**: This script now replaces:
- `check_vendor_metadata_status.py` → Use `--quick` mode
- `scripts/compute_metadata_coverage.py` → Use `--from-log` / `--from-json` modes
- `analyze_vendor_coverage.py` → Use `--focus vendor` mode

**Usage**:
```bash
# Quick vendor check (replaces check_vendor_metadata_status.py)
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --quick

# Basic comprehensive analysis
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25

# Larger sample for more accurate analysis
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --sample-size 2000

# Focus on vendor fields only
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --focus vendor

# Full namespace scan with CSV export
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --full-scan --csv output.csv

# Analyze documents from processing logs
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --from-log logs/processing/*.log

# Analyze documents from JSON discovery files
python src/metadata_mgmt/analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --from-json discovery.json
```

**Output**:
- Metadata field population percentages
- Client/vendor ID vs name coverage
- Enhancement potential estimates
- Unmappable ID identification
- CSV export (for full scan and log/JSON modes)

### 2. `update_client_vendor_names.py`
**Purpose**: Safely update Pinecone metadata with client/vendor names

**Usage**:
```bash
# Dry run to see what would be updated
python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25 --dry-run

# Production run with default batch size
python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25

# Custom batch size for rate limiting
python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25 --batch-size 50

# Test with limited documents
python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25 --limit 500
```

**Features**:
- ✅ **Thread-safe**: Uses Pinecone's `update()` method for metadata-only changes
- ✅ **Resume capability**: Automatically resumes from interruption points
- ✅ **Rate limiting**: 500ms delays between batches to respect concurrent operations
- ✅ **Progress tracking**: Real-time progress with ETA calculations
- ✅ **Error handling**: Exponential backoff retry logic
- ✅ **Dry run mode**: Test without making actual changes

## 🔗 Data Sources

### Client Mapping
**File**: `/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Cust-Mapping.csv`
**Format**: `Account Name, Website, Account ID, 18 Digit ID`
**Purpose**: Maps Salesforce client IDs to readable client names

### Vendor Mapping  
**File**: `/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1/SF-Vendor_mapping.csv`
**Format**: `Account Name, Website, Account ID, 18 Digit ID`
**Purpose**: Maps Salesforce vendor IDs to readable vendor names

## 🛡️ Safety Features

### Concurrent Operation Safety
- **Uses `update()` not `upsert()`**: Metadata-only changes don't interfere with vector operations
- **Exponential backoff**: Handles rate limiting gracefully
- **Batch processing**: Limits concurrent load on Pinecone
- **Rate limiting**: 500ms delays between batches

### Error Handling
- **Retry logic**: 3 attempts with exponential backoff for transient failures
- **Progress persistence**: Saves progress every 10 batches for resume capability
- **Error isolation**: Individual document failures don't stop the entire process
- **Comprehensive logging**: Detailed success/failure tracking

### Data Integrity
- **Dry run mode**: Test enhancement logic without making changes
- **Existing data preservation**: Only adds missing fields, never overwrites
- **Validation**: Checks for existing client_name/vendor_name before updating
- **Audit trail**: Complete logging of all changes made

## 📊 Expected Results

Based on analysis of the SF-Files-2020-8-15-25 namespace:

### Current State (from inspection)
- **383,132 total documents** in namespace
- **client_id**: 100% populated with Salesforce IDs
- **client_name**: 0% populated (empty/None)
- **vendor_id**: Variable population 
- **vendor_name**: 0% populated (empty/None)

### Enhancement Potential
- **Client names**: Up to 100% of documents could gain readable client names
- **Vendor names**: Depends on vendor_id population in actual data
- **Search improvement**: Enables filtering by actual company names instead of cryptic IDs

## 🚀 Workflow

### 1. Analysis Phase
```bash
python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --sample-size 2000
```
**Purpose**: Understand current state and enhancement potential

### 2. Testing Phase  
```bash
python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25 --dry-run --limit 100
```
**Purpose**: Validate enhancement logic without making changes

### 3. Production Phase
```bash
python update_client_vendor_names.py --namespace SF-Files-2020-8-15-25
```
**Purpose**: Execute full metadata enhancement

### 4. Verification Phase
```bash
python analyze_metadata_coverage.py --namespace SF-Files-2020-8-15-25 --sample-size 1000
```
**Purpose**: Confirm enhancements were applied successfully

## ⚠️ Important Notes

### Pinecone Compatibility
- **Serverless indexes only**: Uses `list()` method for document ID enumeration
- **Eventually consistent**: May take a few seconds for updates to be visible in queries
- **Thread-safe**: Safe to run while other operations are accessing the same namespace

### Resource Considerations
- **Memory usage**: Processes documents in batches to manage memory
- **API rate limits**: Built-in delays and retry logic handle Pinecone rate limits
- **Progress persistence**: Large operations can be interrupted and resumed

### Data Requirements
- **CSV mapping files**: Must be accessible at specified paths
- **Pinecone access**: Requires valid API key with write permissions
- **Namespace existence**: Target namespace must exist with documents

## 🔧 Configuration

### Environment Variables
```bash
PINECONE_API_KEY=your_pinecone_api_key
PINECONE_INDEX_NAME=business-documents  # or your index name
```

### Customization Options
- **Batch size**: Adjust `--batch-size` for rate limiting (default: 100)
- **Sample size**: Adjust `--sample-size` for analysis accuracy (default: 1000)
- **Processing limit**: Use `--limit` for testing with subset of documents
- **Dry run**: Use `--dry-run` to test without making changes

## 📈 Performance Expectations

### Analysis Script
- **Speed**: ~1000 documents/second for metadata analysis
- **Memory**: Low (streaming processing)
- **Duration**: 1-2 minutes for 1000 document sample

### Enhancement Script
- **Speed**: ~2-5 documents/second (with rate limiting and API calls)
- **Memory**: Moderate (batch processing)
- **Duration**: Variable based on namespace size and enhancement potential

### Example Timeline (383K documents)
- **Analysis**: ~5 minutes
- **Enhancement**: ~20-40 hours (depends on how many need updates)
- **Can be interrupted and resumed** at any point

This module ensures safe, efficient metadata enhancement while respecting Pinecone's operational characteristics and concurrent processing requirements.
