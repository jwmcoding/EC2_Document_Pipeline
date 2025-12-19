# Business Document Processing Pipeline - Documentation Index

**Created**: September 19, 2025  
**Archive**: `business_document_pipeline_20250919_142030.zip` (488KB)

## 🎯 Quick Start for Colleagues

### **Essential Reading (Start Here)**
1. **`README.md`** - Main project overview, setup instructions, and usage examples
2. **`memory-bank/activeContext.md`** - Current project status and latest achievements
3. **`memory-bank/systemPatterns.md`** - Critical architectural patterns and anti-patterns
4. **`memory-bank/PIPELINE_TECHNICAL_SUMMARY.md`** - 2–3 page technical overview (architecture + tech stack + parser benchmark)

### **Key Achievements & Status**
- **Vendor Metadata Population**: Currently running, 1M+ documents enhanced (97.8% success rate)
- **String Detection Bug Fixed**: Critical fix for "None" string handling in metadata validation
- **SQLite Sidecar Database**: Metadata tracking and validation system implemented
- **Multi-source Processing**: Salesforce, Dropbox, and local filesystem support

## 📚 Documentation Categories

### **🏗️ Architecture & Patterns**
- `memory-bank/systemPatterns.md` - Critical design patterns and anti-patterns
- `memory-bank/techContext.md` - Technical architecture and dependencies
- `PRODUCTION_FILE_STRUCTURE.md` - Project organization and file structure

### **🧼 Redaction (PII / NER)**
- `src/redaction/README.md` - Redaction stage overview, step-by-step logic, strict mode, and harness usage
- `tests/REDACTION_TEST_PLAN.md` - Unit test plan for redaction components and end-to-end behavior

### **🔧 Setup & Configuration**
- `README.md` - Main setup guide with source configuration
- `requirements.txt` - Python dependencies
- `config/` - Configuration files and logging setup
- `OLLAMA_SETUP_GUIDE.md` - Local LLM setup (optional)

### **📊 Processing Workflows**
- `SALESFORCE_PROCESSING_WORKFLOW.md` - Salesforce-specific processing guide
- `RAW_SALESFORCE_EXPORT_FORMAT.md` - Raw Salesforce export structure and requirements
- `FILE_PARSING_REFERENCE.md` - Comprehensive file parsing tools and techniques
- `../DOCLING_COMPARISON.md` - IBM Granite Docling vs current pipeline analysis
- `LOCAL_FILESYSTEM_MIGRATION_PLAN.md` - Local filesystem processing
- `BATCH_PROCESSING_GUIDE.md` - Batch API integration for cost savings

### **🔍 Search & Intelligence**
- `HYDE_IMPLEMENTATION_GUIDE.md` - Advanced search with hypothetical document embedding
- `CHUNKING_STRATEGY_EVALUATION.md` - Text chunking strategies
- `streamlit-ui/README.md` - Web interface for document search

### **📈 Metadata Management**
- `src/metadata_mgmt/README.md` - Metadata analysis and correction tools
- `METADATA_OPTIMIZATION_ANALYSIS.md` - Metadata schema optimization
- `ENHANCED_METADATA_ENRICHMENT_GUIDE.md` - LLM-powered metadata enhancement
- `memory-bank/NPI_DEAL_DATA__sf-export-aug15-2025__METADATA_DICTIONARY.md` - Field dictionary for `npi-deal-data` / `sf-export-aug15-2025`

### **🧪 Testing & Validation**
- `tests/` - Comprehensive test suite
- `tests/chunking_evaluation/README.md` - Chunking strategy testing
- `tests/SALESFORCE_EXPORT_VALIDATION.md` - Salesforce export format validation
- `tests/TEST_LENIENCY_ANALYSIS.md` - Integration test leniency analysis
- `tests/README_INTEGRATION_TESTS.md` - Integration test running guide

## 🎉 Recent Breakthroughs (September 2025)

### **Critical Bug Fix: String Detection**
- **Problem**: `vendor_id: "None"` strings treated as populated data
- **Impact**: 99.6% of documents incorrectly skipped
- **Solution**: Explicit string validation for "None", "nan", empty values
- **Result**: 1M+ documents now receiving proper vendor metadata

### **SQLite Sidecar Database**
- **Purpose**: Track metadata coverage across processing runs
- **Benefits**: Before/after validation, progress monitoring, quality assurance
- **Location**: External database for metadata statistics and analysis

### **Production Scale Validation**
- **Namespace Size**: 1.26M documents (vs 460K estimated)
- **Processing Rate**: 775 docs/minute sustained
- **Success Rate**: 97.8% vendor population success
- **Coverage Improvement**: 1.85% → 81%+ (and growing)

## 🚀 Current Status

**Vendor Population Script**: ✅ **ACTIVE**
- **Progress**: 1,052,900 / 1,264,723 documents (83.2% complete)
- **Vendor IDs Added**: 1,029,513+ (massive improvement)
- **ETA**: ~4.6 hours remaining
- **Expected Final Coverage**: 98%+ vendor metadata population

## 💡 Key Insights for Colleagues

1. **Source Configuration Critical**: Always use `--source salesforce` for organized Salesforce files or `--source salesforce_raw` for raw exports (not `--source local`)
2. **String Validation Pattern**: Check for "None" strings explicitly in metadata validation
3. **SQLite Monitoring**: Use sidecar database for metadata health tracking
4. **Batch Processing**: 50% cost savings with OpenAI Batch API integration
5. **Resume Capability**: All scripts support interruption and continuation

## 📞 Support & Questions

For questions about this pipeline:
- Review the memory bank files for current context
- Check system patterns for architectural decisions
- Refer to README.md for setup and usage examples
- Test scripts are available in `tests/` directory

**Last Updated**: September 19, 2025 - During active vendor metadata population

