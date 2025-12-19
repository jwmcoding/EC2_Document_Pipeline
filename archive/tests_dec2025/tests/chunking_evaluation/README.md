# Chunking Strategy Evaluation Test Artifacts

This directory contains the test scripts and data from our comprehensive chunking strategy evaluation performed on July 28, 2025.

## Files

### Test Scripts
- **`test_chunking_retrieval.py`** - Main retrieval comparison test with updated relevant queries
- **`setup_chunking_test_index.py`** - Script to create test index with documents processed using both chunking strategies

### Test Data
- **`chunking_test_discovery_20250728_161734.json`** - Discovery data for 50 test documents from `/Volumes/Jeff_2TB/2024 Deal Docs`
- **`chunking_retrieval_summary_20250728_163416.json`** - Final test results showing semantic chunking performance advantages

## Key Results
- **Semantic chunking wins** with 31% better relevance scores
- **5.5x more relevant results** with semantic chunking
- **33% faster search times** with semantic chunking
- Test validated with vendor/client-specific queries matching actual document content

## Usage
```bash
# To re-run the retrieval comparison test
cd tests/chunking_evaluation
python test_chunking_retrieval.py
```

See [../../CHUNKING_STRATEGY_EVALUATION.md](../../CHUNKING_STRATEGY_EVALUATION.md) for complete analysis and methodology. 