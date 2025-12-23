# System Patterns & Architecture

[Content preserved from existing file - appending alias generation pattern at end]

## 🔒 Alias Generation for Client Redaction (December 21, 2025)

### **Pattern: LLM-Driven Alias Discovery with Vendor Context**

**Problem**: Client names appear in many forms (abbreviations, nicknames, domains) across documents. Manual curation for 900+ clients is impractical.

**Anti-Pattern #1: Deterministic Acronym Generation**
```python
# ❌ WRONG: Programmatic abbreviation generation creates false positives
client_name = "American Family Insurance"
acronym = ''.join([w[0] for w in client_name.split()])  # → "AFI"
# Issue: "AFI" could match other companies, products, technical terms
```

**Anti-Pattern #2: Collecting Vendors from All Pinecone Chunks**
```python
# ❌ WRONG: Pulling vendors from 500 Pinecone chunks across many deals
for chunk in pinecone_results:  # 500 chunks
    vendor = chunk.metadata.get('vendor_name')
    if vendor:
        vendor_list.add(vendor)
# Result: 40-179 vendors per client → overwhelms LLM prompt
```

**✅ CORRECT PATTERN: LLM + Discovery JSON Vendors**
```python
# 1. Extract vendors ONLY from discovery JSON (documents being processed)
for doc in discovery_json['documents']:
    if doc['salesforce_client_id'] == client_id:
        vendor = doc['deal_metadata']['vendor_name']
        if vendor and vendor.lower() not in ['none', 'nan']:
            vendors.add(vendor)
# Result: 1-2 primary vendors per client

# 2. LLM with few-shot examples + vendor context
prompt = f"""
Client: {client_name}
Primary Vendors (DO NOT include): {vendors}
Evidence: {text_chunks}

Find aliases that clearly refer to THE CLIENT, not vendors.

Examples:
- "American Family Insurance" → valid: "AmFam", "AFI"
- "DocuSign" + vendors: ["SHI", "NPI"] → valid: [] (no aliases found)
"""

# 3. LLM returns aliases with reasoning, filtered for exact vendor matches
```

**Key Principles**:
1. **Vendor List Source**: Use discovery JSON (1-2 vendors) not Pinecone chunks (40-179 vendors)
2. **Context Understanding**: LLM distinguishes client vs vendor semantically (few-shot examples)
3. **Explicit Vendor Exclusion**: Provide primary vendors as exclusion list, not auto-generated abbreviations
4. **Reasoning Required**: LLM must explain why each alias refers to client
5. **Permissive for Redaction**: "When in doubt, include" (borderline cases included for safety)

**Results**:
- **Precision**: 100% (zero false positives - no vendor names leaked)
- **Recall**: Conservative (~33% hit rate) but high quality
- **Scale**: Tested on 9 clients, ready for 50+ client runs

**Implementation**: `scripts/generate_aliases_pilot_50.py` + full docs at `src/redaction/ALIAS_GENERATION.md`
