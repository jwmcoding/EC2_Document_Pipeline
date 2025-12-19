# npi-deal-data / sf-export-aug15-2025 — Pinecone Metadata Dictionary
**Purpose**: Field-by-field reference for metadata written into Pinecone for the `npi-deal-data` index, namespace `sf-export-aug15-2025`.

This document is derived from the actual upsert schema implemented in:
- `src/connectors/pinecone_client.py` → `PineconeDocumentClient.upsert_chunks()`

---

## ✅ How to interpret identifiers in this namespace (critical)

In `npi-deal-data / sf-export-aug15-2025`, **do not assume `deal_id` matches other indexes/namespaces**.

- **`deal_id`**: Often a numeric string like `"68624"` (human-friendly deal number).
- **`salesforce_deal_id`**: Often the Salesforce-style record id like `"a0WQg000004wW21MAE"`.

**Cross-index join recommendation**:
- Join documents across indexes using **`salesforce_deal_id` + `file_name`** when available.

---

## Field schema (32 metadata fields)

> Types shown are how we serialize them at upsert time (string/float/int/bool).  
> Some string fields are truncated for safety (see “Truncation & limits”).

### 📋 Core Document

#### `file_name` (string)
- **Description**: Document filename (no path)
- **Source**: `metadata["name"]`
- **Notes**: Truncated to 200 chars
- **Filterable**: ✅ Yes

#### `file_type` (string)
- **Description**: File extension
- **Example**: `".pdf"`, `".docx"`, `".xlsx"`, `".msg"`
- **Filterable**: ✅ Yes

#### `deal_creation_date` (string)
- **Description**: Deal creation date (stringified; typically ISO-ish)
- **Filterable**: ✅ Yes (string comparisons only; **not reliable** for range filtering)

#### `deal_creation_date_ts` (int)
- **Description**: Deal creation date as Unix timestamp (seconds since epoch, UTC)
- **Source**: Parsed from `metadata["deal_creation_date"]`
- **Filterable**: ✅ Yes (**recommended** for Pinecone range filters with `$gt/$gte/$lt/$lte`)

---

### 🧾 Identifiers (Salesforce linkage)

#### `deal_id` (string)
- **Description**: Deal identifier used in this namespace; often a numeric deal number like `"68624"`
- **Filterable**: ✅ Yes

#### `salesforce_deal_id` (string)
- **Description**: Salesforce deal record id (e.g., `"a0W..."`)
- **Filterable**: ✅ Yes

#### `salesforce_client_id` (string)
- **Description**: Salesforce client account id (e.g., `"001..."`)
- **Filterable**: ✅ Yes

#### `salesforce_vendor_id` (string)
- **Description**: Salesforce vendor account id
- **Filterable**: ✅ Yes

---

### 💰 Financial (numeric fields)

#### `final_amount` (float)
- **Description**: Final negotiated amount (USD)
- **Default**: 0.0
- **Filterable**: ✅ Yes (range queries)

#### `savings_1yr` (float)
- **Description**: First-year savings (USD)
- **Default**: 0.0
- **Filterable**: ✅ Yes (range queries)

#### `savings_3yr` (float)
- **Description**: Three-year savings (USD)
- **Default**: 0.0
- **Filterable**: ✅ Yes (range queries)

#### `savings_achieved` (string)
- **Description**: Outcome/notes like `"Y - Full target achieved"` / `"N - Time constraint"`
- **Notes**: Truncated to 200 chars
- **Filterable**: ✅ Yes

#### `fixed_savings` (float)
- **Description**: Actual fixed savings amount (USD)
- **Default**: 0.0
- **Filterable**: ✅ Yes (range queries)

#### `savings_target_full_term` (float)
- **Description**: Full contract term savings target (USD)
- **Default**: 0.0
- **Filterable**: ✅ Yes (range queries)

---

### 📄 Contract

#### `contract_term` (string)
- **Description**: Contract term summary (e.g., `"36 months"`)
- **Notes**: Truncated to 100 chars
- **Filterable**: ✅ Yes

#### `contract_start` (string)
- **Description**: Contract start date (stringified)
- **Filterable**: ✅ Yes

#### `contract_end` (string)
- **Description**: Contract end date (stringified)
- **Filterable**: ✅ Yes

#### `contract_start_ts` (int)
- **Description**: Contract start date as Unix timestamp (seconds since epoch, UTC)
- **Source**: Parsed from `metadata["contract_start"]`
- **Filterable**: ✅ Yes (**recommended** for Pinecone range filters)

#### `contract_end_ts` (int)
- **Description**: Contract end date as Unix timestamp (seconds since epoch, UTC)
- **Source**: Parsed from `metadata["contract_end"]`
- **Filterable**: ✅ Yes (**recommended** for Pinecone range filters)

---

### ⚙️ Processing

#### `chunk_index` (int)
- **Description**: Chunk ordering index within a document
- **Filterable**: ✅ Yes (range queries)
- **Use**: Reconstruct full documents by sorting chunks on `chunk_index`

---

### 🔍 Search display

#### `client_name` (string)
- **Description**: Human-readable client name
- **Notes**: Truncated to 100 chars
- **Filterable**: ✅ Yes

#### `vendor_name` (string)
- **Description**: Human-readable vendor name
- **Notes**: Truncated to 100 chars
- **Filterable**: ✅ Yes

---

### 🧪 Quality / status

#### `has_parsing_errors` (bool)
- **Description**: True if parser reported errors for this document/chunk
- **Filterable**: ✅ Yes

#### `deal_status` (string)
- **Description**: Deal status (e.g., `"Closed"`, `"In Progress"`)
- **Filterable**: ✅ Yes

---

### ✉️ Email

#### `email_has_attachments` (bool)
- **Description**: True if an email document has attachments
- **Filterable**: ✅ Yes

---

### 🧭 Deal classification (December 2025 additions)

#### `report_type` (string)
- **Description**: High-level report categorization
- **Notes**: Truncated to 100 chars
- **Filterable**: ✅ Yes

#### `project_type` (string)
- **Description**: Project type
- **Notes**: Truncated to 50 chars
- **Filterable**: ✅ Yes

#### `competition` (string)
- **Description**: Competition flag / code
- **Notes**: Truncated to 10 chars
- **Filterable**: ✅ Yes

#### `npi_analyst` (string)
- **Description**: Analyst identifier/name
- **Notes**: Truncated to 50 chars
- **Filterable**: ✅ Yes

#### `dual_multi_sourcing` (string)
- **Description**: Dual/multi-sourcing flag
- **Notes**: Truncated to 10 chars
- **Filterable**: ✅ Yes

#### `time_pressure` (string)
- **Description**: Time pressure flag/category
- **Notes**: Truncated to 20 chars
- **Filterable**: ✅ Yes

#### `advisor_network_used` (string)
- **Description**: Whether an advisor network was used
- **Notes**: Truncated to 10 chars
- **Filterable**: ✅ Yes

---

### 🧾 Chunk text

#### `text` (string)
- **Description**: Extracted text for the **chunk** (not the full document)
- **Notes**: Truncated to 37,000 characters at upsert
- **Filterable**: Technically yes, but **not recommended** for filters; use it as content for retrieval/reconstruction
- **Use**: Reconstruct full document by concatenating `text` across chunks ordered by `chunk_index`

---

## Truncation & limits (as implemented)

These truncations are applied during upsert:
- `file_name`: 200 chars
- `client_name`, `vendor_name`: 100 chars
- `contract_term`: 100 chars
- `savings_achieved`: 200 chars
- Deal classification fields: 10–100 chars depending on field
- `text`: 37,000 chars

---

## Example filters (recommended)

### Find PDFs for a Salesforce deal (cross-index-safe)
```json
{
  "salesforce_deal_id": {"$eq": "a0WQg000004wW21MAE"},
  "file_type": {"$eq": ".pdf"}
}
```

### Find a specific PDF within a deal
```json
{
  "$and": [
    {"salesforce_deal_id": {"$eq": "a0WQg000004wW21MAE"}},
    {"file_name": {"$eq": "FMV Report - Mattress Firm - Databricks July 2025.pdf"}}
  ]
}
```

### Find “has parsing errors”
```json
{"has_parsing_errors": {"$eq": true}}
```

### Date range filter (recommended: numeric timestamps)
```json
{
  "$and": [
    {"deal_creation_date_ts": {"$gte": 1704067200}},
    {"deal_creation_date_ts": {"$lt": 1735689600}}
  ]
}
```


