# Current Metadata Fields - December 2025

**Last Updated**: December 18, 2025  
**Total Fields**: 31 base fields + 3 conditional timestamp fields = **34 max**  
**Status**: ✅ **IMPLEMENTATION COMPLETE** - All fields extracted and mapped  
**Source of Truth**: `src/connectors/pinecone_client.py` lines 613-663

---

## 📋 Complete Field List (31 Base + 3 Conditional)
> **Note**: For reliable Pinecone date range filtering (`$gt/$gte/$lt/$lte`), use the numeric `*_ts` fields.

### Core Document (4 fields)
| Field | Type | Description | Source | Population |
|-------|------|-------------|--------|------------|
| `file_name` | string | Document filename (truncated to 200 chars) | `file_info.name` | 100% |
| `file_type` | string | File extension (.pdf, .xlsx, etc.) | `file_info.file_type` | 100% |
| `deal_creation_date` | string | Deal creation date (ISO format) | `deal_metadata.creation_date` | 100% |
| `deal_creation_date_ts` | integer | Deal creation date (Unix timestamp, seconds UTC) | parsed from `deal_creation_date` | ~100% |

### Identifiers (4 fields)
| Field | Type | Description | Source | Population |
|-------|------|-------------|--------|------------|
| `deal_id` | string | User-friendly deal number (e.g., "58773") | `deal_metadata.deal_id` | ~95% |
| `salesforce_deal_id` | string | Raw Salesforce 18-char ID | `deal_metadata.salesforce_deal_id` | 100% |
| `salesforce_client_id` | string | Raw Salesforce client Account ID | `deal_metadata.salesforce_client_id` | 99.96% |
| `salesforce_vendor_id` | string | Raw Salesforce vendor Account ID | `deal_metadata.salesforce_vendor_id` | 99.83% |

### Financial Metrics (6 fields)
| Field | Type | Description | Source Column | Population |
|-------|------|-------------|---------------|------------|
| `final_amount` | float | Final negotiated amount (USD) | `Total_Final_Amount_Year_1__c` or `Total_Final_Amount__c` | 100% |
| `savings_1yr` | float | First year savings (USD) | `Actual_Savings_Year_1__c` or `Total_Savings_1yr__c` | 100% |
| `savings_3yr` | float | Three year savings (USD) | `Total_Savings_3yr__c` | 100% |
| `savings_achieved` | string | Outcome description | `Savings_Achieved__c` | 90.9% |
| `fixed_savings` | float | Actual fixed savings (USD) | `Fixed_Savings__c` | 92.3% |
| `savings_target_full_term` | float | Full contract term target (USD) | `Actual_Savings_Full_Contract_Term__c` or `NPI_Savings_Target_Full_Contract_Term__c` | ~85% |

### Contract Information (5 fields)
| Field | Type | Description | Source Column | Population |
|-------|------|-------------|---------------|------------|
| `contract_term` | string | Contract duration (e.g., "36 months") | `Term__c` | ~70% |
| `contract_start` | string | Contract start date (ISO format) | `Contract_Start_Date__c` | ~65% |
| `contract_end` | string | Contract end/renewal date (ISO format) | `Contract_Renewal_Date__c` | ~65% |
| `contract_start_ts` | integer | Contract start date (Unix timestamp, seconds UTC) | parsed from `contract_start` | ~65% |
| `contract_end_ts` | integer | Contract end date (Unix timestamp, seconds UTC) | parsed from `contract_end` | ~65% |

### Business Relationships (2 fields)
| Field | Type | Description | Source | Population |
|-------|------|-------------|--------|------------|
| `client_name` | string | Client company name (mapped) | `deal_metadata.client_name` | ~95% |
| `vendor_name` | string | Vendor company name (mapped) | `deal_metadata.vendor_name` | ~90% |

### Deal Status & Quality (3 fields)
| Field | Type | Description | Source | Population |
|-------|------|-------------|--------|------------|
| `deal_status` | string | Deal status ("Closed", "In Progress", etc.) | `deal_metadata.status` | 100% |
| `deal_reason` | string | Deal reason ("New purchase", "Renewal", "Add-on", etc.) | `Deal_Reason__c` | ~95% |
| `has_parsing_errors` | boolean | Whether parsing encountered errors | `parsing_errors` list | 100% |

### Processing Metadata (1 field)
| Field | Type | Description | Source | Population |
|-------|------|-------------|--------|------------|
| `chunk_index` | integer | Chunk number within document (0-based) | Generated during chunking | 100% |

### Email Metadata (1 field)
| Field | Type | Description | Source | Population |
|-------|------|-------------|--------|------------|
| `email_has_attachments` | boolean | Whether email contained attachments | Email parser | 100% (for .msg files) |

### Deal Classification (7 fields) - December 2025
| Field | Type | Description | Source Column | Population | Status |
|-------|------|-------------|---------------|------------|-------|
| `report_type` | string | Type of report delivered | `Report_Type__c` | 19% | ✅ Implemented |
| `project_type` | string | Project category | `Project_Type__c` | 1.7% | ✅ Implemented |
| `competition` | string | Whether there was competition (Yes/No) | `Competition__c` | 24.2% | ✅ Implemented |
| `npi_analyst` | string | Assigned analyst (Salesforce user ID) | `NPI_Analyst__c` | 91.4% | ✅ Implemented |
| `dual_multi_sourcing` | string | Multi-vendor strategy used (0.0/1.0) | `Dual_Multi_sourcing_strategy__c` | 99.4% | ✅ Implemented |
| `time_pressure` | string | Urgency level (Some/Moderate/Extreme) | `Time_Pressure__c` | 9.0% | ✅ Implemented |
| `advisor_network_used` | string | External expertise used (Yes/No) | `Was_Advisor_Network_SME_Used__c` | 64.5% | ✅ Implemented |

> **Note**: `description` field was **REMOVED Dec 14, 2025** - long text not suitable for metadata filtering.

---

## ⚠️ Text Field - Removed from Metadata (December 11, 2025)

**Status**: ✅ **REMOVED** - Text field no longer stored in metadata

The `text` field has been **removed from metadata** to prevent Pinecone's 40KB metadata size limit errors. Text content is now stored at the **top level** of Pinecone records (standard Pinecone pattern) and is accessible via `match.text` attribute in search results.

**Why removed:**
- Text field alone can be 40KB+ (causing metadata size limit errors)
- Text is redundant - already stored at top level of Pinecone records
- Removing text from metadata reduces metadata size by ~40KB per chunk
- Prevents "Metadata size exceeds 40960 bytes" errors

**How to access text:**
- In search results: `result.text` (top level attribute)
- In Pinecone queries: `match.text` (automatically returned by Pinecone)

---

## ✅ Implementation Status - COMPLETE

### ✅ Completed (December 8, 2025)
- **Pinecone Schema**: All 30 fields are defined in `src/connectors/pinecone_client.py` (lines 467-521)
- **Storage**: Fields are ready to be stored in Pinecone with proper truncation
- **Extraction**: ✅ 8 new fields extracted from `Deal__c.csv` in `src/connectors/raw_salesforce_export_connector.py` (lines 274-281)
- **Enrichment**: ✅ Fields added to `DocumentMetadata` in `_enrich_with_deal_metadata()` method (lines 632-639)
- **Discovery**: ✅ Fields added to discovery JSON in `discover_documents.py` (lines 352-359)
- **Mapping**: ✅ Fields mapped in `build_metadata_dict()` in `src/pipeline/parallel_processor.py` (lines 159-166)
- **Model**: ✅ Fields added to `DocumentMetadata` dataclass in `src/models/document_models.py` (lines 104-111)

### 📝 Files Updated

1. **`src/connectors/raw_salesforce_export_connector.py`**
   - ✅ Added 8 fields to `_deal_metadata` dictionary in `_load_all_mappings()` (lines 274-281)
   - ✅ Added 8 fields to `_enrich_with_deal_metadata()` method (lines 632-639)

2. **`discover_documents.py`**
   - ✅ Added 8 fields to `deal_metadata` section when building discovery JSON (lines 352-359)

3. **`src/pipeline/parallel_processor.py`**
   - ✅ Added 8 fields to `build_metadata_dict()` return dictionary (lines 159-166)

4. **`src/models/document_models.py`**
   - ✅ Added 8 fields to `DocumentMetadata` dataclass (lines 104-111)

---

## 📊 Field Count Summary

| Category | Count | Fields |
|----------|-------|--------|
| Core Document | 3 | file_name, file_type, deal_creation_date |
| Identifiers | 4 | deal_id, salesforce_deal_id, salesforce_client_id, salesforce_vendor_id |
| Financial Metrics | 6 | final_amount, savings_1yr, savings_3yr, savings_achieved, fixed_savings, savings_target_full_term |
| Contract Information | 3 | contract_term, contract_start, contract_end |
| Business Relationships | 2 | client_name, vendor_name |
| Deal Status & Quality | 3 | has_parsing_errors, deal_status, deal_reason |
| Processing Metadata | 1 | chunk_index |
| Email Metadata | 1 | email_has_attachments |
| Deal Classification | 7 | report_type, project_type, competition, npi_analyst, dual_multi_sourcing, time_pressure, advisor_network_used |
| Text Content | 1 | text |
| **BASE TOTAL** | **31** | |
| Conditional Timestamps | +3 | deal_creation_date_ts, contract_start_ts, contract_end_ts |
| **MAX TOTAL** | **34** | (when all timestamps present) |

---

## 🔍 Source Column Mapping

Deal classification fields map to these Salesforce columns in `Deal__c.csv`:

| Pinecone Field | Salesforce Column | Notes |
|----------------|-------------------|-------|
| `deal_reason` | `Deal_Reason__c` | Deal reason ("New purchase", "Renewal", "Add-on", etc.) (truncate to 50 chars) |
| `report_type` | `Report_Type__c` | Type of report delivered |
| `description` | `Description__c` | Detailed deal description (truncate to 500 chars) |
| `project_type` | `Project_Type__c` | Project category (truncate to 50 chars) |
| `competition` | `Competition__c` | Yes/No (truncate to 10 chars) |
| `npi_analyst` | `NPI_Analyst__c` | Salesforce user ID (truncate to 50 chars) |
| `dual_multi_sourcing` | `Dual_Multi_sourcing_strategy__c` | 0.0/1.0 (truncate to 10 chars) |
| `time_pressure` | `Time_Pressure__c` | Some/Moderate/Extreme (truncate to 20 chars) |
| `advisor_network_used` | `Was_Advisor_Network_SME_Used__c` | Yes/No (truncate to 10 chars) |

---

## 📚 Related Documentation

- **Pinecone Schema**: `src/connectors/pinecone_client.py` (lines 467-521)
- **Salesforce Fields**: `AUGUST_SALESFORCE_METADATA_FIELDS.md`
- **Pinecone Dictionary**: `PINECONE_METADATA_DICTIONARY.md` (may need update)
- **Implementation Plan**: `METADATA_SIMPLIFIED_IMPLEMENTATION.md`
- **Memory Bank**: `memory-bank/activeContext.md`, `memory-bank/systemPatterns.md`, `memory-bank/OPTIMIZED_METADATA_SCHEMA.md`

## 🔍 Memory Bank Review Findings (December 8, 2025)

After reviewing all memory bank files, **the 8 new deal classification fields are NOT documented** in:
- ❌ `memory-bank/activeContext.md` - Mentions "22 fields" schema, no mention of 8 new fields
- ❌ `memory-bank/systemPatterns.md` - No mention of deal classification fields
- ❌ `memory-bank/CURRENT_METADATA_SCHEMA.md` - File is empty
- ❌ `memory-bank/OPTIMIZED_METADATA_SCHEMA.md` - Shows 29 fields, doesn't include the 8 new fields
- ❌ `memory-bank/METADATA_OPTIMIZATION_ANALYSIS.md` - No mention of deal classification fields

**Conclusion**: The 8 fields were added to the Pinecone schema in code (`pinecone_client.py` line 524 comment: "ADDED in Dec 7 update") but:
1. **Not extracted** from source data
2. **Not documented** in memory bank files
3. **Not included** in discovery/processing pipeline

This confirms the fields are **recently added but incomplete** - schema ready, extraction pending.

---

## ✅ Implementation Complete

All 8 deal classification fields are now fully implemented:

1. ✅ **Extracted** from `Deal__c.csv` in `raw_salesforce_export_connector.py`
2. ✅ **Added to discovery JSON** in `discover_documents.py`
3. ✅ **Mapped in processor** in `parallel_processor.py`
4. ✅ **Added to data model** in `document_models.py`
5. ⏭️ **Next**: Test with a small discovery file to verify all 8 fields populate correctly
6. ⏭️ **Next**: Update documentation (`PINECONE_METADATA_DICTIONARY.md`) with the new fields

