# Current Pinecone Metadata Schema

**Last Updated**: December 18, 2025  
**Source of Truth**: `src/connectors/pinecone_client.py` lines 613-679  
**Index**: `npi-deal-data`  
**Total Fields**: 30 base + 3 conditional timestamps = **33 max**

---

## Quick Reference (30 Base Fields)

```
CORE DOCUMENT (3)     : file_name, file_type, deal_creation_date
IDENTIFIERS (4)       : deal_id, salesforce_deal_id, salesforce_client_id, salesforce_vendor_id
FINANCIAL (6)         : final_amount, savings_1yr, savings_3yr, savings_achieved, fixed_savings, savings_target_full_term
CONTRACT (3)          : contract_term, contract_start, contract_end
PROCESSING (1)        : chunk_index
SEARCH (2)            : client_name, vendor_name
QUALITY (2)           : has_parsing_errors, deal_status
EMAIL (1)             : email_has_attachments
DEAL CLASSIFICATION (7): report_type, project_type, competition, npi_analyst, dual_multi_sourcing, time_pressure, advisor_network_used
TEXT (1)              : text

CONDITIONAL TIMESTAMPS (+3): deal_creation_date_ts, contract_start_ts, contract_end_ts
```

---

## Field Details by Category

### Core Document (3 fields)
| Field | Type | Max Length | Source |
|-------|------|------------|--------|
| `file_name` | string | 200 chars | `file_info.name` |
| `file_type` | string | - | `file_info.file_type` |
| `deal_creation_date` | string | - | `deal_metadata.creation_date` |

### Identifiers (4 fields)
| Field | Type | Source |
|-------|------|--------|
| `deal_id` | string | `deal_metadata.deal_id` |
| `salesforce_deal_id` | string | `deal_metadata.salesforce_deal_id` |
| `salesforce_client_id` | string | `deal_metadata.salesforce_client_id` |
| `salesforce_vendor_id` | string | `deal_metadata.salesforce_vendor_id` |

### Financial (6 fields)
| Field | Type | Source |
|-------|------|--------|
| `final_amount` | float | `Total_Final_Amount_Year_1__c` |
| `savings_1yr` | float | `Actual_Savings_Year_1__c` |
| `savings_3yr` | float | `Total_Savings_3yr__c` |
| `savings_achieved` | string (200) | `Savings_Achieved__c` |
| `fixed_savings` | float | `Fixed_Savings__c` |
| `savings_target_full_term` | float | `NPI_Savings_Target_Full_Contract_Term__c` |

### Contract (3 fields)
| Field | Type | Max Length | Source |
|-------|------|------------|--------|
| `contract_term` | string | 100 chars | `Term__c` |
| `contract_start` | string | - | `Contract_Start_Date__c` |
| `contract_end` | string | - | `Contract_Renewal_Date__c` |

### Processing (1 field)
| Field | Type | Source |
|-------|------|--------|
| `chunk_index` | integer | Generated during chunking |

### Search (2 fields)
| Field | Type | Max Length | Source |
|-------|------|------------|--------|
| `client_name` | string | 100 chars | `deal_metadata.client_name` |
| `vendor_name` | string | 100 chars | `deal_metadata.vendor_name` |

### Quality (2 fields)
| Field | Type | Source |
|-------|------|--------|
| `has_parsing_errors` | boolean | `parsing_errors` list |
| `deal_status` | string | `deal_metadata.status` |

### Email (1 field)
| Field | Type | Source |
|-------|------|--------|
| `email_has_attachments` | boolean | Email parser |

### Deal Classification (7 fields)
| Field | Type | Max Length | Source |
|-------|------|------------|--------|
| `report_type` | string | 100 chars | `Report_Type__c` |
| `project_type` | string | 50 chars | `Project_Type__c` |
| `competition` | string | 10 chars | `Competition__c` |
| `npi_analyst` | string | 50 chars | `NPI_Analyst__c` |
| `dual_multi_sourcing` | string | 10 chars | `Dual_Multi_sourcing_strategy__c` |
| `time_pressure` | string | 20 chars | `Time_Pressure__c` |
| `advisor_network_used` | string | 10 chars | `Was_Advisor_Network_SME_Used__c` |

### Text Content (1 field)
| Field | Type | Max Length | Source |
|-------|------|------------|--------|
| `text` | string | 37,000 chars | Chunk text content |

### Conditional Timestamps (3 fields)
| Field | Type | Condition | Source |
|-------|------|-----------|--------|
| `deal_creation_date_ts` | integer | When date parses | Unix timestamp (seconds UTC) |
| `contract_start_ts` | integer | When date parses | Unix timestamp (seconds UTC) |
| `contract_end_ts` | integer | When date parses | Unix timestamp (seconds UTC) |

---

## Removed Fields (Historical)

| Field | Removed | Reason |
|-------|---------|--------|
| `description` | Dec 14, 2025 | Long text not suitable for filtering |
| `client_id`, `vendor_id` | Dec 5, 2025 | Duplicates of salesforce_* fields |
| `parser_backend`, `processing_method` | Dec 5, 2025 | Internal use only |
| `document_path`, `file_size_mb`, `modified_time` | Earlier | Not searchable/useful |
| `week_number`, `week_date`, `year` | Earlier | Redundant with timestamps |

---

## Usage Notes

### Sanitization
- **Strings**: `_sanitize_str()` converts `None`, `NaN`, `'None'`, `'nan'` → empty string
- **Numbers**: `_sanitize_numeric()` converts `None`, `NaN` → `0.0`
- **Dates**: `_parse_date_to_unix_ts()` handles NaN → `None`

### Pinecone Limits
- **Metadata size**: 40KB per record
- **Text field**: Truncated to 37,000 chars to stay within limit

### Date Filtering
```python
# Use timestamp fields for range queries (not string dates)
filter = {
    "deal_creation_date_ts": {"$gte": 1704067200}  # Jan 1, 2024
}
```

---

## Related Documentation

- **Detailed field guide**: `/CURRENT_METADATA_FIELDS_DEC2025.md`
- **Source code**: `src/connectors/pinecone_client.py`
- **Discovery docs**: `AGENTS.md` (Salesforce processing section)
