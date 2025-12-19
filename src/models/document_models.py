"""
Document data models for the processing pipeline
Contains shared data structures to avoid circular imports
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class DocumentMetadata:
    """Enhanced metadata structure for deal files."""
    # File Information
    path: str
    name: str
    size: int
    size_mb: float
    file_type: str
    modified_time: str
    
    # Business Metadata from Path / Deal Info
    deal_creation_date: Optional[str] = None  # YYYY-MM-DD format from deal__cs.csv
    week_number: Optional[int] = None
    week_date: Optional[str] = None
    vendor: Optional[str] = None
    client: Optional[str] = None
    deal_number: Optional[str] = None
    deal_name: Optional[str] = None
    
    # Derived/Computed Fields
    full_path: str = ""
    path_components: List[str] = field(default_factory=list)
    extraction_confidence: float = 0.0
    parsing_errors: List[str] = field(default_factory=list)
    
    # Dropbox metadata
    dropbox_id: str = ""
    content_hash: Optional[str] = None
    is_downloadable: bool = True
    
    # LLM Document Classification
    document_type: Optional[str] = None
    document_type_confidence: float = 0.0
    classification_reasoning: Optional[str] = None
    classification_method: Optional[str] = None
    alternative_document_types: List[Dict] = field(default_factory=list)
    classification_tokens_used: int = 0
    
    # Enhanced LLM Classification Metadata (v3) - pruned
    product_pricing_depth: Optional[str] = None  # "low", "medium", "high"
    commercial_terms_depth: Optional[str] = None  # "low", "medium", "high"
    proposed_term_start: Optional[str] = None  # "YYYY-MM-DD" format
    proposed_term_end: Optional[str] = None  # "YYYY-MM-DD" format
    # Pruned: key_topics, vendor_products_mentioned, pricing_indicators
    
    # Salesforce Deal Metadata (NEW!)
    deal_id: Optional[str] = None  # User-friendly deal number (e.g., "58773" from Deal-58773)
    salesforce_deal_id: Optional[str] = None  # Raw Salesforce ID (e.g., "a0WQg000001QKH3MAO") for lookups/tracing
    deal_subject: Optional[str] = None  # From Deal__c.csv
    deal_status: Optional[str] = None
    deal_reason: Optional[str] = None
    deal_start_date: Optional[str] = None
    negotiated_by: Optional[str] = None
    
    # Financial Metrics (from Deal__c.csv) - ENHANCED WITH MISSING FIELDS
    proposed_amount: Optional[float] = None
    final_amount: Optional[float] = None
    savings_1yr: Optional[float] = None
    savings_3yr: Optional[float] = None
    savings_target: Optional[float] = None
    savings_percentage: Optional[float] = None  # Calculated
    
    # MISSING CRITICAL SAVINGS FIELDS (From EDA Analysis!)
    savings_achieved: Optional[str] = None  # 90.9% populated - actual outcomes like "N - Time constraint"
    fixed_savings: Optional[float] = None  # 92.3% populated - actual savings amounts
    savings_target_full_term: Optional[float] = None  # Full contract term target
    final_amount_full_term: Optional[float] = None  # Full contract term final amount
    
    # Client/Vendor Information
    client_id: Optional[str] = None  # Friendly client identifier or name
    client_name: Optional[str] = None  # Full client name (from mapping CSV)
    salesforce_client_id: Optional[str] = None  # Raw Salesforce ID for tracing
    vendor_id: Optional[str] = None  # Friendly vendor identifier or name
    vendor_name: Optional[str] = None  # Full vendor name (from mapping CSV)
    salesforce_vendor_id: Optional[str] = None  # Raw Salesforce ID for tracing
    
    # Contract Information (when available)
    contract_term: Optional[str] = None
    contract_start: Optional[str] = None
    contract_end: Optional[str] = None
    effort_level: Optional[str] = None
    has_fmv_report: Optional[bool] = None
    deal_origin: Optional[str] = None
    
    # Salesforce-specific fields
    salesforce_content_version_id: Optional[str] = None  # From mapping CSV
    
    # Rich Narrative Content (MISSING CRITICAL FIELDS!)
    current_narrative: Optional[str] = None  # From Current_Narrative__c (88% populated)
    customer_comments: Optional[str] = None  # From Comments_To_Customer__c (40% populated)
    content_source: Optional[str] = None  # "document_file", "deal_narrative", "customer_comments"
    
    # Deal Classification Fields (added December 2025)
    # Note: description removed Dec 14 - long text not suitable for Pinecone filtering
    report_type: Optional[str] = None  # From Report_Type__c (19% populated)
    project_type: Optional[str] = None  # From Project_Type__c (1.7% populated)
    competition: Optional[str] = None  # From Competition__c (24.2% populated)
    npi_analyst: Optional[str] = None  # From NPI_Analyst__c (91.4% populated)
    dual_multi_sourcing: Optional[str] = None  # From Dual_Multi_sourcing_strategy__c (99.4% populated)
    time_pressure: Optional[str] = None  # From Time_Pressure__c (9.0% populated)
    advisor_network_used: Optional[str] = None  # From Was_Advisor_Network_SME_Used__c (64.5% populated)
    
    # Email Metadata (for .msg files only)
    email_subject: Optional[str] = None  # Email subject line (for .msg files)
    email_has_attachments: Optional[bool] = None  # Whether email has attachments
    # Note: email_sender, email_recipients_to, email_date removed (PII)
    
    # Mapping status tracking (NEW for debugging)
    mapping_status: Optional[str] = None  # "mapped", "unmapped"
    mapping_method: Optional[str] = None  # "exact_filename", "fuzzy_filename", etc.
    mapping_reason: Optional[str] = None  # "file_not_in_csv", etc.
    
    # Legacy fields (kept for backwards compatibility)
    document_subtype: Optional[str] = None
    classification_confidence: float = 0.0
    
    # Content Information (filled by document parser)
    content_preview: Optional[str] = None
    page_count: Optional[int] = None
    word_count: Optional[int] = None
    
    # Processing Status
    processed: bool = False
    processing_error: Optional[str] = None
    processing_time: Optional[float] = None
    
    def __post_init__(self):
        """Initialize default values and computed fields"""
        # Set full_path from path
        self.full_path = self.path
        
        # Calculate size_mb if not already set
        if self.size:
            self.size_mb = round(self.size / (1024 * 1024), 2)
        else:
            self.size_mb = 0.0 