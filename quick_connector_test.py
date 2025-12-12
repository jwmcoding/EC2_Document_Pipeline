import sys
sys.path.insert(0, "src")
from connectors.raw_salesforce_export_connector import RawSalesforceExportConnector

EXPORT_DIR = "/data/august-2024"

print("=== Testing connector WITHOUT ContentDocument.csv ===")
print()

try:
    connector = RawSalesforceExportConnector(
        export_root_dir=EXPORT_DIR,
        content_versions_csv=f"{EXPORT_DIR}/ContentVersion.csv",
        content_documents_csv=None,
        content_document_links_csv=f"{EXPORT_DIR}/ContentDocumentLink.csv",
        deal_metadata_csv=f"{EXPORT_DIR}/Deal__c.csv"
    )
    
    print()
    print("SUCCESS! Connector loaded:")
    print(f"  ContentVersions: {len(connector._content_versions):,}")
    print(f"  ContentDocuments (derived): {len(connector._content_documents):,}")
    print(f"  ContentDocumentLinks: {len(connector._content_document_links):,}")
    print(f"  Deals: {len(connector._deal_metadata):,}")
    
    # Check new deal fields
    if connector._deal_metadata:
        sample_deal = list(connector._deal_metadata.values())[0]
        new_fields = ["report_type", "description", "project_type", "competition",
                     "npi_analyst", "dual_multi_sourcing", "time_pressure", "advisor_network_used"]
        print()
        print("=== New Deal Classification Fields (sample) ===")
        for field in new_fields:
            value = sample_deal.get(field, "MISSING")
            status = "OK" if value and value != "MISSING" else "empty"
            empty_str = "(empty)"
            val_str = str(value)[:50] if value else empty_str
            print(f"  [{status}] {field}: {val_str}")
            
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
