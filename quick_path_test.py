import sys
sys.path.insert(0, "src")
from connectors.raw_salesforce_export_connector import RawSalesforceExportConnector

EXPORT_DIR = "/data/august-2024"

print("=== Testing file path resolution ===")
print()

connector = RawSalesforceExportConnector(
    export_root_dir=EXPORT_DIR,
    content_versions_csv=f"{EXPORT_DIR}/ContentVersion.csv",
    content_documents_csv=None,
    content_document_links_csv=f"{EXPORT_DIR}/ContentDocumentLink.csv",
    deal_metadata_csv=f"{EXPORT_DIR}/Deal__c.csv"
)

print()
print(f"ContentVersions loaded: {len(connector._content_versions):,}")
print(f"Valid file paths found: {len(connector._valid_file_paths):,}")
print()

if connector._valid_file_paths:
    print("Sample resolved paths:")
    for i, (cv_id, path) in enumerate(list(connector._valid_file_paths.items())[:5]):
        print(f"  {cv_id} -> {path}")
else:
    print("No valid file paths found!")
    # Try resolving manually
    print()
    print("Testing manual resolution for first 5 ContentVersions:")
    for cv_id in list(connector._content_versions.keys())[:5]:
        cv_data = connector._content_versions[cv_id]
        path = connector._resolve_file_path(cv_id, cv_data.get("content_document_id"), cv_data.get("deal_id"))
        status = "FOUND" if path else "NOT FOUND"
        print(f"  {cv_id}: {status} -> {path}")
