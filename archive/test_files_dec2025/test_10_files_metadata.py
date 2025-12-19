#!/usr/bin/env python3
"""
Test script for 10 files to verify metadata updates (8 new deal classification fields)
Tests the full pipeline: discovery → processing → Pinecone verification
"""

import os
import sys
import json
import argparse
import random
import hashlib
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from discover_documents import DocumentDiscovery
from process_discovered_documents import DiscoveredDocumentProcessor
from src.config.colored_logging import ColoredLogger
from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings
from src.connectors.raw_salesforce_export_connector import RawSalesforceExportConnector
from src.models.document_models import DocumentMetadata
from src.parsers.document_converter import DocumentConverter
from src.parsers.pdfplumber_parser import PDFPlumberParser
try:
    from src.parsers.docling_parser import DoclingParser, is_docling_available
except Exception:
    DoclingParser = None  # type: ignore[assignment]
    def is_docling_available() -> bool:  # type: ignore[no-redef]
        return False
try:
    from src.parsers.mistral_parser import MistralParser, is_mistral_available
except Exception:
    MistralParser = None  # type: ignore[assignment]
    def is_mistral_available() -> bool:  # type: ignore[no-redef]
        return False

logger = ColoredLogger("test_10_files")

def sample_discovery_by_deals(
    discovery_file: str,
    output_file: str,
    sample_deals: int = 10,
    seed: int = 1337,
    max_files_per_deal: int | None = None,
) -> bool:
    """
    Take an existing discovery JSON and write a new discovery JSON containing
    all documents for a random sample of deals.

    Deal selection key:
    - Prefer doc['deal_metadata']['deal_id']
    - Else fallback to doc['deal_metadata']['salesforce_deal_id']
    """
    try:
        with open(discovery_file, "r") as f:
            data = json.load(f)

        docs = data.get("documents", []) or []
        if not docs:
            logger.error("❌ No documents found in discovery file")
            return False

        deal_to_docs: dict[str, list[dict]] = {}
        for doc in docs:
            deal_meta = doc.get("deal_metadata", {}) or {}
            deal_id = (deal_meta.get("deal_id") or deal_meta.get("salesforce_deal_id") or "").strip()
            if not deal_id:
                continue
            deal_to_docs.setdefault(deal_id, []).append(doc)

        if not deal_to_docs:
            logger.error("❌ No deal-associated documents found (deal_id missing).")
            return False

        all_deals = sorted(deal_to_docs.keys())
        rng = random.Random(seed)
        if len(all_deals) <= sample_deals:
            sampled_deals = all_deals
        else:
            sampled_deals = rng.sample(all_deals, sample_deals)

        sampled_docs: list[dict] = []
        for deal_id in sampled_deals:
            deal_docs = deal_to_docs.get(deal_id, [])
            if max_files_per_deal is not None and max_files_per_deal > 0:
                deal_docs = deal_docs[:max_files_per_deal]
            sampled_docs.extend(deal_docs)

        new_data = dict(data)
        new_data["documents"] = sampled_docs
        new_data.setdefault("metadata", {})
        new_data["metadata"]["sampled_deals"] = sampled_deals
        new_data["metadata"]["sample_seed"] = seed
        new_data["metadata"]["sample_deals_requested"] = sample_deals
        new_data["metadata"]["sample_docs_count"] = len(sampled_docs)
        new_data["metadata"]["max_files_per_deal"] = max_files_per_deal

        with open(output_file, "w") as f:
            json.dump(new_data, f, indent=2)

        logger.info(f"🎲 Sampled deals: {len(sampled_deals)}/{len(all_deals)}")
        logger.info(f"   Output discovery: {output_file}")
        logger.info(f"   Documents selected: {len(sampled_docs)}")
        return True

    except Exception as e:
        logger.error(f"❌ Error sampling discovery by deals: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_test_discovery(export_dir: str, output_file: str, max_docs: int = 10) -> bool:
    """Create discovery file with full metadata enrichment"""
    
    logger.info("🔍 Step 1: Creating discovery with full metadata...")
    logger.info(f"   Source: {export_dir}")
    logger.info(f"   Output: {output_file}")
    
    try:
        discovery = DocumentDiscovery()
        
        # Use raw_salesforce_export_connector to get full metadata
        # Auto-detect CSV paths
        export_path = Path(export_dir)
        content_versions_csv = str(export_path / "ContentVersion.csv")
        content_documents_csv = str(export_path / "ContentDocument.csv") if (export_path / "ContentDocument.csv").exists() else None
        content_document_links_csv = str(export_path / "ContentDocumentLink.csv")
        deal_metadata_csv = str(export_path / "Deal__c.csv")
        
        # Auto-detect client/vendor mapping CSVs
        client_mapping_csv = str(export_path / "SF-Cust-Mapping.csv") if (export_path / "SF-Cust-Mapping.csv").exists() else None
        vendor_mapping_csv = str(export_path / "SF-Vendor_mapping.csv") if (export_path / "SF-Vendor_mapping.csv").exists() else None
        
        logger.info(f"   Client mapping: {'Found' if client_mapping_csv else 'Not found'}")
        logger.info(f"   Vendor mapping: {'Found' if vendor_mapping_csv else 'Not found'}")
        
        discovery_args = argparse.Namespace(
            source='salesforce_raw',
            export_root_dir=export_dir,
            content_versions_csv=content_versions_csv,
            content_documents_csv=content_documents_csv,
            content_document_links_csv=content_document_links_csv,
            deal_metadata_csv=deal_metadata_csv,
            client_mapping_csv=client_mapping_csv,
            vendor_mapping_csv=vendor_mapping_csv,
            deal_mapping_csv=None,
            require_deal_association=False,
            output=output_file,
            max_docs=max_docs,
            limit=None,
            batch_size=100,  # Required for discovery
            resume=False  # Don't resume existing discovery
        )
        
        discovery.run(discovery_args)
        
        # Verify discovery file
        if not Path(output_file).exists():
            logger.error(f"❌ Discovery file not created: {output_file}")
            return False
        
        with open(output_file, 'r') as f:
            data = json.load(f)
        
        doc_count = len(data.get('documents', []))
        logger.info(f"✅ Discovery created: {doc_count} documents")
        
        # Check if documents have deal metadata
        docs_with_deals = sum(1 for doc in data.get('documents', []) 
                             if doc.get('deal_metadata', {}).get('deal_id'))
        logger.info(f"   Documents with deal metadata: {docs_with_deals}/{doc_count}")
        
        # Check for new fields in first document
        if data.get('documents'):
            first_doc = data['documents'][0]
            deal_meta = first_doc.get('deal_metadata', {})
            new_fields = ['report_type', 'description', 'project_type', 'competition', 
                         'npi_analyst', 'dual_multi_sourcing', 'time_pressure', 'advisor_network_used']
            found_fields = [f for f in new_fields if deal_meta.get(f) is not None]
            logger.info(f"   New fields found in sample: {len(found_fields)}/8")
            if found_fields:
                logger.info(f"      Found: {', '.join(found_fields[:3])}...")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating discovery: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_test_files(
    discovery_file: str,
    namespace: str = "test-metadata-dec2025",
    export_dir: str | None = None,
    limit: int | None = 10,
    workers: int = 6,
    enable_redaction: bool = False,
    client_redaction_csv: str | None = None,
    redaction_model: str = "gpt-5-mini-2025-08-07",
) -> bool:
    """Process the test files"""
    
    logger.info(f"⚙️  Step 2: Processing {discovery_file}...")
    
    try:
        processor = DiscoveredDocumentProcessor()
        
        args = argparse.Namespace(
            input=discovery_file,
            namespace=namespace,
            export_root_dir=export_dir,  # Pass export root for salesforce_raw source
            workers=workers,  # Configurable workers (default: 6)
            parser_backend='docling',
            chunking_strategy='business_aware',
            batch_size=10,  # Batch size for progress reporting
            limit=limit,
            resume=False,
            reprocess=False,
            use_batch=False,
            batch_only=False,
            interactive=False,
            filter_type=None,
            filter_file_type=None,
            filter_vendor=None,
            filter_client=None,
            max_size_mb=None,
            # Docling OCR mode and quality thresholds
            docling_ocr_mode='auto',
            docling_timeout_seconds=240,
            docling_min_text_chars=800,
            docling_min_word_count=150,
            docling_alnum_threshold=0.5,
            # Additional filter arguments
            exclude_file_type=None,
            modified_after=None,
            modified_before=None,
            min_size_kb=None,
            # Redaction stage (wired into process_discovered_documents.py)
            enable_redaction=enable_redaction,
            client_redaction_csv=client_redaction_csv,
            redaction_model=redaction_model,
            redaction_strict_mode=True,
        )
        
        processor.run(args)
        
        logger.info("✅ Processing complete")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error processing: {e}")
        import traceback
        traceback.print_exc()
        return False

def _convert_discovery_doc_to_document_metadata(doc_data: dict) -> DocumentMetadata:
    """Convert discovery JSON document entry to DocumentMetadata (minimal required fields + deal/client/vendor IDs)."""
    file_info = doc_data.get("file_info", {}) or {}
    business_meta = doc_data.get("business_metadata", {}) or {}
    deal_meta = doc_data.get("deal_metadata", {}) or {}
    source_meta = doc_data.get("source_metadata", {}) or {}

    return DocumentMetadata(
        path=file_info.get("path", ""),
        name=file_info.get("name", ""),
        size=file_info.get("size", 0),
        size_mb=file_info.get("size_mb", 0.0),
        file_type=file_info.get("file_type", ""),
        modified_time=file_info.get("modified_time", ""),
        deal_creation_date=business_meta.get("deal_creation_date") or deal_meta.get("deal_creation_date"),
        vendor=business_meta.get("vendor"),
        client=business_meta.get("client"),
        deal_number=business_meta.get("deal_number"),
        deal_name=business_meta.get("deal_name"),
        deal_id=deal_meta.get("deal_id"),
        salesforce_deal_id=deal_meta.get("salesforce_deal_id"),
        client_name=deal_meta.get("client_name"),
        vendor_name=deal_meta.get("vendor_name"),
        salesforce_client_id=deal_meta.get("salesforce_client_id"),
        salesforce_vendor_id=deal_meta.get("salesforce_vendor_id"),
        salesforce_content_version_id=deal_meta.get("salesforce_content_version_id") or source_meta.get("source_id"),
        full_path=source_meta.get("source_path", file_info.get("path", "")),
        path_components=business_meta.get("path_components", []),
        dropbox_id=source_meta.get("source_id", ""),
        content_hash=file_info.get("content_hash"),
        is_downloadable=True,
    )


def _init_pdf_parser(parser_backend: str, docling_kwargs: dict | None = None):
    backend = (parser_backend or "pdfplumber").lower().strip()
    if backend == "mistral":
        if MistralParser is None or not is_mistral_available():
            logger.warning("⚠️ Mistral parser requested but not available. Falling back to PDFPlumber.")
            return PDFPlumberParser(), "pdfplumber"
        return MistralParser(), "mistral"
    if backend == "docling":
        if DoclingParser is None or not is_docling_available():
            logger.warning("⚠️ Docling parser requested but not available. Falling back to PDFPlumber.")
            return PDFPlumberParser(), "pdfplumber"
        kwargs = docling_kwargs or {}
        # Maintain parity with pipeline: accept ocr_mode, map to ocr bool
        if "ocr_mode" in kwargs:
            ocr_mode_val = kwargs.pop("ocr_mode")
            kwargs["ocr"] = str(ocr_mode_val).lower() in ("on", "auto")
        kwargs.setdefault("ocr", True)
        kwargs.setdefault("timeout_seconds", 240)
        return DoclingParser(**kwargs), "docling"
    return PDFPlumberParser(), "pdfplumber"


def run_local_redaction_harness(
    discovery_file: str,
    export_dir: str,
    output_dir: str,
    parser_backend: str,
    enable_redaction: bool,
    client_redaction_csv: str | None,
    redaction_model: str,
) -> bool:
    """
    Local-only harness: for each document in the discovery JSON, download bytes from the export_dir,
    extract text (convert + parse), apply redaction, and write results to output_dir.

    No chunking, embedding, or Pinecone upsert is performed.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    docs_out = out / "docs"
    docs_out.mkdir(parents=True, exist_ok=True)

    with open(discovery_file, "r") as f:
        data = json.load(f)
    docs = data.get("documents", []) or []
    if not docs:
        logger.error("❌ No documents in discovery file")
        return False

    # Init file source for raw export paths
    export_path = Path(export_dir)
    content_versions_csv = str(export_path / "ContentVersion.csv")
    content_documents_csv = str(export_path / "ContentDocument.csv") if (export_path / "ContentDocument.csv").exists() else None
    content_document_links_csv = str(export_path / "ContentDocumentLink.csv")
    deal_metadata_csv = str(export_path / "Deal__c.csv")
    client_mapping_csv = str(export_path / "SF-Cust-Mapping.csv") if (export_path / "SF-Cust-Mapping.csv").exists() else None
    vendor_mapping_csv = str(export_path / "SF-Vendor_mapping.csv") if (export_path / "SF-Vendor_mapping.csv").exists() else None

    source = RawSalesforceExportConnector(
        export_root_dir=export_dir,
        content_versions_csv=content_versions_csv,
        content_documents_csv=content_documents_csv,
        content_document_links_csv=content_document_links_csv,
        deal_metadata_csv=deal_metadata_csv,
        client_mapping_csv=client_mapping_csv,
        vendor_mapping_csv=vendor_mapping_csv,
        deal_mapping_csv=None,
    )

    converter = DocumentConverter(openai_client=None, enable_vision_analysis=False)
    pdf_parser, effective_backend = _init_pdf_parser(parser_backend, docling_kwargs={
        "ocr_mode": "on",
        "timeout_seconds": 240,
    })

    redaction_service = None
    if enable_redaction:
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            logger.error("❌ OPENAI_API_KEY is required for --enable-redaction in local harness")
            return False
        if not client_redaction_csv:
            logger.error("❌ --client-redaction-csv is required for --enable-redaction in local harness")
            return False
        from src.redaction.client_registry import ClientRegistry
        from src.redaction.llm_span_detector import LLMSpanDetector
        from src.redaction.redaction_service import RedactionService
        client_registry = ClientRegistry(client_redaction_csv)
        llm_detector = LLMSpanDetector(api_key=openai_api_key, model=redaction_model)
        redaction_service = RedactionService(client_registry=client_registry, llm_span_detector=llm_detector, strict_mode=True)

    manifest = {
        "run": {
            "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "discovery_file": discovery_file,
            "export_dir": export_dir,
            "parser_backend": effective_backend,
            "redaction_enabled": bool(enable_redaction),
            "client_redaction_csv": client_redaction_csv if enable_redaction else None,
            "redaction_model": redaction_model if enable_redaction else None,
            "docs_count": len(docs),
        },
        "documents": [],
        "stats": {
            "processed": 0,
            "failed": 0,
            "redaction_failed": 0,
            "total_client_replacements": 0,
            "total_person_replacements": 0,
            "total_email_replacements": 0,
            "total_phone_replacements": 0,
            "total_address_replacements": 0,
        },
    }

    from src.redaction.redaction_context import RedactionContext

    for i, doc_data in enumerate(docs, 1):
        doc_meta = _convert_discovery_doc_to_document_metadata(doc_data)
        file_info = doc_data.get("file_info", {}) or {}
        rel_path = file_info.get("path", "")
        file_name = file_info.get("name", "") or Path(rel_path).name
        file_type = (file_info.get("file_type", "") or "").lower()

        doc_dir = docs_out / f"{i:03d}_{doc_meta.salesforce_deal_id or doc_meta.deal_id or 'no_deal'}_{doc_meta.salesforce_content_version_id or 'no_cv'}"
        doc_dir.mkdir(parents=True, exist_ok=True)

        entry = {
            "index": i,
            "file_name": file_name,
            "file_type": file_type,
            "relative_path": rel_path,
            "deal_id": doc_meta.deal_id,
            "salesforce_deal_id": doc_meta.salesforce_deal_id,
            "salesforce_client_id": doc_meta.salesforce_client_id,
            "client_name": doc_meta.client_name,
            "vendor_name": doc_meta.vendor_name,
            "success": False,
            "errors": [],
            "redaction": None,
            "text_stats": {},
        }

        try:
            content = source.download_document(rel_path)
            processed_content, content_type = converter.convert_to_processable_content(rel_path, content, file_name)

            # Use PDF parser only for PDFs; otherwise DocumentConverter already returns a text-like form for some types
            if file_type == ".pdf" or file_name.lower().endswith(".pdf"):
                parsed = pdf_parser.parse(processed_content, doc_meta.__dict__, content_type)
            else:
                # For non-PDF: still parse via PDFPlumberParser interface for unified output if possible
                parsed = PDFPlumberParser().parse(processed_content, doc_meta.__dict__, content_type)

            original_text = (parsed.text or "") if parsed else ""
            (doc_dir / "original.txt").write_text(original_text, encoding="utf-8", errors="ignore")

            redacted_text = original_text
            redaction_result_dict = None
            if redaction_service:
                ctx = RedactionContext(
                    salesforce_client_id=doc_meta.salesforce_client_id,
                    client_name=doc_meta.client_name,
                    industry_label=None,
                    vendor_name=doc_meta.vendor_name,
                    file_type=doc_meta.file_type,
                    document_type=getattr(doc_meta, "document_type", None),
                )
                rr = redaction_service.redact(original_text, ctx)
                redacted_text = rr.redacted_text
                redaction_result_dict = {
                    "success": rr.success,
                    "validation_passed": rr.validation_passed,
                    "errors": rr.errors,
                    "validation_failures": rr.validation_failures,
                    "counts": {
                        "client": rr.client_replacements,
                        "person": rr.person_replacements,
                        "email": rr.email_replacements,
                        "phone": rr.phone_replacements,
                        "address": rr.address_replacements,
                    },
                    "model_used": rr.model_used,
                }
                if not rr.success or not rr.validation_passed:
                    manifest["stats"]["redaction_failed"] += 1
                    entry["errors"].append("Redaction failed (strict mode)")

                manifest["stats"]["total_client_replacements"] += rr.client_replacements
                manifest["stats"]["total_person_replacements"] += rr.person_replacements
                manifest["stats"]["total_email_replacements"] += rr.email_replacements
                manifest["stats"]["total_phone_replacements"] += rr.phone_replacements
                manifest["stats"]["total_address_replacements"] += rr.address_replacements

            (doc_dir / "redacted.txt").write_text(redacted_text, encoding="utf-8", errors="ignore")

            entry["text_stats"] = {
                "original_chars": len(original_text),
                "redacted_chars": len(redacted_text),
                "original_sha256": hashlib.sha256(original_text.encode("utf-8", errors="ignore")).hexdigest(),
                "redacted_sha256": hashlib.sha256(redacted_text.encode("utf-8", errors="ignore")).hexdigest(),
            }
            entry["redaction"] = redaction_result_dict
            entry["success"] = True
            manifest["stats"]["processed"] += 1
        except Exception as e:
            entry["errors"].append(str(e))
            manifest["stats"]["failed"] += 1

        manifest["documents"].append(entry)

    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info(f"✅ Local redaction harness complete. Output: {out}")
    logger.info(f"   Docs processed: {manifest['stats']['processed']}/{len(docs)}")
    return True

def verify_metadata_fields(namespace: str) -> bool:
    """Verify expected Pinecone metadata fields are present.

    Notes:
    - We do NOT store chunk text in Pinecone (neither in metadata nor “top-level”).
    - This function samples a few vectors to validate schema; it does not rely on text storage.
    """
    
    logger.info(f"🔍 Step 3: Verifying metadata fields in Pinecone...")
    
    try:
        settings = Settings()
        pc_client = PineconeDocumentClient(
            api_key=os.getenv("PINECONE_API_KEY"),
            index_name=settings.PINECONE_INDEX_NAME
        )
        
        # First, get an accurate vector count for the namespace (if available).
        # Then sample a few vectors to validate metadata schema.
        total_vectors: int | None = None
        try:
            stats = pc_client.index.describe_index_stats()
            # SDK may return dict or an object with .namespaces
            namespaces = getattr(stats, "namespaces", None)
            if namespaces is None and isinstance(stats, dict):
                namespaces = stats.get("namespaces", {})
            if isinstance(namespaces, dict) and namespace in namespaces:
                ns_obj = namespaces[namespace]
                total_vectors = getattr(ns_obj, "vector_count", None)
                if total_vectors is None and isinstance(ns_obj, dict):
                    total_vectors = ns_obj.get("vector_count")
        except Exception:
            # Non-fatal: stats may fail depending on permissions/SDK behavior.
            total_vectors = None

        # Query a few vectors to check metadata.
        # Note: Using 1024 dimensions to match multilingual-e5-large embeddings.
        results = pc_client.index.query(
            vector=[0.0] * 1024,  # Dummy vector (1024 dim for multilingual-e5-large)
            top_k=5,
            namespace=namespace,
            include_metadata=True
        )
        
        if not results.matches:
            logger.warning("⚠️  No vectors found in namespace. Processing may have failed.")
            return False
        
        if total_vectors is not None:
            logger.info(f"   Namespace vector count: {total_vectors}")
            logger.info(f"   Sampled {len(results.matches)} vectors for schema check")
        else:
            logger.info(f"   Sampled {len(results.matches)} vectors for schema check")
        
        # Check metadata fields in first result
        first_match = results.matches[0]
        metadata = first_match.metadata if hasattr(first_match, 'metadata') else {}
        
        # Expected fields (align with Pinecone upsert metadata schema).
        expected_fields = {
            # Core document (3)
            "file_name",
            "file_type",
            "deal_creation_date",

            # Identifiers (4)
            "deal_id",
            "salesforce_deal_id",
            "salesforce_client_id",
            "salesforce_vendor_id",

            # Financial (6)
            "final_amount",
            "savings_1yr",
            "savings_3yr",
            "savings_achieved",
            "fixed_savings",
            "savings_target_full_term",

            # Contract (3)
            "contract_term",
            "contract_start",
            "contract_end",

            # Processing (1)
            "chunk_index",

            # Search (2)
            "client_name",
            "vendor_name",

            # Quality (2)
            "has_parsing_errors",
            "deal_status",

            # Email (1)
            "email_has_attachments",

            # Deal Classification (8)
            "report_type",
            "description",
            "project_type",
            "competition",
            "npi_analyst",
            "dual_multi_sourcing",
            "time_pressure",
            "advisor_network_used",
        }
        
        found_fields = set(metadata.keys())
        missing_fields = expected_fields - found_fields
        
        logger.info(f"   Metadata fields found: {len(found_fields)}")
        logger.info(f"   Expected fields: {len(expected_fields)}")
        
        if missing_fields:
            logger.warning(f"   ⚠️  Missing fields ({len(missing_fields)}): {', '.join(list(missing_fields)[:5])}...")
        else:
            logger.info("   ✅ All expected fields present!")
        
        # Specifically check the 8 new fields
        new_fields = ['report_type', 'description', 'project_type', 'competition',
                     'npi_analyst', 'dual_multi_sourcing', 'time_pressure', 'advisor_network_used']
        found_new_fields = [f for f in new_fields if f in metadata]
        
        logger.info(f"\n   📊 New Deal Classification Fields:")
        logger.info(f"      Found: {len(found_new_fields)}/8")
        for field in new_fields:
            value = metadata.get(field, 'MISSING')
            status = "✅" if field in metadata else "❌"
            logger.info(f"      {status} {field}: {str(value)[:50]}")
        
        return len(found_new_fields) == 8
        
    except Exception as e:
        logger.error(f"❌ Error verifying metadata: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description='Test 10 files with metadata updates')
    parser.add_argument('--export-dir', type=str, 
                       default='/Volumes/Jeff_2TB/WE_00D80000000aWoiEAE_1',
                       help='Salesforce export directory')
    parser.add_argument('--discovery-file', type=str,
                       default='test_10files_metadata_discovery.json',
                       help='Output discovery file')
    parser.add_argument('--namespace', type=str,
                       default='test-metadata-dec2025',
                       help='Pinecone namespace for test')
    parser.add_argument('--skip-discovery', action='store_true',
                       help='Skip discovery step (use existing file)')
    parser.add_argument('--skip-processing', action='store_true',
                       help='Skip processing step (only verify)')
    parser.add_argument('--max-docs', type=int, default=10,
                       help='Maximum number of documents to discover (default: 10)')
    parser.add_argument('--sample-deals', type=int, default=10,
                       help='Randomly sample this many deals from discovery and include all their docs (default: 10)')
    parser.add_argument('--seed', type=int, default=1337,
                       help='Random seed for deal sampling (default: 1337)')
    parser.add_argument('--max-files-per-deal', type=int, default=0,
                       help='If >0, cap docs per sampled deal to this many (default: 0 = no cap)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of documents to process (default: same as --max-docs)')
    parser.add_argument('--workers', type=int, default=6,
                       help='Number of parallel workers for processing (default: 6)')
    parser.add_argument('--enable-redaction', action='store_true',
                       help='Enable redaction stage during processing')
    parser.add_argument('--client-redaction-csv', type=str, default=str(Path("src/redaction/SF-Cust-Mapping.csv")),
                       help='Path to client mapping CSV for redaction (default: src/redaction/SF-Cust-Mapping.csv)')
    parser.add_argument('--redaction-model', type=str, default="gpt-5-mini-2025-08-07",
                       help='OpenAI model for redaction span detection (default: gpt-5-mini-2025-08-07)')
    parser.add_argument('--local-redaction-only', action='store_true',
                       help='Run local-only extraction+redaction and write before/after outputs (no Pinecone upsert)')
    parser.add_argument('--output-dir', type=str, default="output",
                       help='Base output directory for local redaction-only runs (default: output)')
    parser.add_argument('--skip-verify', action='store_true',
                       help='Skip Pinecone metadata verification step')
    
    args = parser.parse_args()
    
    # If limit not specified, use max_docs
    if args.limit is None:
        args.limit = args.max_docs
    
    logger.info("=" * 60)
    logger.info(f"🧪 Testing {args.max_docs} Files with Updated Metadata Schema")
    logger.info("=" * 60)
    logger.info(f"   Export Dir: {args.export_dir}")
    logger.info(f"   Discovery File: {args.discovery_file}")
    logger.info(f"   Namespace: {args.namespace}")
    logger.info(f"   Max Docs (discovery): {args.max_docs}")
    logger.info(f"   Sample Deals: {args.sample_deals} (seed={args.seed})")
    if args.max_files_per_deal and args.max_files_per_deal > 0:
        logger.info(f"   Max Files/Deal: {args.max_files_per_deal}")
    logger.info(f"   Redaction: {'ENABLED' if args.enable_redaction else 'disabled'}")
    if args.enable_redaction:
        logger.info(f"   Client CSV: {args.client_redaction_csv}")
        logger.info(f"   Redaction model: {args.redaction_model}")
    if args.local_redaction_only:
        logger.info("   Mode: local-redaction-only (no Pinecone upsert)")
    logger.info(f"   Process Limit: {args.limit}")
    logger.info(f"   Workers: {args.workers}")
    logger.info("")
    
    success = True
    
    # Step 1: Create discovery
    if not args.skip_discovery:
        if not create_test_discovery(args.export_dir, args.discovery_file, args.max_docs):
            logger.error("❌ Discovery failed")
            return 1
    else:
        logger.info("⏭️  Skipping discovery (using existing file)")
    
    # Optional: sample deals + their docs from the discovery file (writes a new discovery file)
    sampled_discovery_file = args.discovery_file
    if args.sample_deals and args.sample_deals > 0:
        sampled_discovery_file = str(
            Path(args.discovery_file).with_suffix("").as_posix()
            + f"_sample_{args.sample_deals}_deals.json"
        )
        max_files_per_deal = args.max_files_per_deal if args.max_files_per_deal and args.max_files_per_deal > 0 else None
        if not sample_discovery_by_deals(
            discovery_file=args.discovery_file,
            output_file=sampled_discovery_file,
            sample_deals=args.sample_deals,
            seed=args.seed,
            max_files_per_deal=max_files_per_deal,
        ):
            logger.error("❌ Deal sampling failed")
            return 1

    # Step 2: Process files
    if not args.skip_processing:
        if args.local_redaction_only:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_dir = Path(args.output_dir) / f"redaction_harness_{timestamp}"
            if not run_local_redaction_harness(
                discovery_file=sampled_discovery_file,
                export_dir=args.export_dir,
                output_dir=str(out_dir),
                parser_backend=args.parser_backend if hasattr(args, "parser_backend") else "docling",
                enable_redaction=args.enable_redaction,
                client_redaction_csv=args.client_redaction_csv,
                redaction_model=args.redaction_model,
            ):
                logger.error("❌ Local redaction harness failed")
                return 1
        else:
            if not process_test_files(
                sampled_discovery_file,
                args.namespace,
                args.export_dir,
                args.limit,
                args.workers,
                enable_redaction=args.enable_redaction,
                client_redaction_csv=args.client_redaction_csv,
                redaction_model=args.redaction_model,
            ):
                logger.error("❌ Processing failed")
                return 1
    else:
        logger.info("⏭️  Skipping processing (verification only)")
    
    # Step 3: Verify metadata
    if args.skip_verify or args.local_redaction_only:
        logger.info("⏭️  Skipping Pinecone verification")
    else:
        if not verify_metadata_fields(args.namespace):
            logger.error("❌ Metadata verification failed")
            return 1
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ Test Complete!")
    logger.info("=" * 60)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())

