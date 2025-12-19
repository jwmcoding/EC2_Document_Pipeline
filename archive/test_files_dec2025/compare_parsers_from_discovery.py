#!/usr/bin/env python3
"""
Compare PDF extraction results across PDFPlumber, Docling, and Mistral parsers.

Reads a discovery JSON file and runs all three parsers on each PDF document,
saving outputs for side-by-side comparison.

Usage:
    # For salesforce_raw source (requires export root directory):
    python compare_parsers_from_discovery.py \
        --input test_easyocr_dec10/test_10files_easyocr_discovery.json \
        --output-dir parser_comparison_results \
        --export-root-dir /Volumes/Jeff_2TB/day_20251202_053804-04f8f8_29771
    
    # For local source (files must exist on disk):
    python compare_parsers_from_discovery.py \
        --input discovery.json \
        --output-dir parser_comparison_results

Note: For salesforce_raw source, ensure the export_root_dir contains the actual
files referenced in the discovery JSON. ContentVersion IDs must match.
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dotenv import load_dotenv
load_dotenv()

from src.parsers.pdfplumber_parser import PDFPlumberParser
from src.parsers.docling_parser import DoclingParser, is_docling_available
from src.parsers.mistral_parser import MistralParser, is_mistral_available
from src.connectors.raw_salesforce_export_connector import RawSalesforceExportConnector
from src.connectors.local_filesystem_client import LocalFilesystemClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare PDF extraction across multiple parsers using discovery JSON"
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to discovery JSON file",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="parser_comparison_results",
        help="Output directory for comparison results (default: parser_comparison_results)",
    )
    parser.add_argument(
        "--export-root-dir",
        type=str,
        help="Salesforce export root directory (required for salesforce_raw source)",
    )
    parser.add_argument(
        "--parsers",
        nargs="+",
        choices=["pdfplumber", "docling", "mistral", "all"],
        default=["all"],
        help="Which parsers to run (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout seconds for Docling/Mistral (default: 300)",
    )
    parser.add_argument(
        "--filter-pdf-only",
        action="store_true",
        help="Only process PDF files (skip Excel, Word, etc.)",
    )
    return parser.parse_args()


def get_file_content(
    doc: Dict[str, Any],
    source_type: str,
    export_root_dir: Optional[str] = None,
    file_name: Optional[str] = None
) -> Optional[bytes]:
    """Get file content bytes from discovery document."""
    
    file_info = doc.get("file_info", {})
    doc_path = file_info.get("path", "")
    file_name = file_info.get("name", "")
    
    if source_type == "salesforce_raw":
        if not export_root_dir:
            # Try to get from discovery metadata
            raise ValueError(
                "export_root_dir required for salesforce_raw source. "
                "Provide via --export-root-dir or ensure discovery_metadata.source_path is set."
            )
        
        # Use the connector's download_file method which handles path resolution
        # Paths in discovery JSON are like "ContentVersion/0680y0000035WqaAAE"
        # The connector knows how to find files in ContentVersions/VersionData/<id>/
        try:
            # The connector needs CSV files for initialization, but we can use minimal setup
            # For download_file, we just need the export_root_dir
            # Let's construct the path directly based on connector's logic
            export_root = Path(export_root_dir)
            
            # Extract ContentVersion ID from path
            if doc_path.startswith("ContentVersion/"):
                cv_id = doc_path.replace("ContentVersion/", "").strip()
            else:
                cv_id = doc_path.split("/")[-1] if "/" in doc_path else doc_path
            
            # Try ContentVersions/VersionData/<cv_id>/ directory (primary location)
            version_data_dir = export_root / "ContentVersions" / "VersionData" / cv_id
            if version_data_dir.exists() and version_data_dir.is_dir():
                # Find the actual file in this directory
                files = [f for f in version_data_dir.iterdir() if f.is_file() and not f.name.startswith('.')]
                if files:
                    return files[0].read_bytes()
            
            # Fallback: try direct path
            full_path = export_root / doc_path
            if full_path.exists():
                return full_path.read_bytes()
            
            # Last resort: search for file by name in VersionData directories
            search_name = file_name or file_info.get("name", "")
            if search_name:
                version_data_base = export_root / "ContentVersions" / "VersionData"
                if version_data_base.exists():
                    # Search recursively for matching filename
                    for version_dir in version_data_base.iterdir():
                        if version_dir.is_dir():
                            matching_files = list(version_dir.glob(search_name))
                            if matching_files:
                                return matching_files[0].read_bytes()
            
            print(f"   ⚠️  File not found for path: {doc_path} (CV ID: {cv_id})")
            print(f"      Searched: {version_data_dir}")
            return None
        except Exception as e:
            print(f"   ⚠️  Error reading {file_name}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    elif source_type == "local":
        # Direct file path
        if Path(doc_path).exists():
            return Path(doc_path).read_bytes()
        else:
            print(f"   ⚠️  File not found: {doc_path}")
            return None
    
    else:
        print(f"   ⚠️  Unsupported source type: {source_type}")
        return None


def run_parser(
    label: str,
    parser_obj,
    content: bytes,
    metadata: Dict[str, Any],
    content_type: str
) -> tuple[Any, str, float]:
    """Run a parser and return result, text, and elapsed time."""
    start = time.time()
    try:
        parsed = parser_obj.parse(content, metadata, content_type=content_type)
        text = parsed.text if parsed else ""
        elapsed = time.time() - start
        return parsed, text, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"      ❌ Error: {e}")
        return None, "", elapsed


def save_extraction_output(
    output_dir: Path,
    file_name: str,
    parser_label: str,
    parsed_result: Any,
    text: str,
    elapsed_time: float,
    file_size: int
):
    """Save extraction output to markdown file."""
    safe_name = file_name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
    output_file = output_dir / f"{safe_name}.{parser_label}.md"
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"# {file_name} - {parser_label.upper()} Extraction\n\n")
        f.write(f"**Processing Time**: {elapsed_time:.2f}s\n")
        f.write(f"**File Size**: {file_size / 1024:.1f} KB\n")
        f.write(f"**Parser**: {parser_label}\n")
        f.write(f"**Platform**: Local\n\n")
        
        if parsed_result:
            page_count = parsed_result.metadata.get("page_count", "Unknown") if parsed_result.metadata else "Unknown"
            table_count = len(parsed_result.tables) if parsed_result.tables else 0
            f.write(f"**Pages**: {page_count}\n")
            f.write(f"**Tables Found**: {table_count}\n\n")
        
        f.write("---\n\n")
        f.write("## Extracted Content\n\n")
        f.write(text if text else "*No content extracted*\n")


def main():
    args = parse_args()
    
    # Normalize parser selection
    if "all" in args.parsers:
        parsers_to_run = ["pdfplumber", "docling", "mistral"]
    else:
        parsers_to_run = args.parsers
    
    print("=" * 80)
    print("🔬 PARSER COMPARISON FROM DISCOVERY JSON")
    print("=" * 80)
    print(f"Input discovery file: {args.input}")
    print(f"Output directory: {args.output_dir}")
    print(f"Parsers to run: {', '.join(parsers_to_run)}")
    print()
    
    # Check parser availability
    print("Parser availability:")
    print(f"  PDFPlumber: ✅ Always available")
    print(f"  Docling: {'✅ Available' if is_docling_available() else '❌ Not available'}")
    print(f"  Mistral: {'✅ Available' if is_mistral_available() else '❌ Not available (MISTRAL_API_KEY not set)'}")
    print()
    
    # Load discovery JSON
    discovery_path = Path(args.input)
    if not discovery_path.exists():
        print(f"❌ Discovery file not found: {args.input}")
        return
    
    with open(discovery_path, "r") as f:
        discovery_data = json.load(f)
    
    discovery_meta = discovery_data.get("discovery_metadata", {})
    source_type = discovery_meta.get("source_type", "unknown")
    source_path = discovery_meta.get("source_path", "")
    documents = discovery_data.get("documents", [])
    
    print(f"📄 Discovery file loaded:")
    print(f"   Source type: {source_type}")
    print(f"   Source path: {source_path or '(empty)'}")
    print(f"   Total documents: {len(documents)}")
    print()
    
    # Determine export root directory
    export_root_dir = args.export_root_dir or source_path
    if source_type == "salesforce_raw" and not export_root_dir:
        print("❌ Error: export_root_dir required for salesforce_raw source")
        print("   Provide via --export-root-dir argument")
        return
    
    # Filter to PDFs only if requested
    if args.filter_pdf_only:
        documents = [d for d in documents if d.get("file_info", {}).get("file_type", "").lower() == ".pdf"]
        print(f"📋 Filtered to PDFs only: {len(documents)} documents")
        print()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize parsers
    pdfplumber_parser = PDFPlumberParser()
    docling_parser = None
    mistral_parser = None
    
    if "docling" in parsers_to_run and is_docling_available():
        try:
            docling_parser = DoclingParser(ocr=True, timeout_seconds=args.timeout)
            print("✅ Docling parser initialized")
        except Exception as e:
            print(f"⚠️  Failed to initialize Docling parser: {e}")
            parsers_to_run = [p for p in parsers_to_run if p != "docling"]
    
    if "mistral" in parsers_to_run and is_mistral_available():
        try:
            mistral_parser = MistralParser(timeout_seconds=args.timeout)
            print("✅ Mistral parser initialized")
        except Exception as e:
            print(f"⚠️  Failed to initialize Mistral parser: {e}")
            parsers_to_run = [p for p in parsers_to_run if p != "mistral"]
    
    print()
    
    # Process documents
    results = []
    total_start = time.time()
    
    for i, doc in enumerate(documents, 1):
        file_info = doc.get("file_info", {})
        file_name = file_info.get("name", "unknown")
        file_type = file_info.get("file_type", "").lower()
        file_size = file_info.get("size", 0)
        
        # Skip non-PDFs if filter is enabled
        if args.filter_pdf_only and file_type != ".pdf":
            continue
        
        print(f"📄 [{i}/{len(documents)}] Processing: {file_name}")
        print(f"   Type: {file_type}, Size: {file_size / 1024:.1f} KB")
        
        # Get file content
        content = get_file_content(doc, source_type, export_root_dir, file_name)
        if content is None:
            print(f"   ❌ Could not retrieve file content")
            results.append({
                "file": file_name,
                "type": file_type,
                "size_kb": file_size / 1024,
                "status": "failed_to_load",
                "parsers": {}
            })
            continue
        
        # Prepare metadata
        metadata = {
            "file_name": file_name,
            "file_type": file_type,
            "file_size": file_size,
            "name": file_name,
            "path": file_info.get("path", ""),
        }
        
        # Determine content type
        if file_type == ".pdf":
            content_type = "pdf"
        elif file_type in [".xlsx", ".xls"]:
            content_type = "excel"
        elif file_type == ".docx":
            content_type = "docx"
        else:
            content_type = "pdf"  # Default
        
        doc_results = {
            "file": file_name,
            "type": file_type,
            "size_kb": file_size / 1024,
            "parsers": {}
        }
        
        # Run each parser
        for parser_label in parsers_to_run:
            parser_obj = None
            
            if parser_label == "pdfplumber":
                parser_obj = pdfplumber_parser
            elif parser_label == "docling" and docling_parser:
                parser_obj = docling_parser
            elif parser_label == "mistral" and mistral_parser:
                parser_obj = mistral_parser
            
            if parser_obj is None:
                continue
            
            # Only run PDF parsers on PDFs
            if file_type == ".pdf" or parser_label == "pdfplumber":
                print(f"   🔧 Running {parser_label}...", end=" ", flush=True)
                parsed, text, elapsed = run_parser(parser_label, parser_obj, content, metadata, content_type)
                
                # Save output
                save_extraction_output(
                    output_dir, file_name, parser_label, parsed, text, elapsed, file_size
                )
                
                doc_results["parsers"][parser_label] = {
                    "time": elapsed,
                    "text_length": len(text),
                    "table_count": len(parsed.tables) if parsed and parsed.tables else 0,
                    "status": "success" if parsed else "failed"
                }
                
                status_icon = "✅" if parsed else "❌"
                print(f"{status_icon} {elapsed:.2f}s ({len(text)} chars, {doc_results['parsers'][parser_label]['table_count']} tables)")
        
        results.append(doc_results)
        print()
    
    total_time = time.time() - total_start
    
    # Generate summary report
    print("=" * 80)
    print("📊 COMPARISON SUMMARY")
    print("=" * 80)
    print()
    
    # Per-parser statistics
    for parser_label in parsers_to_run:
        parser_results = [
            r["parsers"].get(parser_label, {})
            for r in results
            if parser_label in r["parsers"]
        ]
        
        if not parser_results:
            continue
        
        successful = [r for r in parser_results if r.get("status") == "success"]
        if not successful:
            continue
        
        avg_time = sum(r["time"] for r in successful) / len(successful)
        avg_text_len = sum(r["text_length"] for r in successful) / len(successful)
        avg_tables = sum(r["table_count"] for r in successful) / len(successful)
        total_tables = sum(r["table_count"] for r in successful)
        
        print(f"{parser_label.upper()}:")
        print(f"  Files processed: {len(successful)}/{len(parser_results)}")
        print(f"  Average time: {avg_time:.2f}s")
        print(f"  Average text length: {avg_text_len:,.0f} chars")
        print(f"  Average tables: {avg_tables:.1f}")
        print(f"  Total tables found: {total_tables}")
        print()
    
    # Save summary report
    summary_file = output_dir / "COMPARISON_SUMMARY.md"
    with open(summary_file, "w") as f:
        f.write("# Parser Comparison Summary\n\n")
        f.write(f"**Discovery File**: {args.input}\n")
        f.write(f"**Total Documents**: {len(documents)}\n")
        f.write(f"**PDFs Processed**: {len([r for r in results if r['type'] == '.pdf'])}\n")
        f.write(f"**Total Processing Time**: {total_time:.2f}s\n")
        f.write(f"**Parsers Used**: {', '.join(parsers_to_run)}\n\n")
        
        f.write("## Per-Parser Statistics\n\n")
        for parser_label in parsers_to_run:
            parser_results = [
                r["parsers"].get(parser_label, {})
                for r in results
                if parser_label in r["parsers"]
            ]
            successful = [r for r in parser_results if r.get("status") == "success"]
            if not successful:
                continue
            
            avg_time = sum(r["time"] for r in successful) / len(successful)
            avg_text_len = sum(r["text_length"] for r in successful) / len(successful)
            avg_tables = sum(r["table_count"] for r in successful) / len(successful)
            total_tables = sum(r["table_count"] for r in successful)
            
            f.write(f"### {parser_label.upper()}\n\n")
            f.write(f"- Files processed: {len(successful)}/{len(parser_results)}\n")
            f.write(f"- Average time: {avg_time:.2f}s\n")
            f.write(f"- Average text length: {avg_text_len:,.0f} chars\n")
            f.write(f"- Average tables per file: {avg_tables:.1f}\n")
            f.write(f"- Total tables found: {total_tables}\n\n")
        
        f.write("## Detailed Results\n\n")
        f.write("| File | Type | Size | PDFPlumber | Docling | Mistral |\n")
        f.write("|------|------|------|------------|---------|---------|\n")
        
        for r in results:
            if r["type"] != ".pdf":
                continue
            
            pdfplumber_info = r["parsers"].get("pdfplumber", {})
            docling_info = r["parsers"].get("docling", {})
            mistral_info = r["parsers"].get("mistral", {})
            
            pdfplumber_str = f"{pdfplumber_info.get('time', 0):.2f}s ({pdfplumber_info.get('table_count', 0)} tables)" if pdfplumber_info else "-"
            docling_str = f"{docling_info.get('time', 0):.2f}s ({docling_info.get('table_count', 0)} tables)" if docling_info else "-"
            mistral_str = f"{mistral_info.get('time', 0):.2f}s ({mistral_info.get('table_count', 0)} tables)" if mistral_info else "-"
            
            f.write(f"| {r['file'][:40]} | {r['type']} | {r['size_kb']:.1f}KB | {pdfplumber_str} | {docling_str} | {mistral_str} |\n")
    
    print(f"✅ Results saved to: {output_dir}")
    print(f"📄 Summary report: {summary_file}")
    print("Done!")


if __name__ == "__main__":
    main()

