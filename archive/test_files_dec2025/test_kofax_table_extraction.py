#!/usr/bin/env python3
"""
Test script to inspect table extraction from Kofax PDF.
Shows what's in the lossless JSON and how tables are being extracted.
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from src.parsers.docling_parser import DoclingParser

def test_kofax_extraction():
    """Extract tables from Kofax PDF and show detailed information."""
    
    pdf_path = '/Volumes/Jeff_2TB/pre_2010_files/documents/general/BNYM Kofax KTA Configuration Revised_9M_2018_Final.pdf'
    
    if not Path(pdf_path).exists():
        print(f"❌ File not found: {pdf_path}")
        return
    
    print("=" * 80)
    print("Testing Table Extraction from Kofax PDF")
    print("=" * 80)
    print()
    
    # Read PDF
    pdf_bytes = Path(pdf_path).read_bytes()
    
    # Initialize parser with OCR enabled
    parser = DoclingParser(ocr=True, timeout_seconds=240)
    
    # Parse PDF
    print("📄 Parsing PDF with Docling...")
    metadata = {"name": Path(pdf_path).name, "path": pdf_path}
    result = parser.parse(pdf_bytes, metadata, "pdf")
    
    print(f"✅ Parsing complete")
    print(f"   Text length: {len(result.text)} characters")
    print(f"   Tables found: {len(result.tables) if result.tables else 0}")
    print()
    
    # Check metadata for table info
    print("📊 Parser Metadata:")
    for key, value in result.metadata.items():
        if 'table' in key.lower():
            print(f"   {key}: {value}")
    print()
    
    # Show table details
    if result.tables:
        print("=" * 80)
        print("EXTRACTED TABLES:")
        print("=" * 80)
        for i, table in enumerate(result.tables):
            print(f"\nTable {i+1}:")
            print(f"   Page: {table.get('page', 'N/A')}")
            print(f"   Rows: {table.get('rows', 'N/A')}")
            print(f"   Cols: {table.get('cols', 'N/A')}")
            print(f"   Contains Currency: {table.get('contains_currency', False)}")
            
            # Show first few rows
            data = table.get('data', [])
            if data:
                print(f"\n   First 3 rows:")
                for row_idx, row in enumerate(data[:3]):
                    print(f"      Row {row_idx}: {row[:5]}...")  # First 5 columns
    else:
        print("⚠️  No tables found in result.tables")
    
    # Check if tables are in the markdown text
    print()
    print("=" * 80)
    print("CHECKING MARKDOWN TEXT FOR TABLES:")
    print("=" * 80)
    
    markdown_lines = result.text.split('\n')
    table_lines = [line for line in markdown_lines if '|' in line or 'TABLE' in line.upper()]
    
    if table_lines:
        print(f"\nFound {len(table_lines)} lines with table markers:")
        print("\nFirst 20 table-related lines:")
        for i, line in enumerate(table_lines[:20]):
            print(f"   {i+1}: {line[:100]}...")
    else:
        print("\n⚠️  No table markers found in markdown text")
    
    # Try to get lossless JSON from the parser (we'll need to modify parser temporarily)
    print()
    print("=" * 80)
    print("INSPECTING LOSSLESS JSON:")
    print("=" * 80)
    
    # Re-parse to get lossless JSON
    from docling.document_converter import DocumentConverter as DoclingDocumentConverter
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode, OcrOptions
    from docling.document_converter import PdfFormatOption
    import tempfile
    
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.images_scale = 2.0
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
    pipeline_options.table_structure_options.do_cell_matching = True
    pipeline_options.ocr_options = OcrOptions(
        lang=["en"],
        force_full_page_ocr=True,
        bitmap_area_threshold=0.0,
        use_gpu=True,
    )
    
    converter = DoclingDocumentConverter(
        format_options={PdfFormatOption: pipeline_options}
    )
    
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
        tmp_file.write(pdf_bytes)
        tmp_path = tmp_file.name
    
    try:
        result_doc = converter.convert(tmp_path)
        doc = result_doc.document
        
        # Get lossless JSON
        lossless_json = {}
        try:
            if hasattr(doc, "export_to_dict_json"):
                lossless_json = json.loads(doc.export_to_dict_json())
            elif hasattr(doc, "export_to_dict"):
                lossless_json = doc.export_to_dict()
        except Exception as e:
            print(f"❌ Failed to export lossless JSON: {e}")
        
        if lossless_json:
            print(f"\n✅ Lossless JSON keys: {list(lossless_json.keys())}")
            
            # Check the 'tables' key directly
            if 'tables' in lossless_json:
                tables_data = lossless_json['tables']
                print(f"\n📊 Found 'tables' key in JSON:")
                print(f"   Type: {type(tables_data)}")
                if isinstance(tables_data, list):
                    print(f"   Count: {len(tables_data)} tables")
                    for i, table in enumerate(tables_data[:3]):  # Show first 3
                        print(f"\n   Table {i+1}:")
                        print(f"      Keys: {list(table.keys()) if isinstance(table, dict) else 'not a dict'}")
                        if isinstance(table, dict):
                            # Show structure
                            for key in ['type', 'kind', 'cells', 'rows', 'data', 'page', 'page_number', 'pageIndex']:
                                if key in table:
                                    value = table[key]
                                    if key in ['cells', 'rows', 'data'] and isinstance(value, list):
                                        print(f"      {key}: list with {len(value)} items")
                                        if value:
                                            if isinstance(value[0], list):
                                                print(f"         First item has {len(value[0])} columns")
                                                print(f"         Sample first row: {value[0][:3]}")
                                            elif isinstance(value[0], dict):
                                                print(f"         First item is dict with keys: {list(value[0].keys())[:5]}")
                                    else:
                                        print(f"      {key}: {value}")
                            
                            # Deep dive into 'data' structure
                            if 'data' in table:
                                data = table['data']
                                print(f"\n      🔍 Deep dive into 'data' structure:")
                                print(f"         Type: {type(data)}")
                                if isinstance(data, list):
                                    print(f"         Length: {len(data)}")
                                    if data:
                                        print(f"         First item type: {type(data[0])}")
                                        if isinstance(data[0], list):
                                            print(f"         First row: {data[0]}")
                                        elif isinstance(data[0], dict):
                                            print(f"         First item keys: {list(data[0].keys())}")
                                            print(f"         First item sample: {str(data[0])[:200]}")
                elif isinstance(tables_data, dict):
                    print(f"   Tables is a dict with keys: {list(tables_data.keys())}")
                else:
                    print(f"   Tables value: {tables_data}")
            
            # Also check 'body' and 'groups' for nested tables
            print(f"\n🔍 Checking 'body' key:")
            if 'body' in lossless_json:
                body = lossless_json['body']
                if isinstance(body, list):
                    print(f"   Body is a list with {len(body)} items")
                    # Check first few items
                    for i, item in enumerate(body[:3]):
                        if isinstance(item, dict):
                            item_type = item.get('type') or item.get('kind', 'unknown')
                            print(f"   Item {i}: type={item_type}, keys={list(item.keys())[:5]}")
                elif isinstance(body, dict):
                    print(f"   Body is a dict with keys: {list(body.keys())}")
            
            # Search for table-related keys recursively
            def find_tables(obj, path="", depth=0):
                """Recursively find table structures in JSON"""
                if depth > 5:  # Limit depth
                    return []
                
                tables_found = []
                if isinstance(obj, dict):
                    node_type = obj.get("type") or obj.get("kind")
                    if node_type and "table" in str(node_type).lower():
                        tables_found.append((path, node_type, obj))
                    if "cells" in obj or "rows" in obj:
                        tables_found.append((path, "table-like", obj))
                    
                    for key, value in obj.items():
                        tables_found.extend(find_tables(value, f"{path}.{key}", depth+1))
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        tables_found.extend(find_tables(item, f"{path}[{i}]", depth+1))
                
                return tables_found
            
            tables_in_json = find_tables(lossless_json)
            print(f"\n📊 Found {len(tables_in_json)} table-like structures in JSON (recursive search):")
            for path, node_type, obj in tables_in_json[:5]:  # Show first 5
                print(f"   {path}: type={node_type}")
                if "cells" in obj:
                    cells = obj["cells"]
                    if isinstance(cells, list) and cells:
                        print(f"      -> Has {len(cells)} rows")
                        if cells[0]:
                            print(f"      -> First row has {len(cells[0])} columns")
                elif "rows" in obj:
                    rows = obj["rows"]
                    if isinstance(rows, list):
                        print(f"      -> Has {len(rows)} rows")
        else:
            print("❌ Lossless JSON is empty")
    
    finally:
        import os
        os.unlink(tmp_path)
    
    print()
    print("=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    print(f"✅ Parsed successfully")
    print(f"   Tables in result.tables: {len(result.tables) if result.tables else 0}")
    print(f"   Table markers in text: {len(table_lines)}")
    print(f"   Total text length: {len(result.text)} chars")

if __name__ == "__main__":
    test_kofax_extraction()

