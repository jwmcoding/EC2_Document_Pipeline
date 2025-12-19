#!/usr/bin/env python3
"""
Local test script to compare extraction results with EC2.
Processes the same 10 files using local Docling setup.
"""

import argparse
import os
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

load_dotenv()

from src.parsers.docling_parser import DoclingParser, is_docling_available
from src.parsers.pdfplumber_parser import PDFPlumberParser
from src.parsers.mistral_parser import MistralParser, is_mistral_available


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local extraction benchmark")
    parser.add_argument(
        "--parser",
        choices=["docling", "pdfplumber", "mistral", "all"],
        default="all",
        help="Which parser to run for PDFs (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout seconds for Docling/Mistral PDF parsing (default: 300)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print("🧪 LOCAL TEST - Multi-parser Comparison")
    print("=" * 70)
    print(f"Docling available: {is_docling_available()}")
    print(f"Mistral available: {is_mistral_available()}")
    print(f"Selected parser mode: {args.parser}")
    print()

    # Directory setup
    input_dir = Path(__file__).parent / "original_files"
    output_dir = Path(__file__).parent

    files = sorted(input_dir.glob("*"))
    print(f"Found {len(files)} files to process")
    print()

    # Initialize parsers on demand
    docling_parser = None
    mistral_parser = None
    pdfplumber_parser = PDFPlumberParser()

    if args.parser in ("docling", "all"):
        if not is_docling_available():
            print("⚠️ Docling requested but not available; will skip Docling runs.")
        else:
            docling_parser = DoclingParser(ocr=True, timeout_seconds=args.timeout)

    if args.parser in ("mistral", "all"):
        if not is_mistral_available():
            print("⚠️ Mistral requested but MISTRAL_API_KEY is not set; will skip Mistral runs.")
        else:
            mistral_parser = MistralParser(timeout_seconds=args.timeout)

    results = []
    total_start = time.time()

    def run_parser(label: str, parser_obj, content: bytes, metadata: dict, content_type: str):
        start = time.time()
        parsed = parser_obj.parse(content, metadata, content_type=content_type)
        text = parsed.text if parsed else ""
        elapsed = time.time() - start
        return parsed, text, elapsed

    for i, file_path in enumerate(files, 1):
        file_name = file_path.name
        file_size = file_path.stat().st_size
        file_type = file_path.suffix.lower()

        print(f"📄 Processing {i}/{len(files)}: {file_name}")
        print(f"   Size: {file_size / 1024:.1f} KB, Type: {file_type}")

        try:
            with open(file_path, "rb") as f:
                content = f.read()

            metadata = {
                "file_name": file_name,
                "file_type": file_type,
                "file_size": file_size,
                "name": file_name,
                "path": str(file_path),
            }

            # Determine which parsers to run for this file
            parser_runs = []
            if file_type == ".pdf":
                if args.parser in ("pdfplumber", "all"):
                    parser_runs.append(("pdfplumber", pdfplumber_parser, "pdf"))
                if args.parser in ("docling", "all") and docling_parser:
                    parser_runs.append(("docling", docling_parser, "pdf"))
                if args.parser in ("mistral", "all") and mistral_parser:
                    parser_runs.append(("mistral", mistral_parser, "pdf"))
            elif file_type in [".xlsx", ".xls"]:
                # Only PDFPlumber handles Excel via pandas in this script
                parser_runs.append(("pdfplumber_excel", pdfplumber_parser, "excel"))
            else:
                parser_runs.append(("none", None, None))

            for label, parser_obj, ctype in parser_runs:
                if parser_obj is None:
                    text = ""
                    elapsed = 0.0
                    status = "skipped"
                    parsed = None
                else:
                    parsed, text, elapsed = run_parser(label, parser_obj, content, metadata, ctype)
                    status = "success"

                safe_name = file_name.replace(" ", "_").replace("(", "").replace(")", "")
                output_file = output_dir / f"{safe_name}.{label}.md"

                with open(output_file, "w") as f:
                    f.write(f"# LOCAL Extraction: {file_name}\n\n")
                    f.write(f"**Processing Time**: {elapsed:.2f}s\n")
                    f.write(f"**File Size**: {file_size / 1024:.1f} KB\n")
                    f.write(f"**Parser**: {label}\n")
                    f.write(f"**Platform**: Local Mac (MPS)\n\n")
                    f.write("---\n\n")
                    f.write("## Extracted Content\n\n")
                    f.write(text if text else "*No content extracted*\n")

                results.append(
                    {
                        "file": file_name,
                        "type": file_type,
                        "size_kb": file_size / 1024,
                        "time": elapsed,
                        "status": status,
                        "text_len": len(text) if text else 0,
                        "parser": label,
                    }
                )

                print(f"   ✅ [{label}] {elapsed:.2f}s ({len(text) if text else 0} chars)")

        except Exception as e:
            elapsed = time.time() - total_start
            results.append(
                {
                    "file": file_name,
                    "type": file_type,
                    "size_kb": file_size / 1024,
                    "time": elapsed,
                    "status": f"error: {str(e)[:50]}",
                    "text_len": 0,
                    "parser": "failed",
                }
            )
            print(f"   ❌ Error: {e}")
            import traceback

            traceback.print_exc()

        print()

    total_time = time.time() - total_start

    # Summary
    print("=" * 70)
    print("📊 RESULTS SUMMARY")
    print("=" * 70)
    print()
    print(f"{'File':<45} {'Parser':<14} {'Type':<6} {'Size':<10} {'Time':<10} {'Status'}")
    print("-" * 110)

    for r in results:
        status = "✅" if r["status"] == "success" else "❌"
        print(f"{r['file'][:44]:<45} {r['parser']:<14} {r['type']:<6} {r['size_kb']:>7.1f}KB {r['time']:>7.2f}s  {status}")

    print("-" * 110)
    print(f"{'TOTAL':<45} {'':<14} {'':<6} {'':<10} {total_time:>7.2f}s")
    print()

    # Per-parser averages for PDFs
    def avg_time(parser_label: str):
        times = [r["time"] for r in results if r["parser"] == parser_label and r["type"] == ".pdf" and r["status"] == "success"]
        return sum(times) / len(times) if times else None

    for label in ["pdfplumber", "docling", "mistral"]:
        avg = avg_time(label)
        if avg is not None:
            print(f"{label} PDF average: {avg:.2f}s")

    print()

    # Save summary
    summary_file = output_dir / "LOCAL_RESULTS_SUMMARY.md"
    with open(summary_file, "w") as f:
        f.write("# Local Test Results - Multi-parser\n\n")
        f.write(f"**Platform**: Mac (Apple Silicon MPS)\n")
        f.write(f"**Docling Available**: {is_docling_available()}\n")
        f.write(f"**Mistral Available**: {is_mistral_available()}\n")
        f.write(f"**Total Time**: {total_time:.2f}s\n")
        f.write(f"**Files Processed**: {len(results)}\n\n")
        f.write("## Processing Times\n\n")
        f.write("| File | Parser | Type | Size | Time | Status |\n")
        f.write("|------|--------|------|------|------|--------|\n")
        for r in results:
            status = "✅" if r["status"] == "success" else f"❌ {r['status']}"
            f.write(f"| {r['file'][:40]} | {r['parser']} | {r['type']} | {r['size_kb']:.1f}KB | {r['time']:.2f}s | {status} |\n")
        f.write(f"\n**Total**: {total_time:.2f}s\n")
        for label in ["pdfplumber", "docling", "mistral"]:
            avg = avg_time(label)
            if avg is not None:
                f.write(f"**{label} PDF Average**: {avg:.2f}s\n")

    print(f"Results saved to: {output_dir}")
    print("Done!")


if __name__ == "__main__":
    main()
