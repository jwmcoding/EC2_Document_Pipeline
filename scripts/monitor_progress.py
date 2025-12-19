#!/usr/bin/env python3
"""
Pipeline Progress & Cost Monitor
=================================
Monitors document processing progress and estimates Mistral AI costs.

Usage:
    python scripts/monitor_progress.py [--once]
    
Options:
    --once      Run once and exit (default: runs every 30 minutes)
"""

import os
import sys
import json
import time
import re
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

# === CONFIGURATION ===
TERMINAL_LOG = Path.home() / ".cursor/projects/Users-jeffmuscarella-2025-Python-Dropbox/terminals/17.txt"
DISCOVERY_JSON = Path("production_august_discovery_12_13_2025.json")
CHECK_INTERVAL_MINUTES = 30

# Mistral OCR Pricing (as of Dec 2024)
# https://mistral.ai/technology/#pricing
# OCR: $1.00 per 1000 pages (pixtral-12b for OCR)
MISTRAL_OCR_COST_PER_1000_PAGES = 1.00

# Pinecone embedding costs (via Mistral embed model)
# Embedding: $0.10 per 1M tokens
MISTRAL_EMBED_COST_PER_1M_TOKENS = 0.10


def get_terminal_progress():
    """Parse the terminal log to get current progress."""
    if not TERMINAL_LOG.exists():
        return None
    
    try:
        with open(TERMINAL_LOG, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Find progress bar lines (format: [░░░░] X% | Y/Z | ✅A ❌B | 🧩C | ETA: Dm)
        progress_pattern = r'\[.*?\]\s+(\d+\.?\d*)%\s*\|\s*(\d+)/(\d+)\s*\|\s*✅(\d+)\s*❌(\d+)\s*\|\s*🧩(\d+)\s*\|\s*ETA:\s*(\d+)m'
        matches = re.findall(progress_pattern, content)
        
        if matches:
            last_match = matches[-1]
            return {
                'percent': float(last_match[0]),
                'processed': int(last_match[1]),
                'total': int(last_match[2]),
                'success': int(last_match[3]),
                'failed': int(last_match[4]),
                'chunks': int(last_match[5]),
                'eta_minutes': int(last_match[6])
            }
        
        # Fallback: count OCR calls
        ocr_calls = content.count('POST https://api.mistral.ai/v1/ocr')
        file_uploads = content.count('POST https://api.mistral.ai/v1/files')
        upserts = len(re.findall(r'Upserted (\d+) chunks', content))
        
        return {
            'ocr_calls': ocr_calls,
            'file_uploads': file_uploads,
            'upserts': upserts,
            'chunks': sum(int(x) for x in re.findall(r'Upserted (\d+) chunks', content))
        }
    except Exception as e:
        return {'error': str(e)}


def get_discovery_stats():
    """Get processing stats from discovery JSON."""
    if not DISCOVERY_JSON.exists():
        return None
    
    try:
        with open(DISCOVERY_JSON, 'r') as f:
            data = json.load(f)
        
        docs = data.get('documents', [])
        total = len(docs)
        processed = sum(1 for d in docs if d.get('processing_status', {}).get('processed', False))
        
        return {
            'total': total,
            'processed': processed,
            'remaining': total - processed,
            'percent': (processed / total * 100) if total > 0 else 0
        }
    except Exception as e:
        return {'error': str(e)}


def get_pinecone_stats():
    """Get current Pinecone index statistics."""
    try:
        from pinecone import Pinecone
        
        api_key = os.getenv('PINECONE_API_KEY')
        if not api_key:
            return {'error': 'PINECONE_API_KEY not found'}
        
        pc = Pinecone(api_key=api_key)
        index = pc.Index('npi-deal-data')
        stats = index.describe_index_stats()
        
        namespaces = stats.get('namespaces', {})
        target_ns = namespaces.get('sf-export-aug15-2025', {})
        
        return {
            'total_vectors': stats.get('total_vector_count', 0),
            'namespace_vectors': target_ns.get('record_count', 0),
            'dimension': stats.get('dimension', 0)
        }
    except Exception as e:
        return {'error': str(e)}


def estimate_mistral_costs(progress):
    """Estimate Mistral AI costs based on processing."""
    if not progress or 'error' in progress:
        return None
    
    # Each document typically = 1 OCR page (average)
    # PDFs may have multiple pages, images are 1 page
    estimated_pages = progress.get('processed', 0) or progress.get('ocr_calls', 0)
    
    # OCR cost
    ocr_cost = (estimated_pages / 1000) * MISTRAL_OCR_COST_PER_1000_PAGES
    
    # Embedding cost (rough estimate: ~500 tokens per chunk average)
    chunks = progress.get('chunks', 0)
    estimated_tokens = chunks * 500
    embed_cost = (estimated_tokens / 1_000_000) * MISTRAL_EMBED_COST_PER_1M_TOKENS
    
    return {
        'estimated_pages': estimated_pages,
        'estimated_chunks': chunks,
        'ocr_cost': ocr_cost,
        'embed_cost': embed_cost,
        'total_estimated': ocr_cost + embed_cost
    }


def format_time(minutes):
    """Format minutes to hours:minutes."""
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m"


def print_report():
    """Print a comprehensive status report."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print("\n" + "=" * 70)
    print(f"📊 PIPELINE PROGRESS REPORT - {now}")
    print("=" * 70)
    
    # Terminal Progress
    print("\n🖥️  TERMINAL PROGRESS:")
    progress = get_terminal_progress()
    if progress and 'error' not in progress:
        if 'percent' in progress:
            print(f"   Progress: {progress['percent']:.1f}% ({progress['processed']:,}/{progress['total']:,})")
            print(f"   Success:  {progress['success']:,} ✅")
            print(f"   Failed:   {progress['failed']:,} ❌")
            print(f"   Chunks:   {progress['chunks']:,} 🧩")
            print(f"   ETA:      {format_time(progress['eta_minutes'])}")
        else:
            print(f"   OCR Calls:    {progress.get('ocr_calls', 0):,}")
            print(f"   File Uploads: {progress.get('file_uploads', 0):,}")
            print(f"   Chunks:       {progress.get('chunks', 0):,}")
    else:
        print(f"   Status: Unable to read progress - {progress}")
    
    # Discovery JSON Stats
    print("\n📁 DISCOVERY FILE STATUS:")
    discovery = get_discovery_stats()
    if discovery and 'error' not in discovery:
        print(f"   Total:     {discovery['total']:,}")
        print(f"   Processed: {discovery['processed']:,} ({discovery['percent']:.1f}%)")
        print(f"   Remaining: {discovery['remaining']:,}")
    else:
        print(f"   Status: Unable to read - {discovery}")
    
    # Pinecone Stats
    print("\n🌲 PINECONE INDEX (npi-deal-data):")
    pinecone = get_pinecone_stats()
    if pinecone and 'error' not in pinecone:
        print(f"   Namespace 'sf-export-aug15-2025': {pinecone['namespace_vectors']:,} vectors")
        print(f"   Total Index:                     {pinecone['total_vectors']:,} vectors")
    else:
        print(f"   Status: Unable to connect - {pinecone}")
    
    # Cost Estimates
    print("\n💰 ESTIMATED MISTRAL AI COSTS:")
    costs = estimate_mistral_costs(progress)
    if costs:
        print(f"   Estimated Pages (OCR):  {costs['estimated_pages']:,}")
        print(f"   Estimated Chunks:       {costs['estimated_chunks']:,}")
        print(f"   OCR Cost:               ${costs['ocr_cost']:.2f}")
        print(f"   Embedding Cost:         ${costs['embed_cost']:.4f}")
        print(f"   ─────────────────────────────────")
        print(f"   Total Estimated:        ${costs['total_estimated']:.2f}")
    else:
        print("   Unable to estimate costs")
    
    # Mistral Console Link
    print("\n🔗 CHECK ACTUAL COSTS:")
    print("   Mistral Console: https://console.mistral.ai/billing")
    print("   (Login required - check 'Usage' tab for actual charges)")
    
    print("\n" + "=" * 70)
    print(f"Next check in {CHECK_INTERVAL_MINUTES} minutes...")
    print("=" * 70 + "\n")


def main():
    """Main entry point."""
    run_once = '--once' in sys.argv
    
    print("\n🚀 Pipeline Monitor Started")
    print(f"   Terminal log: {TERMINAL_LOG}")
    print(f"   Discovery:    {DISCOVERY_JSON}")
    print(f"   Interval:     {CHECK_INTERVAL_MINUTES} minutes")
    
    if run_once:
        print("   Mode: Single run (--once)")
        print_report()
    else:
        print("   Mode: Continuous monitoring (Ctrl+C to stop)")
        while True:
            try:
                print_report()
                time.sleep(CHECK_INTERVAL_MINUTES * 60)
            except KeyboardInterrupt:
                print("\n\n👋 Monitor stopped by user")
                break


if __name__ == '__main__':
    main()



