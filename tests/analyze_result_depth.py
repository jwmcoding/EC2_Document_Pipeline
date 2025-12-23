#!/usr/bin/env python3
"""
Analyze the depth and quality of Pinecone search results.

Fetches actual text content from both indexes and compares:
- Word count / character count
- Information density (pricing/contract terms detected)
- Content quality metrics

Usage:
    python tests/analyze_result_depth.py
"""

import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pinecone import Pinecone

# Load environment
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

LEFT_INDEX = "npi-deal-data"
LEFT_NAMESPACE = "sf-export-aug15-2025"
LEFT_LABEL = "Production (Aug 2025)"

RIGHT_INDEX = "business-documents"
RIGHT_NAMESPACE = "SF-Files-2020-8-15-25"
RIGHT_LABEL = "Legacy SF-Files"

# Sample queries for deep analysis
SAMPLE_QUERIES = [
    "What are the pricing terms and payment schedules in software contracts?",
    "Show me license costs and annual maintenance fees",
    "What are the renewal terms and auto-renewal clauses?",
    "Find FMV reports and fair market value assessments",
    "Show me Microsoft enterprise agreements and licensing terms",
]

# Keywords that indicate valuable content
PRICING_KEYWORDS = [
    "price", "pricing", "cost", "fee", "rate", "discount", "tier",
    "subscription", "license", "per user", "per seat", "annual",
    "monthly", "yearly", "term", "$", "usd", "amount", "total"
]

CONTRACT_KEYWORDS = [
    "agreement", "contract", "term", "renewal", "termination", "sla",
    "liability", "indemnity", "warranty", "guarantee", "clause",
    "provision", "obligation", "compliance", "effective date"
]

FINANCIAL_KEYWORDS = [
    "savings", "roi", "tco", "budget", "spend", "investment",
    "benchmark", "fmv", "fair market value", "negotiated", "proposal"
]


# ─────────────────────────────────────────────────────────────────────────────
# Analysis Functions
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ContentAnalysis:
    """Analysis of a single result's content."""
    id: str
    file_name: str
    vendor_name: str
    score: float
    
    # Content metrics
    text_length: int
    word_count: int
    
    # Quality signals
    pricing_keyword_count: int
    contract_keyword_count: int
    financial_keyword_count: int
    
    # Specific patterns
    dollar_amount_count: int
    percentage_count: int
    date_count: int
    
    # Sample text
    text_preview: str


def count_patterns(text: str) -> Tuple[int, int, int]:
    """Count dollar amounts, percentages, and dates in text."""
    text_lower = text.lower()
    
    # Dollar amounts: $X, $X.XX, $X,XXX, etc.
    dollar_pattern = r'\$[\d,]+(?:\.\d{2})?|\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:dollars|usd|million|billion|k\b)'
    dollars = len(re.findall(dollar_pattern, text_lower))
    
    # Percentages: X%, X.X%
    percent_pattern = r'\d+(?:\.\d+)?%'
    percents = len(re.findall(percent_pattern, text))
    
    # Dates: various formats
    date_pattern = r'\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4})\b'
    dates = len(re.findall(date_pattern, text_lower))
    
    return dollars, percents, dates


def count_keywords(text: str, keywords: List[str]) -> int:
    """Count occurrences of keywords in text."""
    text_lower = text.lower()
    count = 0
    for keyword in keywords:
        count += len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
    return count


def analyze_content(result: Dict[str, Any]) -> ContentAnalysis:
    """Analyze a single search result's content quality."""
    metadata = result.get("metadata", {})
    text = metadata.get("text", "")
    
    # Basic metrics
    text_length = len(text)
    word_count = len(text.split()) if text else 0
    
    # Keyword counts
    pricing_count = count_keywords(text, PRICING_KEYWORDS)
    contract_count = count_keywords(text, CONTRACT_KEYWORDS)
    financial_count = count_keywords(text, FINANCIAL_KEYWORDS)
    
    # Pattern counts
    dollars, percents, dates = count_patterns(text)
    
    # Preview
    preview = text[:300] + "..." if len(text) > 300 else text
    
    return ContentAnalysis(
        id=result.get("id", ""),
        file_name=metadata.get("file_name", ""),
        vendor_name=metadata.get("vendor_name", ""),
        score=result.get("score", 0.0),
        text_length=text_length,
        word_count=word_count,
        pricing_keyword_count=pricing_count,
        contract_keyword_count=contract_count,
        financial_keyword_count=financial_count,
        dollar_amount_count=dollars,
        percentage_count=percents,
        date_count=dates,
        text_preview=preview
    )


def fetch_results_with_text(
    pc: Pinecone,
    index_name: str,
    namespace: str,
    query: str,
    top_k: int = 10
) -> List[Dict[str, Any]]:
    """Fetch search results with full text content."""
    index = pc.Index(index_name)
    
    # Generate query embedding
    dense_response = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=[query],
        parameters={"input_type": "query"}
    )
    dense_values = dense_response[0]["values"]
    
    # Sparse embedding
    sparse_response = pc.inference.embed(
        model="pinecone-sparse-english-v0",
        inputs=[query],
        parameters={"input_type": "query"}
    )
    sparse_indices = sparse_response[0].get("sparse_indices", [])
    sparse_values = sparse_response[0].get("sparse_values", [])
    
    # Apply hybrid weights
    alpha = 0.6
    weighted_dense = [v * alpha for v in dense_values]
    weighted_sparse = {
        "indices": sparse_indices,
        "values": [v * (1 - alpha) for v in sparse_values]
    } if sparse_indices else None
    
    # Query
    query_params = {
        "namespace": namespace,
        "top_k": top_k,
        "vector": weighted_dense,
        "include_metadata": True,
    }
    if weighted_sparse and weighted_sparse["indices"]:
        query_params["sparse_vector"] = weighted_sparse
    
    results = index.query(**query_params)
    return results.get("matches", [])


def compare_results(
    left_results: List[ContentAnalysis],
    right_results: List[ContentAnalysis]
) -> Dict[str, Any]:
    """Compare aggregate metrics between two result sets."""
    
    def avg(values: List[float]) -> float:
        return sum(values) / len(values) if values else 0
    
    left_metrics = {
        "avg_text_length": avg([r.text_length for r in left_results]),
        "avg_word_count": avg([r.word_count for r in left_results]),
        "avg_pricing_keywords": avg([r.pricing_keyword_count for r in left_results]),
        "avg_contract_keywords": avg([r.contract_keyword_count for r in left_results]),
        "avg_financial_keywords": avg([r.financial_keyword_count for r in left_results]),
        "avg_dollar_amounts": avg([r.dollar_amount_count for r in left_results]),
        "avg_percentages": avg([r.percentage_count for r in left_results]),
        "avg_dates": avg([r.date_count for r in left_results]),
        "total_with_text": sum(1 for r in left_results if r.text_length > 0),
        "total_results": len(left_results),
    }
    
    right_metrics = {
        "avg_text_length": avg([r.text_length for r in right_results]),
        "avg_word_count": avg([r.word_count for r in right_results]),
        "avg_pricing_keywords": avg([r.pricing_keyword_count for r in right_results]),
        "avg_contract_keywords": avg([r.contract_keyword_count for r in right_results]),
        "avg_financial_keywords": avg([r.financial_keyword_count for r in right_results]),
        "avg_dollar_amounts": avg([r.dollar_amount_count for r in right_results]),
        "avg_percentages": avg([r.percentage_count for r in right_results]),
        "avg_dates": avg([r.date_count for r in right_results]),
        "total_with_text": sum(1 for r in right_results if r.text_length > 0),
        "total_results": len(right_results),
    }
    
    return {"left": left_metrics, "right": right_metrics}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        print("ERROR: PINECONE_API_KEY not found")
        return 1
    
    pc = Pinecone(api_key=api_key)
    
    print("=" * 80)
    print("PINECONE RESULT DEPTH ANALYSIS")
    print("=" * 80)
    print(f"\nLeft:  {LEFT_INDEX}/{LEFT_NAMESPACE}")
    print(f"Right: {RIGHT_INDEX}/{RIGHT_NAMESPACE}")
    print(f"\nAnalyzing {len(SAMPLE_QUERIES)} queries with top-10 results each...\n")
    
    all_left_analyses = []
    all_right_analyses = []
    
    for i, query in enumerate(SAMPLE_QUERIES, 1):
        print(f"\n{'─' * 80}")
        print(f"Query {i}: {query[:60]}...")
        print("─" * 80)
        
        # Fetch results from both indexes
        print("  Fetching from Production index...")
        left_results = fetch_results_with_text(pc, LEFT_INDEX, LEFT_NAMESPACE, query, top_k=10)
        
        print("  Fetching from Legacy index...")
        right_results = fetch_results_with_text(pc, RIGHT_INDEX, RIGHT_NAMESPACE, query, top_k=10)
        
        # Analyze each result
        left_analyses = [analyze_content(r) for r in left_results]
        right_analyses = [analyze_content(r) for r in right_results]
        
        all_left_analyses.extend(left_analyses)
        all_right_analyses.extend(right_analyses)
        
        # Compare this query
        comparison = compare_results(left_analyses, right_analyses)
        
        print(f"\n  PRODUCTION ({LEFT_LABEL}):")
        print(f"    Results with text: {comparison['left']['total_with_text']}/{comparison['left']['total_results']}")
        print(f"    Avg text length:   {comparison['left']['avg_text_length']:.0f} chars")
        print(f"    Avg word count:    {comparison['left']['avg_word_count']:.0f} words")
        print(f"    Avg $ amounts:     {comparison['left']['avg_dollar_amounts']:.1f}")
        print(f"    Avg pricing terms: {comparison['left']['avg_pricing_keywords']:.1f}")
        print(f"    Avg contract terms:{comparison['left']['avg_contract_keywords']:.1f}")
        
        print(f"\n  LEGACY ({RIGHT_LABEL}):")
        print(f"    Results with text: {comparison['right']['total_with_text']}/{comparison['right']['total_results']}")
        print(f"    Avg text length:   {comparison['right']['avg_text_length']:.0f} chars")
        print(f"    Avg word count:    {comparison['right']['avg_word_count']:.0f} words")
        print(f"    Avg $ amounts:     {comparison['right']['avg_dollar_amounts']:.1f}")
        print(f"    Avg pricing terms: {comparison['right']['avg_pricing_keywords']:.1f}")
        print(f"    Avg contract terms:{comparison['right']['avg_contract_keywords']:.1f}")
        
        # Show top results with their content preview
        print(f"\n  TOP 3 PRODUCTION RESULTS:")
        for j, r in enumerate(left_analyses[:3], 1):
            print(f"    {j}. {r.file_name[:50]} (vendor: {r.vendor_name})")
            print(f"       Score: {r.score:.4f} | Words: {r.word_count} | $: {r.dollar_amount_count} | %: {r.percentage_count}")
            if r.text_preview:
                preview = r.text_preview[:150].replace('\n', ' ')
                print(f"       Preview: {preview}...")
        
        print(f"\n  TOP 3 LEGACY RESULTS:")
        for j, r in enumerate(right_analyses[:3], 1):
            print(f"    {j}. {r.file_name[:50]} (vendor: {r.vendor_name})")
            print(f"       Score: {r.score:.4f} | Words: {r.word_count} | $: {r.dollar_amount_count} | %: {r.percentage_count}")
            if r.text_preview:
                preview = r.text_preview[:150].replace('\n', ' ')
                print(f"       Preview: {preview}...")
    
    # Aggregate summary
    print("\n" + "=" * 80)
    print("AGGREGATE SUMMARY ACROSS ALL QUERIES")
    print("=" * 80)
    
    final_comparison = compare_results(all_left_analyses, all_right_analyses)
    
    print(f"\n{'Metric':<25} {'Production':>15} {'Legacy':>15} {'Winner':>15}")
    print("-" * 70)
    
    metrics_to_compare = [
        ("Results with text", "total_with_text", "total_results"),
        ("Avg text length", "avg_text_length", None),
        ("Avg word count", "avg_word_count", None),
        ("Avg $ amounts", "avg_dollar_amounts", None),
        ("Avg percentages", "avg_percentages", None),
        ("Avg dates", "avg_dates", None),
        ("Avg pricing terms", "avg_pricing_keywords", None),
        ("Avg contract terms", "avg_contract_keywords", None),
        ("Avg financial terms", "avg_financial_keywords", None),
    ]
    
    production_wins = 0
    legacy_wins = 0
    
    for label, key, denom_key in metrics_to_compare:
        left_val = final_comparison["left"][key]
        right_val = final_comparison["right"][key]
        
        if denom_key:
            left_str = f"{left_val}/{final_comparison['left'][denom_key]}"
            right_str = f"{right_val}/{final_comparison['right'][denom_key]}"
        else:
            left_str = f"{left_val:.1f}"
            right_str = f"{right_val:.1f}"
        
        if left_val > right_val:
            winner = "← Production"
            production_wins += 1
        elif right_val > left_val:
            winner = "Legacy →"
            legacy_wins += 1
        else:
            winner = "Tie"
        
        print(f"{label:<25} {left_str:>15} {right_str:>15} {winner:>15}")
    
    print("-" * 70)
    print(f"\n📊 OVERALL WINNER: ", end="")
    if production_wins > legacy_wins:
        print(f"PRODUCTION ({production_wins}-{legacy_wins})")
    elif legacy_wins > production_wins:
        print(f"LEGACY ({legacy_wins}-{production_wins})")
    else:
        print(f"TIE ({production_wins}-{legacy_wins})")
    
    # Information density score
    left_density = (
        final_comparison["left"]["avg_pricing_keywords"] +
        final_comparison["left"]["avg_contract_keywords"] +
        final_comparison["left"]["avg_financial_keywords"] +
        final_comparison["left"]["avg_dollar_amounts"] * 2
    )
    right_density = (
        final_comparison["right"]["avg_pricing_keywords"] +
        final_comparison["right"]["avg_contract_keywords"] +
        final_comparison["right"]["avg_financial_keywords"] +
        final_comparison["right"]["avg_dollar_amounts"] * 2
    )
    
    print(f"\n📈 Information Density Score:")
    print(f"   Production: {left_density:.1f}")
    print(f"   Legacy:     {right_density:.1f}")
    print(f"   Winner:     {'Production' if left_density > right_density else 'Legacy'}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

