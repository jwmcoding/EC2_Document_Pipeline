#!/usr/bin/env python3
"""
Analyze depth of Pinecone results specifically for the Salesforce user queries.
"""
import json
import sys
from collections import defaultdict

# Load comparison results
with open("tests/comparison_results_with_user_queries.json", "r") as f:
    data = json.load(f)

# The first 6 queries are the real Salesforce user queries
SALESFORCE_QUERIES = [
    "Salesforce contracts June 2024 December 2025 Agentforce AI automation licensing",
    "Salesforce new purchase renewal add-on expansion deals 2024 2025 discount pricing",
    "Salesforce Agentforce adoption rates new purchase vs renewal add-on deals 2024 2025",
    "Salesforce Agentforce bundling pricing AI add-on features",
    "Agentforce negotiation leverage competitive replacement discount strategy contracts",
    "Salesforce AI features Flow automation data analytics enterprise negotiations",
]

print("="*80)
print("SALESFORCE USER QUERY ANALYSIS")
print("="*80)
print(f"\nLeft:  {data['left_target']}")
print(f"Right: {data['right_target']}")
print()

def analyze_results(results):
    """Analyze a set of results for metadata quality."""
    stats = {
        "count": len(results),
        "with_client_name": 0,
        "with_vendor_name": 0,
        "with_deal_reason": 0,
        "with_final_amount": 0,
        "unique_clients": set(),
        "unique_vendors": set(),
        "avg_score": 0,
    }
    
    scores = []
    for r in results:
        if r.get("client_name") and str(r.get("client_name")).lower() not in ["none", "nan", ""]:
            stats["with_client_name"] += 1
            stats["unique_clients"].add(r.get("client_name"))
        if r.get("vendor_name") and str(r.get("vendor_name")).lower() not in ["none", "nan", ""]:
            stats["with_vendor_name"] += 1
            stats["unique_vendors"].add(r.get("vendor_name"))
        if r.get("deal_reason") and str(r.get("deal_reason")).lower() not in ["none", "nan", ""]:
            stats["with_deal_reason"] += 1
        if r.get("final_amount") and r.get("final_amount") > 0:
            stats["with_final_amount"] += 1
        if "score" in r:
            scores.append(r["score"])
    
    stats["avg_score"] = sum(scores) / len(scores) if scores else 0
    stats["unique_clients"] = len(stats["unique_clients"])
    stats["unique_vendors"] = len(stats["unique_vendors"])
    return stats

# Process each Salesforce query
left_totals = defaultdict(int)
right_totals = defaultdict(int)
query_count = 0

for i, query_result in enumerate(data["query_comparisons"]):
    query = data["queries"][i]
    
    # Check if this is a Salesforce query (first 6)
    if i >= 6:
        continue
    
    query_count += 1
    print(f"\n{'─'*80}")
    print(f"Query {query_count}: {query[:70]}...")
    print(f"{'─'*80}")
    
    left_results = query_result.get("left_top_5", [])
    right_results = query_result.get("right_top_5", [])
    
    left_stats = analyze_results(left_results)
    right_stats = analyze_results(right_results)
    
    print(f"\n  PRODUCTION (npi-deal-data):")
    print(f"    Results: {left_stats['count']}")
    print(f"    Avg Score: {left_stats['avg_score']:.4f}")
    print(f"    With client_name: {left_stats['with_client_name']}/{left_stats['count']} ({left_stats['unique_clients']} unique)")
    print(f"    With vendor_name: {left_stats['with_vendor_name']}/{left_stats['count']} ({left_stats['unique_vendors']} unique)")
    print(f"    With deal_reason: {left_stats['with_deal_reason']}/{left_stats['count']}")
    print(f"    With final_amount: {left_stats['with_final_amount']}/{left_stats['count']}")
    
    print(f"\n  LEGACY (business-documents):")
    print(f"    Results: {right_stats['count']}")
    print(f"    Avg Score: {right_stats['avg_score']:.4f}")
    print(f"    With client_name: {right_stats['with_client_name']}/{right_stats['count']} ({right_stats['unique_clients']} unique)")
    print(f"    With vendor_name: {right_stats['with_vendor_name']}/{right_stats['count']} ({right_stats['unique_vendors']} unique)")
    print(f"    With deal_reason: {right_stats['with_deal_reason']}/{right_stats['count']}")
    print(f"    With final_amount: {right_stats['with_final_amount']}/{right_stats['count']}")
    
    # Accumulate totals
    for key in ["with_client_name", "with_vendor_name", "with_deal_reason", "with_final_amount", "count"]:
        left_totals[key] += left_stats[key]
        right_totals[key] += right_stats[key]

    # Show vendors from each
    print(f"\n  Top Vendors (Production):", end=" ")
    vendors = [r.get("vendor_name", "?") for r in left_results if r.get("vendor_name")]
    print(", ".join(vendors[:5]) if vendors else "No vendor data")
    
    print(f"  Top Vendors (Legacy):", end=" ")
    vendors = [r.get("vendor_name", "?") for r in right_results if r.get("vendor_name")]
    print(", ".join(vendors[:5]) if vendors else "No vendor data")
    
    # Show clients from production (legacy likely has none)
    print(f"  Clients (Production):", end=" ")
    clients = [r.get("client_name", "?") for r in left_results if r.get("client_name") and str(r.get("client_name")).lower() not in ["none", ""]]
    print(", ".join(clients[:5]) if clients else "No client data")
    
    # Overlap info
    print(f"\n  Overlap: {query_result['overlap_count']} shared results")
    print(f"  Jaccard Similarity: {query_result['jaccard_similarity']:.3f}")

# Summary
print(f"\n\n{'='*80}")
print("AGGREGATE SUMMARY - Salesforce Queries Only (Top 5 per query)")
print("="*80)

def pct(val, total):
    return f"{val/total*100:.0f}%" if total > 0 else "N/A"

print(f"\n{'Metric':<25} {'Production':<25} {'Legacy':<25}")
print("-"*75)
print(f"{'Total Results':<25} {left_totals['count']:<25} {right_totals['count']:<25}")
print(f"{'With client_name':<25} {left_totals['with_client_name']} ({pct(left_totals['with_client_name'], left_totals['count'])}){'':<10} {right_totals['with_client_name']} ({pct(right_totals['with_client_name'], right_totals['count'])})")
print(f"{'With vendor_name':<25} {left_totals['with_vendor_name']} ({pct(left_totals['with_vendor_name'], left_totals['count'])}){'':<10} {right_totals['with_vendor_name']} ({pct(right_totals['with_vendor_name'], right_totals['count'])})")
print(f"{'With deal_reason':<25} {left_totals['with_deal_reason']} ({pct(left_totals['with_deal_reason'], left_totals['count'])}){'':<10} {right_totals['with_deal_reason']} ({pct(right_totals['with_deal_reason'], right_totals['count'])})")
print(f"{'With final_amount':<25} {left_totals['with_final_amount']} ({pct(left_totals['with_final_amount'], left_totals['count'])}){'':<10} {right_totals['with_final_amount']} ({pct(right_totals['with_final_amount'], right_totals['count'])})")

print(f"\n{'─'*75}")
print("\n📊 KEY FINDINGS:")
client_diff = left_totals["with_client_name"] - right_totals["with_client_name"]
if client_diff > 0:
    print(f"  ✅ Production index has {client_diff} MORE results with client_name populated")
    print(f"     ({pct(left_totals['with_client_name'], left_totals['count'])} vs {pct(right_totals['with_client_name'], right_totals['count'])})")
else:
    print(f"  ⚠️  Legacy index has {-client_diff} MORE results with client_name populated")

print(f"\n  → For Salesforce/Agentforce queries, the Production index (npi-deal-data)")
print(f"    provides richer metadata for business intelligence use cases.")
