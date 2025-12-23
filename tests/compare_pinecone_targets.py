#!/usr/bin/env python3
"""
Pinecone Index/Namespace Comparison Tool

Executes identical queries against two different Pinecone indexes/namespaces and
generates a comprehensive quality evaluation report.

Purpose: Compare search quality, result consistency, and metadata coverage between
two processing pipelines or index configurations.

Usage:
    python tests/compare_pinecone_targets.py \
        --left-index npi-deal-data --left-namespace sf-export-aug15-2025 \
        --right-index business-documents --right-namespace SF-Files-2020-8-15-25 \
        --queries config/comparison_queries.yaml \
        --output output/comparison_report_$(date +%Y%m%d_%H%M%S).json

    # Or with default test queries:
    python tests/compare_pinecone_targets.py \
        --left-index npi-deal-data --left-namespace sf-export-aug15-2025 \
        --right-index npi-deal-data --right-namespace benchmark-mistral-all-docs-12-18
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pinecone import Pinecone


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Default queries covering business document intelligence use cases
DEFAULT_QUERIES = [
    # Pricing & Financial
    "What are the pricing terms and payment schedules in software contracts?",
    "Show me license costs and annual maintenance fees",
    "Find contracts with savings over 1 million dollars",
    
    # Contract Terms
    "What are the renewal terms and auto-renewal clauses?",
    "Find contracts with termination for convenience clauses",
    "What are the SLA and uptime guarantees?",
    
    # Vendor-specific
    "Show me Microsoft enterprise agreements and licensing terms",
    "Find Salesforce contracts and subscription details",
    "What are the Oracle licensing terms?",
    
    # Deal Context
    "Find deals with competitive bidding or multi-vendor sourcing",
    "Show me contracts in the healthcare industry",
    "What deals had time pressure or urgent timelines?",
]

# Metadata fields to compare for coverage analysis
METADATA_FIELDS_TO_COMPARE = [
    # Core fields
    "file_name", "file_type", "deal_id", "client_name", "vendor_name",
    # Financial
    "final_amount", "savings_1yr", "savings_3yr", "fixed_savings",
    # Contract
    "contract_term", "contract_start", "contract_end", "deal_status", "deal_reason",
    # Classification
    "report_type", "project_type", "competition", "time_pressure",
]


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    """Single search result from Pinecone."""
    id: str
    score: float
    text: str
    metadata: Dict[str, Any]


@dataclass
class QueryComparison:
    """Comparison results for a single query."""
    query: str
    left_results: List[QueryResult]
    right_results: List[QueryResult]
    overlap_ids: List[str]  # IDs found in both result sets
    left_only_ids: List[str]  # IDs only in left results
    right_only_ids: List[str]  # IDs only in right results
    jaccard_similarity: float  # Overlap / Union
    rank_correlation: float  # Spearman rank correlation for overlapping results
    score_comparison: Dict[str, Any]  # Score statistics


@dataclass
class TargetConfig:
    """Configuration for a Pinecone target (index + namespace)."""
    index_name: str
    namespace: str
    label: str  # Human-readable label for reports
    
    def __str__(self) -> str:
        return f"{self.index_name}/{self.namespace}"


@dataclass
class ComparisonReport:
    """Full comparison report between two targets."""
    timestamp: str
    left_target: Dict[str, str]
    right_target: Dict[str, str]
    queries: List[str]
    query_comparisons: List[Dict[str, Any]]
    aggregate_metrics: Dict[str, Any]
    metadata_coverage: Dict[str, Dict[str, float]]


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging with appropriate level."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Pinecone Client
# ─────────────────────────────────────────────────────────────────────────────

class PineconeComparisonClient:
    """Client for executing comparative queries against Pinecone indexes."""
    
    def __init__(self, api_key: str, logger: logging.Logger):
        self.pc = Pinecone(api_key=api_key)
        self.logger = logger
        self._index_cache: Dict[str, Any] = {}
    
    def _get_index(self, index_name: str):
        """Get or create cached index connection."""
        if index_name not in self._index_cache:
            self._index_cache[index_name] = self.pc.Index(index_name)
            self.logger.info(f"Connected to index: {index_name}")
        return self._index_cache[index_name]
    
    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """Get statistics for an index."""
        index = self._get_index(index_name)
        stats = index.describe_index_stats()
        return {
            "total_vector_count": stats.total_vector_count,
            "dimension": stats.dimension,
            "namespaces": {
                ns: {"record_count": ns_stats.vector_count}
                for ns, ns_stats in (stats.namespaces or {}).items()
            }
        }
    
    def execute_hybrid_query(
        self,
        index_name: str,
        namespace: str,
        query_text: str,
        top_k: int = 20,
        alpha: float = 0.6,
        include_metadata: bool = True
    ) -> List[QueryResult]:
        """
        Execute hybrid search (dense + sparse) against Pinecone.
        
        Args:
            index_name: Target index name
            namespace: Target namespace
            query_text: Natural language query
            top_k: Number of results to return
            alpha: Dense/sparse weight (0.6 = 60% dense, 40% sparse)
            include_metadata: Whether to include metadata in results
            
        Returns:
            List of QueryResult objects
        """
        index = self._get_index(index_name)
        
        # Generate embeddings using Pinecone inference
        try:
            # Dense embedding
            dense_response = self.pc.inference.embed(
                model="multilingual-e5-large",
                inputs=[query_text],
                parameters={"input_type": "query"}
            )
            dense_values = dense_response[0]["values"]
            
            # Sparse embedding
            sparse_response = self.pc.inference.embed(
                model="pinecone-sparse-english-v0",
                inputs=[query_text],
                parameters={"input_type": "query"}
            )
            sparse_indices = sparse_response[0].get("sparse_indices", [])
            sparse_values_raw = sparse_response[0].get("sparse_values", [])
            
            # Apply hybrid weights
            weighted_dense = [v * alpha for v in dense_values]
            weighted_sparse = {
                "indices": sparse_indices,
                "values": [v * (1 - alpha) for v in sparse_values_raw]
            } if sparse_indices else None
            
        except Exception as e:
            self.logger.error(f"Embedding generation failed: {e}")
            raise
        
        # Execute query
        try:
            query_params = {
                "namespace": namespace,
                "top_k": top_k,
                "vector": weighted_dense,
                "include_metadata": include_metadata,
            }
            if weighted_sparse and weighted_sparse["indices"]:
                query_params["sparse_vector"] = weighted_sparse
            
            results = index.query(**query_params)
            
            # Parse results
            query_results = []
            for match in results.get("matches", []):
                metadata = match.get("metadata", {})
                # Try to get text from metadata or use empty string
                text = metadata.get("text", "")
                
                query_results.append(QueryResult(
                    id=match["id"],
                    score=match.get("score", 0.0),
                    text=text[:500] if text else "",  # Truncate for report
                    metadata=metadata
                ))
            
            self.logger.debug(f"Query returned {len(query_results)} results from {index_name}/{namespace}")
            return query_results
            
        except Exception as e:
            self.logger.error(f"Query failed for {index_name}/{namespace}: {e}")
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Comparison Logic
# ─────────────────────────────────────────────────────────────────────────────

def calculate_jaccard_similarity(left_ids: List[str], right_ids: List[str]) -> float:
    """Calculate Jaccard similarity between two ID sets."""
    left_set = set(left_ids)
    right_set = set(right_ids)
    
    if not left_set and not right_set:
        return 1.0  # Both empty = identical
    
    intersection = len(left_set & right_set)
    union = len(left_set | right_set)
    
    return intersection / union if union > 0 else 0.0


def calculate_rank_correlation(left_results: List[QueryResult], right_results: List[QueryResult]) -> float:
    """
    Calculate Spearman rank correlation for overlapping results.
    
    Returns correlation coefficient (-1 to 1) or 0 if no overlap.
    """
    left_ranks = {r.id: i for i, r in enumerate(left_results)}
    right_ranks = {r.id: i for i, r in enumerate(right_results)}
    
    # Find overlapping IDs
    overlap = set(left_ranks.keys()) & set(right_ranks.keys())
    
    if len(overlap) < 2:
        return 0.0  # Need at least 2 points for correlation
    
    # Calculate Spearman correlation
    n = len(overlap)
    overlap_list = list(overlap)
    
    d_squared_sum = sum(
        (left_ranks[id_] - right_ranks[id_]) ** 2
        for id_ in overlap_list
    )
    
    # Spearman formula: 1 - (6 * sum(d^2)) / (n * (n^2 - 1))
    if n * (n**2 - 1) == 0:
        return 0.0
    
    correlation = 1 - (6 * d_squared_sum) / (n * (n**2 - 1))
    return correlation


def compare_scores(left_results: List[QueryResult], right_results: List[QueryResult]) -> Dict[str, Any]:
    """Compare score distributions between result sets."""
    left_scores = [r.score for r in left_results]
    right_scores = [r.score for r in right_results]
    
    def stats(scores: List[float]) -> Dict[str, float]:
        if not scores:
            return {"min": 0, "max": 0, "mean": 0, "count": 0}
        return {
            "min": min(scores),
            "max": max(scores),
            "mean": sum(scores) / len(scores),
            "count": len(scores)
        }
    
    return {
        "left_stats": stats(left_scores),
        "right_stats": stats(right_scores),
    }


def calculate_metadata_coverage(results: List[QueryResult], fields: List[str]) -> Dict[str, float]:
    """Calculate what percentage of results have each metadata field populated."""
    if not results:
        return {field: 0.0 for field in fields}
    
    coverage = {}
    for field in fields:
        populated = sum(
            1 for r in results
            if r.metadata.get(field) and str(r.metadata.get(field)).strip() 
            and str(r.metadata.get(field)).lower() not in ("none", "nan", "null", "")
        )
        coverage[field] = populated / len(results)
    
    return coverage


def compare_query(
    client: PineconeComparisonClient,
    query: str,
    left: TargetConfig,
    right: TargetConfig,
    top_k: int = 20,
    logger: logging.Logger = None
) -> QueryComparison:
    """Execute a query against both targets and compare results."""
    if logger:
        logger.info(f"Comparing query: '{query[:50]}...'")
    
    # Execute queries
    left_results = client.execute_hybrid_query(
        left.index_name, left.namespace, query, top_k=top_k
    )
    right_results = client.execute_hybrid_query(
        right.index_name, right.namespace, query, top_k=top_k
    )
    
    # Extract IDs
    left_ids = [r.id for r in left_results]
    right_ids = [r.id for r in right_results]
    
    # Calculate overlaps
    left_set = set(left_ids)
    right_set = set(right_ids)
    overlap = list(left_set & right_set)
    left_only = list(left_set - right_set)
    right_only = list(right_set - left_set)
    
    return QueryComparison(
        query=query,
        left_results=left_results,
        right_results=right_results,
        overlap_ids=overlap,
        left_only_ids=left_only,
        right_only_ids=right_only,
        jaccard_similarity=calculate_jaccard_similarity(left_ids, right_ids),
        rank_correlation=calculate_rank_correlation(left_results, right_results),
        score_comparison=compare_scores(left_results, right_results)
    )


def generate_aggregate_metrics(comparisons: List[QueryComparison]) -> Dict[str, Any]:
    """Generate aggregate metrics across all query comparisons."""
    if not comparisons:
        return {}
    
    jaccard_scores = [c.jaccard_similarity for c in comparisons]
    rank_correlations = [c.rank_correlation for c in comparisons]
    
    left_result_counts = [len(c.left_results) for c in comparisons]
    right_result_counts = [len(c.right_results) for c in comparisons]
    
    overlap_counts = [len(c.overlap_ids) for c in comparisons]
    
    return {
        "query_count": len(comparisons),
        "jaccard_similarity": {
            "mean": sum(jaccard_scores) / len(jaccard_scores),
            "min": min(jaccard_scores),
            "max": max(jaccard_scores),
        },
        "rank_correlation": {
            "mean": sum(rank_correlations) / len(rank_correlations),
            "min": min(rank_correlations),
            "max": max(rank_correlations),
        },
        "result_counts": {
            "left_mean": sum(left_result_counts) / len(left_result_counts),
            "right_mean": sum(right_result_counts) / len(right_result_counts),
        },
        "overlap": {
            "mean_overlap": sum(overlap_counts) / len(overlap_counts),
            "min_overlap": min(overlap_counts),
            "max_overlap": max(overlap_counts),
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# Report Generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(
    left: TargetConfig,
    right: TargetConfig,
    queries: List[str],
    comparisons: List[QueryComparison]
) -> ComparisonReport:
    """Generate the full comparison report."""
    
    # Aggregate metadata coverage across all results
    all_left_results = [r for c in comparisons for r in c.left_results]
    all_right_results = [r for c in comparisons for r in c.right_results]
    
    left_coverage = calculate_metadata_coverage(all_left_results, METADATA_FIELDS_TO_COMPARE)
    right_coverage = calculate_metadata_coverage(all_right_results, METADATA_FIELDS_TO_COMPARE)
    
    # Serialize comparisons (convert dataclasses to dicts)
    comparison_dicts = []
    for c in comparisons:
        comparison_dicts.append({
            "query": c.query,
            "left_result_count": len(c.left_results),
            "right_result_count": len(c.right_results),
            "overlap_count": len(c.overlap_ids),
            "left_only_count": len(c.left_only_ids),
            "right_only_count": len(c.right_only_ids),
            "jaccard_similarity": c.jaccard_similarity,
            "rank_correlation": c.rank_correlation,
            "score_comparison": c.score_comparison,
            # Include top 5 results from each side for manual review
            "left_top_5": [
                {"id": r.id, "score": r.score, "file_name": r.metadata.get("file_name", ""), "vendor_name": r.metadata.get("vendor_name", "")}
                for r in c.left_results[:5]
            ],
            "right_top_5": [
                {"id": r.id, "score": r.score, "file_name": r.metadata.get("file_name", ""), "vendor_name": r.metadata.get("vendor_name", "")}
                for r in c.right_results[:5]
            ],
        })
    
    return ComparisonReport(
        timestamp=datetime.now(timezone.utc).isoformat(),
        left_target={"index": left.index_name, "namespace": left.namespace, "label": left.label},
        right_target={"index": right.index_name, "namespace": right.namespace, "label": right.label},
        queries=queries,
        query_comparisons=comparison_dicts,
        aggregate_metrics=generate_aggregate_metrics(comparisons),
        metadata_coverage={
            "left": left_coverage,
            "right": right_coverage,
        }
    )


def generate_markdown_summary(report: ComparisonReport) -> str:
    """Generate a human-readable Markdown summary of the comparison."""
    lines = [
        "# Pinecone Index/Namespace Comparison Report",
        "",
        f"**Generated**: {report.timestamp}",
        "",
        "## Targets Compared",
        "",
        f"| Target | Index | Namespace |",
        f"|--------|-------|-----------|",
        f"| **Left** | `{report.left_target['index']}` | `{report.left_target['namespace']}` |",
        f"| **Right** | `{report.right_target['index']}` | `{report.right_target['namespace']}` |",
        "",
        "## Aggregate Metrics",
        "",
    ]
    
    agg = report.aggregate_metrics
    if agg:
        lines.extend([
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Queries Tested | {agg.get('query_count', 0)} |",
            f"| Mean Jaccard Similarity | {agg.get('jaccard_similarity', {}).get('mean', 0):.3f} |",
            f"| Mean Rank Correlation | {agg.get('rank_correlation', {}).get('mean', 0):.3f} |",
            f"| Mean Overlap (shared results) | {agg.get('overlap', {}).get('mean_overlap', 0):.1f} |",
            "",
        ])
    
    lines.extend([
        "## Metadata Coverage Comparison",
        "",
        "| Field | Left Coverage | Right Coverage | Delta |",
        "|-------|---------------|----------------|-------|",
    ])
    
    left_cov = report.metadata_coverage.get("left", {})
    right_cov = report.metadata_coverage.get("right", {})
    
    for field in METADATA_FIELDS_TO_COMPARE:
        left_val = left_cov.get(field, 0)
        right_val = right_cov.get(field, 0)
        delta = left_val - right_val
        delta_str = f"+{delta:.1%}" if delta > 0 else f"{delta:.1%}"
        lines.append(f"| `{field}` | {left_val:.1%} | {right_val:.1%} | {delta_str} |")
    
    lines.extend([
        "",
        "## Per-Query Results",
        "",
    ])
    
    for i, qc in enumerate(report.query_comparisons, 1):
        query_short = qc["query"][:60] + "..." if len(qc["query"]) > 60 else qc["query"]
        lines.extend([
            f"### Query {i}: {query_short}",
            "",
            f"- **Left results**: {qc['left_result_count']} | **Right results**: {qc['right_result_count']}",
            f"- **Overlap**: {qc['overlap_count']} | **Jaccard**: {qc['jaccard_similarity']:.3f} | **Rank Corr**: {qc['rank_correlation']:.3f}",
            "",
            "**Top 5 Left Results:**",
            "",
        ])
        
        for r in qc.get("left_top_5", [])[:3]:
            lines.append(f"- `{r['file_name'][:50]}` (vendor: {r['vendor_name']}, score: {r['score']:.4f})")
        
        lines.extend([
            "",
            "**Top 5 Right Results:**",
            "",
        ])
        
        for r in qc.get("right_top_5", [])[:3]:
            lines.append(f"- `{r['file_name'][:50]}` (vendor: {r['vendor_name']}, score: {r['score']:.4f})")
        
        lines.append("")
    
    lines.extend([
        "---",
        "",
        "*Report generated by `tests/compare_pinecone_targets.py`*",
    ])
    
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare Pinecone query results between two indexes/namespaces",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare two namespaces in the same index
  python tests/compare_pinecone_targets.py \\
      --left-index npi-deal-data --left-namespace sf-export-aug15-2025 \\
      --right-index npi-deal-data --right-namespace benchmark-mistral-all-docs-12-18

  # Compare across different indexes
  python tests/compare_pinecone_targets.py \\
      --left-index npi-deal-data --left-namespace sf-export-aug15-2025 \\
      --right-index business-documents --right-namespace SF-Files-2020-8-15-25 \\
      --top-k 30 --verbose

  # Use custom queries from a file
  python tests/compare_pinecone_targets.py \\
      --left-index npi-deal-data --left-namespace sf-export-aug15-2025 \\
      --right-index business-documents --right-namespace documents \\
      --queries queries.txt
        """
    )
    
    # Left target
    parser.add_argument(
        "--left-index", required=True,
        help="Left index name (e.g., 'npi-deal-data')"
    )
    parser.add_argument(
        "--left-namespace", required=True,
        help="Left namespace (e.g., 'sf-export-aug15-2025')"
    )
    parser.add_argument(
        "--left-label", default=None,
        help="Human-readable label for left target (default: index/namespace)"
    )
    
    # Right target
    parser.add_argument(
        "--right-index", required=True,
        help="Right index name"
    )
    parser.add_argument(
        "--right-namespace", required=True,
        help="Right namespace"
    )
    parser.add_argument(
        "--right-label", default=None,
        help="Human-readable label for right target (default: index/namespace)"
    )
    
    # Query options
    parser.add_argument(
        "--queries", default=None,
        help="Path to file with queries (one per line) or YAML file. Uses default queries if not specified."
    )
    parser.add_argument(
        "--top-k", type=int, default=20,
        help="Number of results to retrieve per query (default: 20)"
    )
    parser.add_argument(
        "--alpha", type=float, default=0.6,
        help="Dense/sparse weight for hybrid search (default: 0.6 = 60%% dense)"
    )
    
    # Output options
    parser.add_argument(
        "--output", default=None,
        help="Output file path for JSON report (default: output/comparison_<timestamp>.json)"
    )
    parser.add_argument(
        "--no-markdown", action="store_true",
        help="Skip generating Markdown summary"
    )
    
    # Debug options
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show configuration without executing queries"
    )
    
    return parser.parse_args()


def load_queries_from_file(filepath: str) -> List[str]:
    """Load queries from a text file (one per line) or YAML file."""
    path = Path(filepath)
    
    if not path.exists():
        raise FileNotFoundError(f"Queries file not found: {filepath}")
    
    if path.suffix in (".yaml", ".yml"):
        import yaml
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "queries" in data:
            return data["queries"]
        else:
            raise ValueError("YAML file must contain a list or dict with 'queries' key")
    else:
        # Plain text file
        with open(path, "r") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]


def main() -> int:
    """Main entry point."""
    # Load environment variables
    load_dotenv()
    
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logging(verbose=args.verbose)
    
    # Validate API key
    api_key = os.getenv("PINECONE_API_KEY")
    if not api_key:
        logger.error("PINECONE_API_KEY not found in environment. Add to .env file.")
        return 1
    
    # Configure targets
    left = TargetConfig(
        index_name=args.left_index,
        namespace=args.left_namespace,
        label=args.left_label or f"{args.left_index}/{args.left_namespace}"
    )
    right = TargetConfig(
        index_name=args.right_index,
        namespace=args.right_namespace,
        label=args.right_label or f"{args.right_index}/{args.right_namespace}"
    )
    
    # Load queries
    if args.queries:
        try:
            queries = load_queries_from_file(args.queries)
            logger.info(f"Loaded {len(queries)} queries from {args.queries}")
        except Exception as e:
            logger.error(f"Failed to load queries: {e}")
            return 1
    else:
        queries = DEFAULT_QUERIES
        logger.info(f"Using {len(queries)} default queries")
    
    # Setup output path
    if args.output:
        output_path = Path(args.output)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f"comparison_{timestamp}.json"
    
    # Dry run - just show config
    if args.dry_run:
        logger.info("=" * 60)
        logger.info("DRY RUN - Configuration:")
        logger.info(f"  Left:  {left}")
        logger.info(f"  Right: {right}")
        logger.info(f"  Queries: {len(queries)}")
        logger.info(f"  Top-K: {args.top_k}")
        logger.info(f"  Alpha: {args.alpha}")
        logger.info(f"  Output: {output_path}")
        logger.info("=" * 60)
        logger.info("Queries to execute:")
        for i, q in enumerate(queries, 1):
            logger.info(f"  {i}. {q[:70]}...")
        return 0
    
    # Initialize client
    logger.info("=" * 60)
    logger.info("Pinecone Index/Namespace Comparison")
    logger.info("=" * 60)
    logger.info(f"Left target:  {left}")
    logger.info(f"Right target: {right}")
    logger.info(f"Queries: {len(queries)}")
    logger.info(f"Top-K: {args.top_k}")
    
    client = PineconeComparisonClient(api_key, logger)
    
    # Get index stats
    try:
        left_stats = client.get_index_stats(left.index_name)
        right_stats = client.get_index_stats(right.index_name)
        
        left_ns_count = left_stats["namespaces"].get(left.namespace, {}).get("record_count", "N/A")
        right_ns_count = right_stats["namespaces"].get(right.namespace, {}).get("record_count", "N/A")
        
        logger.info(f"Left namespace record count:  {left_ns_count:,}" if isinstance(left_ns_count, int) else f"Left namespace record count: {left_ns_count}")
        logger.info(f"Right namespace record count: {right_ns_count:,}" if isinstance(right_ns_count, int) else f"Right namespace record count: {right_ns_count}")
    except Exception as e:
        logger.warning(f"Could not get index stats: {e}")
    
    logger.info("-" * 60)
    
    # Execute comparisons
    comparisons = []
    for i, query in enumerate(queries, 1):
        logger.info(f"[{i}/{len(queries)}] Executing query...")
        try:
            comparison = compare_query(
                client=client,
                query=query,
                left=left,
                right=right,
                top_k=args.top_k,
                logger=logger
            )
            comparisons.append(comparison)
            
            # Log brief summary
            logger.info(
                f"  → Left: {len(comparison.left_results)}, Right: {len(comparison.right_results)}, "
                f"Overlap: {len(comparison.overlap_ids)}, Jaccard: {comparison.jaccard_similarity:.3f}"
            )
        except Exception as e:
            logger.error(f"  → Query failed: {e}")
            continue
    
    logger.info("-" * 60)
    
    # Generate report
    report = generate_report(left, right, queries, comparisons)
    
    # Save JSON report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(asdict(report), f, indent=2, default=str)
    logger.info(f"JSON report saved to: {output_path}")
    
    # Generate Markdown summary
    if not args.no_markdown:
        md_path = output_path.with_suffix(".md")
        md_content = generate_markdown_summary(report)
        with open(md_path, "w") as f:
            f.write(md_content)
        logger.info(f"Markdown summary saved to: {md_path}")
    
    # Print summary
    agg = report.aggregate_metrics
    logger.info("=" * 60)
    logger.info("COMPARISON COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Queries executed: {agg.get('query_count', 0)}")
    logger.info(f"Mean Jaccard Similarity: {agg.get('jaccard_similarity', {}).get('mean', 0):.3f}")
    logger.info(f"Mean Rank Correlation: {agg.get('rank_correlation', {}).get('mean', 0):.3f}")
    logger.info(f"Mean Overlap: {agg.get('overlap', {}).get('mean_overlap', 0):.1f} shared results")
    
    # Print metadata coverage comparison
    logger.info("\nMetadata Coverage (Left vs Right):")
    left_cov = report.metadata_coverage.get("left", {})
    right_cov = report.metadata_coverage.get("right", {})
    for field in ["client_name", "vendor_name", "final_amount", "deal_reason"]:
        left_val = left_cov.get(field, 0)
        right_val = right_cov.get(field, 0)
        logger.info(f"  {field}: {left_val:.1%} vs {right_val:.1%}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

