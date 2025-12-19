#!/usr/bin/env python3
"""Upload marketing resources from a local directory to Pinecone.

This script discovers files in a local directory, extracts text using the
existing document processing utilities, creates semantic chunks, generates
hybrid embeddings (dense + sparse), and uploads the results to a Pinecone
namespace with minimal metadata. Real-time progress, logging, and error
tracking mirror the production pipeline standards.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from dotenv import load_dotenv

# Ensure repository modules are importable when running as a script
ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from src.config.colored_logging import ColoredLogger, setup_colored_logging
from src.config.progress_logger import ProcessingProgressLogger
from src.connectors.pinecone_client import PineconeDocumentClient
from src.parsers.document_converter import DocumentConverter
from src.parsers.pdfplumber_parser import PDFPlumberParser
from src.chunking.semantic_chunker import SemanticChunker, Chunk


SUPPORTED_EXTENSIONS: Sequence[str] = (
    ".pdf",
    ".docx",
    ".doc",
    ".pptx",
    ".xlsx",
    ".xls",
    ".csv",
    ".txt",
    ".msg",
    ".png",
    ".jpg",
    ".jpeg",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload marketing resources to Pinecone")
    parser.add_argument("--path", required=True, help="Local directory containing marketing files")
    parser.add_argument(
        "--namespace",
        default="marketing-resources",
        help="Pinecone namespace to upsert chunks into (default: marketing-resources)",
    )
    parser.add_argument(
        "--index-name",
        default=os.getenv("PINECONE_INDEX_NAME", "business-documents"),
        help="Target Pinecone index name (default: env PINECONE_INDEX_NAME or business-documents)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for number of files to process (useful for testing)",
    )
    parser.add_argument(
        "--file-types",
        nargs="*",
        default=None,
        help="Optional list of file extensions to include (e.g. --file-types .pdf .docx)",
    )
    parser.add_argument(
        "--max-chunk-size",
        type=int,
        default=int(os.getenv("MAX_CHUNK_SIZE", "500")),
        help="Maximum characters per chunk (default from MAX_CHUNK_SIZE env or 500)",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=int(os.getenv("CHUNK_OVERLAP", "75")),
        help="Chunk overlap size (default from CHUNK_OVERLAP env or 75)",
    )
    return parser.parse_args()


def discover_files(
    base_path: Path,
    allowed_extensions: Iterable[str],
    limit: int | None = None,
) -> List[Dict[str, object]]:
    files: List[Dict[str, object]] = []
    extensions = {ext.lower() for ext in allowed_extensions}

    for file_path in sorted(base_path.rglob("*")):
        if not file_path.is_file():
            continue

        extension = file_path.suffix.lower()
        if extensions and extension not in extensions:
            continue

        try:
            stat = file_path.stat()
        except OSError:
            continue

        relative_path = file_path.relative_to(base_path)
        files.append(
            {
                "absolute_path": file_path,
                "relative_path": relative_path,
                "name": file_path.name,
                "file_type": extension,
                "size_bytes": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        )

        if limit is not None and len(files) >= limit:
            break

    return files


def summarize_extensions(files: Sequence[Dict[str, object]]) -> str:
    counts: Dict[str, int] = {}
    for info in files:
        ext = info["file_type"]
        counts[ext] = counts.get(ext, 0) + 1

    parts = [f"{ext}: {count}" for ext, count in sorted(counts.items())]
    return ", ".join(parts)


def build_chunk_id(document_path: str, chunk_index: int) -> str:
    base_hash = hashlib.md5(document_path.encode("utf-8")).hexdigest()
    return f"{base_hash}-chunk-{chunk_index:04d}"


def prepare_chunk_payloads(
    chunks: Sequence[Chunk],
    dense_embeddings: Sequence[List[float]],
    sparse_embeddings: Sequence[Dict[str, List[float]]],
    document_path: str,
) -> List[Dict[str, object]]:
    payloads: List[Dict[str, object]] = []

    for idx, chunk in enumerate(chunks):
        chunk_id = build_chunk_id(document_path, chunk.metadata.get("chunk_index", idx))
        chunk_metadata = dict(chunk.metadata)
        chunk_metadata.setdefault("document_path", document_path)

        payloads.append(
            {
                "id": chunk_id,
                "text": chunk.text,
                "metadata": chunk_metadata,
                "dense_embedding": dense_embeddings[idx],
                "sparse_embedding": sparse_embeddings[idx],
            }
        )

    return payloads


def main() -> None:
    load_dotenv()
    args = parse_args()

    setup_colored_logging()
    logger = ColoredLogger("marketing_upload")

    base_path = Path(args.path).expanduser().resolve()
    if not base_path.exists() or not base_path.is_dir():
        logger.error(f"Invalid directory: {base_path}")
        sys.exit(1)

    allowed_extensions = args.file_types if args.file_types else SUPPORTED_EXTENSIONS
    files = discover_files(base_path, allowed_extensions, args.limit)

    if not files:
        logger.warning("No files discovered matching the provided criteria.")
        sys.exit(0)

    logger.info(f"Discovered {len(files)} files in {base_path}")
    logger.info(f"Extension summary: {summarize_extensions(files)}")

    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    environment = os.getenv("PINECONE_ENVIRONMENT", "us-east-1")

    if not pinecone_api_key:
        logger.error("PINECONE_API_KEY environment variable is required.")
        sys.exit(1)

    pinecone_client = PineconeDocumentClient(
        api_key=pinecone_api_key,
        index_name=args.index_name,
        environment=environment,
    )

    converter = DocumentConverter()
    parser = PDFPlumberParser()
    chunker = SemanticChunker(
        max_chunk_size=args.max_chunk_size,
        overlap_size=args.chunk_overlap,
    )

    operation_name = f"marketing_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    progress_logger = ProcessingProgressLogger(
        operation_name=operation_name,
        total_items=len(files),
        dataset_name="documents",
    )

    total_chunks = 0
    failed_files: List[str] = []

    for file_info in files:
        absolute_path: Path = file_info["absolute_path"]  # type: ignore[assignment]
        document_path = str(file_info["relative_path"])  # type: ignore[index]
        chunk_count = 0

        try:
            if not converter.can_process(str(absolute_path)):
                logger.warning(f"Skipping unsupported file type: {absolute_path.name}")
                progress_logger.update_progress(
                    increment=1,
                    skipped=1,
                    custom_message=f"Skipped (unsupported): {absolute_path.name}",
                )
                continue

            with absolute_path.open("rb") as file_handle:
                raw_content = file_handle.read()

            processed_content, content_type = converter.convert_to_processable_content(
                str(absolute_path), raw_content
            )

            base_metadata = {
                "path": document_path,
                "name": file_info["name"],
                "file_type": file_info["file_type"],
                "size": file_info["size_bytes"],
                "size_mb": file_info["size_mb"],
                "modified_time": file_info["modified_time"],
                "namespace": args.namespace,
                "extraction_confidence": 0.0,
            }

            parsed = parser.parse(processed_content, base_metadata, content_type=content_type)
            text_content = parsed.text.strip() if parsed.text else ""

            if not text_content:
                logger.warning(f"No text extracted from {absolute_path.name}")
                progress_logger.update_progress(
                    increment=1,
                    skipped=1,
                    custom_message=f"No text: {absolute_path.name}",
                )
                continue

            chunk_input_metadata = dict(parsed.metadata)
            chunk_input_metadata.setdefault("namespace", args.namespace)

            chunks = chunker.chunk_document(text_content, chunk_input_metadata)

            if not chunks:
                logger.warning(f"Chunker returned no chunks for {absolute_path.name}")
                progress_logger.update_progress(
                    increment=1,
                    skipped=1,
                    custom_message=f"No chunks: {absolute_path.name}",
                )
                continue

            embedding_result = pinecone_client._generate_embeddings(
                [chunk.text for chunk in chunks],
                input_type="passage",
            )

            dense_embeddings = embedding_result["dense_embeddings"]
            sparse_embeddings = embedding_result["sparse_embeddings"]

            if len(dense_embeddings) != len(chunks):
                raise ValueError("Mismatch between chunk count and dense embeddings")
            if len(sparse_embeddings) != len(chunks):
                raise ValueError("Mismatch between chunk count and sparse embeddings")

            chunk_payloads = prepare_chunk_payloads(
                chunks,
                dense_embeddings,
                sparse_embeddings,
                document_path,
            )

            if not chunk_payloads:
                logger.warning(f"No chunk payloads prepared for {absolute_path.name}")
                progress_logger.update_progress(
                    increment=1,
                    skipped=1,
                    custom_message=f"No payloads: {absolute_path.name}",
                )
                continue

            upload_success = pinecone_client.upsert_chunks(chunk_payloads, namespace=args.namespace)

            if not upload_success:
                raise RuntimeError("Pinecone upsert_chunks returned False")

            chunk_count = len(chunk_payloads)
            total_chunks += chunk_count

            progress_logger.update_progress(
                increment=1,
                chunks_created=chunk_count,
                custom_message=f"Processed {absolute_path.name} ({chunk_count} chunks)",
            )
            progress_logger.log_processing_detail(
                f"Processed {document_path} | chunks={chunk_count}"
            )

        except Exception as exc:  # pylint: disable=broad-except
            failed_files.append(document_path)
            logger.error(f"Failed to process {absolute_path.name}: {exc}")
            progress_logger.log_error(
                f"Failed to process {document_path}", error_details=str(exc)
            )
            progress_logger.update_progress(
                increment=1,
                failed=1,
                custom_message=f"Failed: {absolute_path.name}",
            )

    additional_stats = {
        "Namespace": args.namespace,
        "Index": args.index_name,
        "Total Chunks": f"{total_chunks:,}",
        "Failed Files": len(failed_files),
    }

    if failed_files:
        additional_stats["Failed Paths"] = failed_files

    progress_logger.log_completion_summary(additional_stats)


if __name__ == "__main__":
    main()

