#!/usr/bin/env python3
"""
Retrieve and display chunk text from Pinecone metadata for comparison with original PDFs.

This script queries Pinecone to retrieve chunks for a specific document and displays
the full text from the metadata 'text' field, allowing you to compare it with the
original PDF to judge parsing effectiveness.
"""

import os
import sys
import argparse
from typing import List, Dict, Any, Optional

# Add src to path
sys.path.insert(0, 'src')
sys.path.insert(0, '.')

from dotenv import load_dotenv
load_dotenv()

from src.connectors.pinecone_client import PineconeDocumentClient
from src.config.settings import Settings


def retrieve_document_chunks(
    pinecone_client: PineconeDocumentClient,
    namespace: str,
    document_path: Optional[str] = None,
    file_name: Optional[str] = None,
    deal_id: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Retrieve chunks for a document using various filter options."""
    
    # Build filter
    filter_dict = {}
    if document_path:
        filter_dict["document_path"] = {"$eq": document_path}
    if file_name:
        filter_dict["file_name"] = {"$eq": file_name}
    if deal_id:
        filter_dict["deal_id"] = {"$eq": deal_id}
    
    chunks = []
    
    # If we have filters, use query method (more efficient for filtered searches)
    if filter_dict:
        # Query Pinecone with filters
        results = pinecone_client.index.query(
            vector=[0.0] * 1024,  # Dummy vector (1024 dim for multilingual-e5-large)
            top_k=limit * 2,  # Get more results to account for filtering
            namespace=namespace,
            include_metadata=True,
            filter=filter_dict
        )
        
        for match in results.matches:
            metadata = match.metadata if hasattr(match, 'metadata') else {}
            
            # Double-check filter matches (Pinecone filters can be approximate)
            if file_name and metadata.get('file_name') != file_name:
                continue
            if document_path and metadata.get('document_path') != document_path:
                continue
            if deal_id and metadata.get('deal_id') != deal_id:
                continue
            
            # Extract text from metadata
            text = metadata.get('text', '')
            
            chunk_data = {
                'id': match.id,
                'score': match.score if hasattr(match, 'score') else None,
                'text': text,
                'text_length': len(text),
                'chunk_index': metadata.get('chunk_index', -1),
                'file_name': metadata.get('file_name', ''),
                'document_path': metadata.get('document_path', ''),
                'deal_id': metadata.get('deal_id', ''),
                'vendor': metadata.get('vendor', ''),
                'client': metadata.get('client', ''),
                'section_name': metadata.get('section_name', ''),
                'chunk_type': metadata.get('chunk_type', ''),
                'metadata': metadata
            }
            chunks.append(chunk_data)
    
    else:
        # No filters - enumerate all chunks (slower but complete)
        print("⚠️  No filter specified. Enumerating all chunks (this may take a while)...")
        
        total_processed = 0
        batch_size = 100
        
        try:
            for vector_ids_batch in pinecone_client.index.list(
                namespace=namespace,
                limit=batch_size
            ):
                if not vector_ids_batch:
                    break
                
                # Fetch metadata for this batch
                fetch_results = pinecone_client.index.fetch(
                    ids=list(vector_ids_batch),
                    namespace=namespace
                )
                
                for vector_id, vector_data in fetch_results.vectors.items():
                    metadata = vector_data.metadata or {}
                    
                    # Extract text from metadata
                    text = metadata.get('text', '')
                    
                    chunk_data = {
                        'id': vector_id,
                        'score': None,
                        'text': text,
                        'text_length': len(text),
                        'chunk_index': metadata.get('chunk_index', -1),
                        'file_name': metadata.get('file_name', ''),
                        'document_path': metadata.get('document_path', ''),
                        'deal_id': metadata.get('deal_id', ''),
                        'vendor': metadata.get('vendor', ''),
                        'client': metadata.get('client', ''),
                        'section_name': metadata.get('section_name', ''),
                        'chunk_type': metadata.get('chunk_type', ''),
                        'metadata': metadata
                    }
                    chunks.append(chunk_data)
                    
                    if limit and len(chunks) >= limit:
                        break
                
                total_processed += len(vector_ids_batch)
                
                if limit and len(chunks) >= limit:
                    break
                
                # Safety limit
                if total_processed >= 10000:
                    break
                    
        except Exception as e:
            print(f"⚠️  Warning: list() method failed ({e}), falling back to query method")
            # Fallback to query method
            results = pinecone_client.index.query(
                vector=[0.0] * 1024,
                top_k=limit,
                namespace=namespace,
                include_metadata=True
            )
            
            for match in results.matches:
                metadata = match.metadata if hasattr(match, 'metadata') else {}
                text = metadata.get('text', '')
                
                chunk_data = {
                    'id': match.id,
                    'score': match.score if hasattr(match, 'score') else None,
                    'text': text,
                    'text_length': len(text),
                    'chunk_index': metadata.get('chunk_index', -1),
                    'file_name': metadata.get('file_name', ''),
                    'document_path': metadata.get('document_path', ''),
                    'deal_id': metadata.get('deal_id', ''),
                    'vendor': metadata.get('vendor', ''),
                    'client': metadata.get('client', ''),
                    'section_name': metadata.get('section_name', ''),
                    'chunk_type': metadata.get('chunk_type', ''),
                    'metadata': metadata
                }
                chunks.append(chunk_data)
    
    # Sort by chunk_index
    chunks.sort(key=lambda x: x.get('chunk_index', 0))
    
    return chunks[:limit] if limit else chunks


def display_chunk_text(chunks: List[Dict[str, Any]], show_full_text: bool = True, output_file: Optional[str] = None):
    """Display chunk text in a readable format, optionally save to file."""
    
    if not chunks:
        print("❌ No chunks found")
        return
    
    # Group by document
    docs = {}
    for chunk in chunks:
        doc_key = chunk.get('file_name') or chunk.get('document_path') or 'unknown'
        if doc_key not in docs:
            docs[doc_key] = []
        docs[doc_key].append(chunk)
    
    # Sort chunks by chunk_index
    for doc_key in docs:
        docs[doc_key].sort(key=lambda x: x.get('chunk_index', 0))
    
    # Prepare output (console and/or file)
    output_lines = []
    
    output_lines.append("=" * 80)
    output_lines.append(f"📄 RETRIEVED CHUNKS: {len(chunks)} total chunks from {len(docs)} document(s)")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    for doc_key, doc_chunks in docs.items():
        output_lines.append("=" * 80)
        output_lines.append(f"📄 DOCUMENT: {doc_key}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        # Show document metadata from first chunk
        if doc_chunks:
            first_chunk = doc_chunks[0]
            output_lines.append("📋 Document Metadata:")
            output_lines.append(f"   File Name: {first_chunk.get('file_name', 'N/A')}")
            output_lines.append(f"   Document Path: {first_chunk.get('document_path', 'N/A')}")
            output_lines.append(f"   Deal ID: {first_chunk.get('deal_id', 'N/A')}")
            output_lines.append(f"   Vendor: {first_chunk.get('vendor', 'N/A')}")
            output_lines.append(f"   Client: {first_chunk.get('client', 'N/A')}")
            output_lines.append(f"   Total Chunks: {len(doc_chunks)}")
            output_lines.append("")
        
        # Assemble all chunk text for file output
        all_text_parts = []
        
        # Show each chunk
        for i, chunk in enumerate(doc_chunks, 1):
            output_lines.append("─" * 80)
            output_lines.append(f"CHUNK {i}/{len(doc_chunks)} (Index: {chunk.get('chunk_index', -1)})")
            output_lines.append("─" * 80)
            output_lines.append(f"Chunk ID: {chunk.get('id', 'N/A')}")
            output_lines.append(f"Section: {chunk.get('section_name', 'N/A')}")
            output_lines.append(f"Type: {chunk.get('chunk_type', 'N/A')}")
            output_lines.append(f"Text Length: {chunk.get('text_length', 0):,} characters")
            
            if chunk.get('score') is not None:
                output_lines.append(f"Score: {chunk.get('score', 0):.4f}")
            
            # Show text
            text = chunk.get('text', '')
            if text:
                if show_full_text:
                    output_lines.append("")
                    output_lines.append("📝 FULL TEXT:")
                    output_lines.append("─" * 80)
                    output_lines.append(text)
                    output_lines.append("─" * 80)
                    # Add to assembled text
                    all_text_parts.append(text)
                else:
                    # Show preview
                    preview_length = 500
                    preview = text[:preview_length]
                    output_lines.append("")
                    output_lines.append(f"📝 TEXT PREVIEW (first {preview_length} chars):")
                    output_lines.append("─" * 80)
                    output_lines.append(preview)
                    if len(text) > preview_length:
                        output_lines.append(f"\n... ({len(text) - preview_length:,} more characters)")
                    output_lines.append("─" * 80)
                    # Add full text to assembled text even if showing preview
                    all_text_parts.append(text)
            else:
                output_lines.append("")
                output_lines.append("⚠️  No text found in metadata")
            
            output_lines.append("")
        
        # If output file specified, create assembled text version
        if output_file and all_text_parts and doc_chunks:
            # Create clean filename from document name
            safe_filename = "".join(c for c in doc_key if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            safe_filename = safe_filename.replace(' ', '_')[:100]  # Limit length
            # If output_file already has .txt extension, insert filename before it
            if output_file.endswith('.txt'):
                output_txt_file = output_file.replace('.txt', f'_{safe_filename}.txt')
            else:
                output_txt_file = f"{output_file}_{safe_filename}.txt"
            
            # Get metadata from first chunk
            first_chunk_meta = doc_chunks[0] if doc_chunks else {}
            
            # Write assembled text
            with open(output_txt_file, 'w', encoding='utf-8') as f:
                f.write(f"Document: {doc_key}\n")
                f.write(f"Deal ID: {first_chunk_meta.get('deal_id', 'N/A')}\n")
                f.write(f"Vendor: {first_chunk_meta.get('vendor', 'N/A')}\n")
                f.write(f"Client: {first_chunk_meta.get('client', 'N/A')}\n")
                f.write(f"Total Chunks: {len(doc_chunks)}\n")
                f.write("=" * 80 + "\n\n")
                
                # Write all chunks assembled
                for i, text_part in enumerate(all_text_parts, 1):
                    f.write(f"\n--- Chunk {i} ---\n\n")
                    f.write(text_part)
                    f.write("\n\n")
            
            print(f"💾 Saved assembled text to: {output_txt_file}")
    
    # Print to console
    print("\n".join(output_lines))


def list_documents_in_namespace(
    pinecone_client: PineconeDocumentClient,
    namespace: str,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """List unique documents in a namespace using proper enumeration."""
    
    # Use Pinecone's list() method to properly enumerate all vectors
    # This is more reliable than querying with dummy vectors
    docs = {}
    total_processed = 0
    batch_size = 100
    
    try:
        # Iterate through all vectors in the namespace
        for vector_ids_batch in pinecone_client.index.list(
            namespace=namespace,
            limit=batch_size
        ):
            if not vector_ids_batch:
                break
            
            # Fetch metadata for this batch
            fetch_results = pinecone_client.index.fetch(
                ids=list(vector_ids_batch),
                namespace=namespace
            )
            
            for vector_id, vector_data in fetch_results.vectors.items():
                metadata = vector_data.metadata or {}
                file_name = metadata.get('file_name', '')
                document_path = metadata.get('document_path', '')
                deal_id = metadata.get('deal_id', '')
                
                # Use file_name as primary key, fallback to document_path
                doc_key = file_name or document_path or 'unknown'
                
                if doc_key not in docs:
                    docs[doc_key] = {
                        'file_name': file_name,
                        'document_path': document_path,
                        'deal_id': deal_id,
                        'vendor': metadata.get('vendor', ''),
                        'client': metadata.get('client', ''),
                        'chunk_count': 0
                    }
                docs[doc_key]['chunk_count'] += 1
            
            total_processed += len(vector_ids_batch)
            
            # Apply limit early if we have enough unique documents
            if limit and len(docs) >= limit:
                break
            
            # Safety limit to prevent infinite loops
            if total_processed >= 10000:
                break
        
        # Return documents sorted by chunk count (most chunks first)
        result = sorted(docs.values(), key=lambda x: x['chunk_count'], reverse=True)
        
        # Apply limit if specified
        if limit:
            result = result[:limit]
        
        return result
        
    except Exception as e:
        # Fallback to old query method if list() fails
        print(f"⚠️  Warning: list() method failed ({e}), falling back to query method")
        results = pinecone_client.index.query(
            vector=[0.0] * 1024,
            top_k=limit * 10,
            namespace=namespace,
            include_metadata=True
        )
        
        docs = {}
        for match in results.matches:
            metadata = match.metadata if hasattr(match, 'metadata') else {}
            file_name = metadata.get('file_name', '')
            document_path = metadata.get('document_path', '')
            deal_id = metadata.get('deal_id', '')
            
            doc_key = file_name or document_path or 'unknown'
            
            if doc_key not in docs:
                docs[doc_key] = {
                    'file_name': file_name,
                    'document_path': document_path,
                    'deal_id': deal_id,
                    'vendor': metadata.get('vendor', ''),
                    'client': metadata.get('client', ''),
                    'chunk_count': 0
                }
            docs[doc_key]['chunk_count'] += 1
        
        return list(docs.values())[:limit] if limit else list(docs.values())


def main():
    parser = argparse.ArgumentParser(
        description="Retrieve and display chunk text from Pinecone metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List documents in namespace
  python retrieve_chunk_text_from_pinecone.py --namespace benchmark-250docs-docling-auto-ocr-2025-12-12 --list-docs

  # Retrieve chunks for a specific file
  python retrieve_chunk_text_from_pinecone.py --namespace benchmark-250docs-docling-auto-ocr-2025-12-12 \\
      --file-name "example.pdf"

  # Retrieve chunks for a specific document path
  python retrieve_chunk_text_from_pinecone.py --namespace benchmark-250docs-docling-auto-ocr-2025-12-12 \\
      --document-path "/path/to/document.pdf"

  # Retrieve chunks for a deal
  python retrieve_chunk_text_from_pinecone.py --namespace benchmark-250docs-docling-auto-ocr-2025-12-12 \\
      --deal-id "0680y0000035YhlAAE"

  # Show preview only (first 500 chars)
  python retrieve_chunk_text_from_pinecone.py --namespace benchmark-250docs-docling-auto-ocr-2025-12-12 \\
      --file-name "example.pdf" --preview-only
        """
    )
    
    parser.add_argument(
        '--namespace',
        type=str,
        required=True,
        help='Pinecone namespace to query'
    )
    
    parser.add_argument(
        '--document-path',
        type=str,
        help='Filter by document_path'
    )
    
    parser.add_argument(
        '--file-name',
        type=str,
        help='Filter by file_name'
    )
    
    parser.add_argument(
        '--deal-id',
        type=str,
        help='Filter by deal_id'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of chunks to retrieve (default: 100)'
    )
    
    parser.add_argument(
        '--preview-only',
        action='store_true',
        help='Show only text preview (first 500 chars) instead of full text'
    )
    
    parser.add_argument(
        '--list-docs',
        action='store_true',
        help='List unique documents in namespace instead of retrieving chunks'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Save assembled chunk text to file (e.g., output.txt). Creates one file per document.'
    )
    
    args = parser.parse_args()
    
    # Initialize Pinecone client
    settings = Settings()
    api_key = os.getenv("PINECONE_API_KEY")
    
    if not api_key:
        print("❌ PINECONE_API_KEY not found in environment")
        sys.exit(1)
    
    try:
        pinecone_client = PineconeDocumentClient(
            api_key=api_key,
            index_name=settings.PINECONE_INDEX_NAME
        )
        
        if args.list_docs:
            print(f"📋 Listing documents in namespace: {args.namespace}")
            docs = list_documents_in_namespace(pinecone_client, args.namespace, limit=args.limit)
            
            print(f"\n{'=' * 80}")
            print(f"Found {len(docs)} unique documents")
            print(f"{'=' * 80}\n")
            
            for i, doc in enumerate(docs, 1):
                print(f"{i}. {doc['file_name'] or doc['document_path']}")
                print(f"   Deal ID: {doc['deal_id']}")
                print(f"   Vendor: {doc['vendor']}")
                print(f"   Client: {doc['client']}")
                print(f"   Chunks: {doc['chunk_count']}")
                print()
        else:
            # Retrieve chunks
            chunks = retrieve_document_chunks(
                pinecone_client=pinecone_client,
                namespace=args.namespace,
                document_path=args.document_path,
                file_name=args.file_name,
                deal_id=args.deal_id,
                limit=args.limit
            )
            
            # Display chunks
            display_chunk_text(chunks, show_full_text=not args.preview_only, output_file=args.output)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


