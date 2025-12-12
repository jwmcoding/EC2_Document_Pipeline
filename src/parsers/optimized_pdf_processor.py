"""
Optimized PDF Processor - Phase 2 Performance Enhancement
Smart PDF processing with size-based strategies and aggressive performance settings
"""

import pdfplumber
import io
import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import platform
import signal
from contextlib import contextmanager


@dataclass
class PDFProcessingStrategy:
    """PDF processing strategy based on document characteristics"""
    name: str
    timeout_seconds: int
    max_pages: Optional[int]
    sample_pages: bool
    table_extraction: bool
    advanced_layout: bool
    pdf_settings: Dict[str, Any]


class OptimizedPDFProcessor:
    """
    Size-aware PDF processor with aggressive performance optimizations
    
    Strategies:
    - Small PDFs (<1MB, <10 pages): Full processing with all features
    - Medium PDFs (1-10MB, 10-50 pages): Optimized processing, reduced precision
    - Large PDFs (>10MB, >50 pages): Sample-based processing, minimal features
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Define processing strategies
        self.strategies = {
            "complete": PDFProcessingStrategy(
                name="complete",
                timeout_seconds=300,  # 5 minutes - generous timeout
                max_pages=None,       # NO PAGE LIMIT
                sample_pages=False,   # NO SAMPLING
                table_extraction=True, # FULL TABLE EXTRACTION
                advanced_layout=True,  # FULL LAYOUT ANALYSIS
                pdf_settings={
                    'laparams': {
                        'char_margin': 2.0,    # Good quality extraction
                        'word_margin': 0.1,    
                        'line_margin': 0.5,    
                        'boxes_flow': 0.5      
                    }
                }
            )
        }
    
    def process_pdf_optimized(self, content: bytes, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process PDF with size-based optimization strategy
        
        Args:
            content: PDF content as bytes
            metadata: Document metadata
            
        Returns:
            Processing result with text and metadata
        """
        start_time = time.time()
        
        # Categorize PDF for optimal processing
        strategy = self._categorize_pdf(content)
        
        self.logger.info(f"📊 PDF Strategy: {strategy.name} "
                        f"(timeout: {strategy.timeout_seconds}s, "
                        f"max_pages: {strategy.max_pages})")
        
        try:
            # Process with selected strategy
            result = self._process_with_strategy(content, metadata, strategy)
            
            processing_time = time.time() - start_time
            result['processing_time'] = processing_time
            result['strategy_used'] = strategy.name
            
            self.logger.info(f"⚡ PDF processed in {processing_time:.2f}s using {strategy.name} strategy")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ PDF processing failed with {strategy.name} strategy: {e}")
            
            # Fallback to simplest strategy
            if strategy.name != "large":
                self.logger.info("🔄 Falling back to large PDF strategy...")
                return self._process_with_strategy(content, metadata, self.strategies["large"])
            else:
                raise
    
    def _categorize_pdf(self, content: bytes) -> PDFProcessingStrategy:
        """Return complete processing strategy - NO TRUNCATION"""
        # Always use complete processing to preserve all data
        return self.strategies["complete"]
    
    def _estimate_page_count(self, content: bytes) -> int:
        """Quick estimation of page count without full parsing"""
        try:
            # Fast page count using pdfplumber
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                return len(pdf.pages)
        except:
            # Fallback estimation based on file size
            size_mb = len(content) / (1024 * 1024)
            return max(1, int(size_mb * 10))  # Rough estimate: 10 pages per MB
    
    def _process_with_strategy(self, content: bytes, metadata: Dict[str, Any], 
                             strategy: PDFProcessingStrategy) -> Dict[str, Any]:
        """Process PDF using specific strategy"""
        
        with self._timeout_context(strategy.timeout_seconds):
            with pdfplumber.open(io.BytesIO(content), **strategy.pdf_settings) as pdf:
                
                total_pages = len(pdf.pages)
                self.logger.debug(f"📄 Processing {total_pages} pages with {strategy.name} strategy")
                
                # Determine pages to process
                pages_to_process = self._get_pages_to_process(pdf, strategy)
                
                text_parts = []
                tables = []
                page_info = []
                
                for page_num in pages_to_process:
                    page = pdf.pages[page_num - 1]  # Convert to 0-based index
                    
                    page_start = time.time()
                    
                    # Extract text
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(f"=== Page {page_num} ===")
                        text_parts.append(page_text)
                        text_parts.append("")
                    
                    # Extract tables only for small PDFs
                    if strategy.table_extraction:
                        page_tables = page.extract_tables()
                        if page_tables:
                            for table_idx, table in enumerate(page_tables):
                                tables.append({
                                    'page': page_num,
                                    'table_index': table_idx,
                                    'data': table,
                                    'rows': len(table),
                                    'cols': len(table[0]) if table else 0
                                })
                    
                    # Track page processing time
                    page_time = time.time() - page_start
                    if page_time > 5.0:  # Log slow pages
                        self.logger.warning(f"⏱️ Slow page {page_num}: {page_time:.2f}s")
                    
                    page_info.append({
                        'page_number': page_num,
                        'text_length': len(page_text) if page_text else 0,
                        'processing_time': page_time
                    })
                
                # Add sampling note for large PDFs
                if strategy.sample_pages and total_pages > len(pages_to_process):
                    text_parts.insert(0, f"=== SAMPLED PDF: Processed {len(pages_to_process)}/{total_pages} pages ===")
                
                # Combine results
                full_text = "\n".join(text_parts)
                
                return {
                    'text': full_text,
                    'metadata': {
                        **metadata,
                        'parser': 'optimized_pdf',
                        'strategy': strategy.name,
                        'total_pages': total_pages,
                        'processed_pages': len(pages_to_process),
                        'sampled': strategy.sample_pages and total_pages > len(pages_to_process),
                        'tables_extracted': len(tables),
                        'text_length': len(full_text)
                    },
                    'tables': tables,
                    'page_info': page_info,
                    'success': True
                }
    
    def _get_pages_to_process(self, pdf, strategy: PDFProcessingStrategy) -> List[int]:
        """Determine which pages to process based on strategy"""
        total_pages = len(pdf.pages)
        
        if not strategy.max_pages or total_pages <= strategy.max_pages:
            # Process all pages
            return list(range(1, total_pages + 1))
        
        if strategy.sample_pages:
            # Sample strategy: first pages + last pages + middle sample
            first_pages = min(10, total_pages // 3)
            last_pages = min(5, total_pages // 4)
            
            pages = []
            # First pages
            pages.extend(range(1, first_pages + 1))
            
            # Middle sample
            if total_pages > 20:
                middle_start = total_pages // 2 - 2
                middle_end = total_pages // 2 + 3
                pages.extend(range(middle_start, min(middle_end, total_pages + 1)))
            
            # Last pages
            if total_pages > last_pages:
                pages.extend(range(total_pages - last_pages + 1, total_pages + 1))
            
            return sorted(list(set(pages)))
        else:
            # Just take first N pages
            return list(range(1, min(strategy.max_pages, total_pages) + 1))
    
    @contextmanager
    def _timeout_context(self, timeout_seconds: int):
        """Context manager for PDF processing timeout - disabled for parallel processing"""
        # Signal handlers don't work in worker threads, so we skip timeout for parallel processing
        # This is safe since we have generous timeouts and parallel processing provides natural limits
        self.logger.debug(f"Processing PDF with {timeout_seconds}s timeout (disabled in parallel mode)")
        yield
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get statistics about optimization strategies"""
        return {
            'strategies': {name: {
                'timeout': strategy.timeout_seconds,
                'max_pages': strategy.max_pages,
                'sample_pages': strategy.sample_pages,
                'table_extraction': strategy.table_extraction,
                'advanced_layout': strategy.advanced_layout
            } for name, strategy in self.strategies.items()}
        } 