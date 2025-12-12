"""
PDFPlumber Parser - Universal Document Processor
Main parser using PDFPlumber for all document processing

Updated Dec 2025: Tables now formatted using unified table_formatter
for consistent detection and preservation during chunking.
"""

import pdfplumber
import io
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import logging
import re
import time
import signal
from contextlib import contextmanager
import platform

from src.parsers.table_formatter import format_table_for_chunking


@dataclass
class ParsedContent:
    """Container for parsed document content with metadata"""
    text: str
    metadata: Dict[str, Any]
    tables: List[Dict] = None
    page_info: List[Dict] = None


class PDFPlumberParser:
    """Universal parser using PDFPlumber for all document processing"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Table extraction settings for bordered tables
        # NOTE (Dec 2025): Tuned to better handle Vodafone-style layouts where
        # headers and cells are separated by thin lines but should still be
        # treated as a single table. We use a slightly looser line strategy
        # while keeping a conservative text-based fallback below.
        self.table_settings = {
            "vertical_strategy": "lines",       # Prefer line-based detection
            "horizontal_strategy": "lines",
            "snap_tolerance": 6,                # Join nearly-touching lines more aggressively
            "join_tolerance": 6,
            "edge_min_length": 1,               # Allow shorter edges for thinner borders
            "min_words_vertical": 1,
            "min_words_horizontal": 1,
            "text_tolerance": 6,
            "intersection_tolerance": 6,
        }
        
        # Fallback settings for tables without clear borders
        self.table_settings_fallback = {
            "vertical_strategy": "text",
            "horizontal_strategy": "text",
            "snap_tolerance": 3,
            "join_tolerance": 3,
        }
        
        # Business document section markers for enhanced extraction
        self.section_markers = [
            'executive summary', 'summary', 'overview',
            'terms and conditions', 'terms', 'conditions',
            'pricing', 'costs', 'fees', 'payment', 'financial',
            'deliverables', 'scope', 'work', 'services',
            'timeline', 'schedule', 'dates', 'milestones',
            'contact', 'signature', 'agreement', 'contract',
            'responsibilities', 'obligations', 'requirements'
        ]
    
    def parse(self, content: bytes, metadata: Dict[str, Any], content_type: str = 'pdf') -> ParsedContent:
        """
        Parse content using PDFPlumber or direct text processing
        
        Args:
            content: File content as bytes
            metadata: Document metadata
            content_type: 'pdf' or 'text'
        """
        
        if content_type == 'text':
            return self._parse_text_content(content, metadata)
        else:
            return self._parse_pdf_content(content, metadata)
    
    def _parse_pdf_content(self, content: bytes, metadata: Dict[str, Any]) -> ParsedContent:
        """Parse PDF content with PDFPlumber"""
        from contextlib import contextmanager
        
        @contextmanager
        def timeout_context(timeout_seconds=60):
            """Context manager for PDF processing timeout"""
            if platform.system() == "Windows":
                # Windows doesn't support signal.SIGALRM, so just proceed without timeout
                self.logger.debug("Windows detected - timeout not available, proceeding without limit")
                yield
            else:
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"PDF processing exceeded {timeout_seconds} seconds")
                
                # Set up timeout (Unix only)
                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout_seconds)
                try:
                    yield
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
        
        try:
            start_time = time.time()
            text_parts = []
            tables = []
            page_info = []
            
            with timeout_context(timeout_seconds=240):  # 4 minute timeout
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    self.logger.info(f"Processing PDF with {len(pdf.pages)} pages")
                    
                    # Check for unusually large PDFs
                    if len(pdf.pages) > 50:
                        self.logger.warning(f"Large PDF detected: {len(pdf.pages)} pages - may take longer to process")
                    
                    for page_num, page in enumerate(pdf.pages, 1):
                        page_start = time.time()
                        
                        # Extract text from page
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(f"=== Page {page_num} ===")
                            text_parts.append(page_text)
                        
                        # Extract tables from page with optimized settings
                        # Try lines-based detection first (better for bordered tables)
                        page_tables = page.extract_tables(self.table_settings)
                        
                        # Fallback to text-based detection if no tables found
                        if not page_tables:
                            page_tables = page.extract_tables(self.table_settings_fallback)
                        
                        # Insert tables INLINE after page text (preserves context)
                        if page_tables:
                            for table_idx, table in enumerate(page_tables):
                                # Store table metadata
                                tables.append({
                                    'page': page_num,
                                    'table_index': table_idx,
                                    'data': table,
                                    'rows': len(table),
                                    'cols': len(table[0]) if table else 0
                                })
                                
                                # Format table for chunker detection and insert inline
                                table_name = f"TABLE_P{page_num}_{table_idx + 1}"
                                formatted_table = format_table_for_chunking(
                                    table,
                                    table_name,
                                    page_num=page_num
                                )
                                if formatted_table:
                                    text_parts.append("")  # Blank line before table
                                    text_parts.append(formatted_table)
                                    self.logger.debug(f"Formatted {table_name} with {len(table)} rows")
                        
                        text_parts.append("")  # Empty line between pages
                        
                        # Store page metadata
                        page_info.append({
                            'page_number': page_num,
                            'width': page.width,
                            'height': page.height,
                            'text_length': len(page_text) if page_text else 0,
                            'table_count': len(page_tables) if page_tables else 0
                        })
                        
                        page_time = time.time() - page_start
                        # Log slow pages
                        if page_time > 5.0:
                            self.logger.warning(f"Slow page processing: Page {page_num} took {page_time:.1f}s")
                        
                        # Check for runaway processing
                        if page_time > 30.0:
                            self.logger.error(f"Extremely slow page {page_num} - may indicate PDF issues")
            
            processing_time = time.time() - start_time
            if processing_time > 10.0:
                self.logger.warning(f"PDF processing took {processing_time:.1f}s - consider optimization")
            
            # Combine all text (tables are already inline)
            full_text = "\n".join(text_parts)
            
            if tables:
                self.logger.info(f"Extracted and formatted {len(tables)} tables inline for chunker preservation")
            
            # Enhanced metadata
            enhanced_metadata = {
                **metadata,
                'parser': 'pdfplumber',
                'total_pages': len(page_info),
                'total_tables': len(tables),
                'text_length': len(full_text),
                'processing_method': 'pdf_extraction'
            }
            
            return ParsedContent(
                text=full_text,
                metadata=enhanced_metadata,
                tables=tables,
                page_info=page_info
            )
            
        except TimeoutError as e:
            self.logger.error(f"PDF processing timeout: {e}")
            # Return partial results with timeout info
            return ParsedContent(
                text=f"PDF processing timed out after 4 minutes. File may be corrupt or extremely complex: {metadata.get('name', 'unknown')}",
                metadata={**metadata, 'parser': 'pdfplumber_timeout', 'error': str(e), 'processing_time': 240}
            )
        except Exception as e:
            self.logger.error(f"PDFPlumber parsing failed: {e}")
            # Fallback to basic text extraction
            try:
                fallback_text = f"PDFPlumber extraction failed: {str(e)}\n\nFile: {metadata.get('name', 'unknown')}\nThis may indicate a corrupted or complex PDF."
                return ParsedContent(
                    text=fallback_text,
                    metadata={**metadata, 'parser': 'pdfplumber_fallback', 'error': str(e)}
                )
            except Exception as fallback_error:
                raise Exception(f"All parsing attempts failed: {fallback_error}")
    
    def _parse_text_content(self, content: bytes, metadata: Dict[str, Any]) -> ParsedContent:
        """Parse direct text content (from converted files)"""
        try:
            text = content.decode('utf-8')
            
            enhanced_metadata = {
                **metadata,
                'parser': 'pdfplumber_text',
                'text_length': len(text),
                'processing_method': 'direct_text'
            }
            
            return ParsedContent(
                text=text,
                metadata=enhanced_metadata
            )
            
        except Exception as e:
            self.logger.error(f"Text parsing failed: {e}")
            raise Exception(f"Text parsing failed: {e}")
    
    def extract_key_sections(self, text: str) -> Dict[str, str]:
        """
        Extract key sections that might be useful for business documents
        This can be enhanced later when we add more sophisticated parsers
        """
        sections = {}
        
        # Simple section detection based on common patterns
        lines = text.split('\n')
        current_section = "main"
        current_content = []
        
        for line in lines:
            line_lower = line.lower().strip()
            
            # Check if this line starts a new section
            section_found = False
            for marker in self.section_markers:
                if marker in line_lower and len(line.strip()) < 100:  # Likely a header
                    # Save previous section
                    if current_content:
                        sections[current_section] = '\n'.join(current_content)
                    
                    current_section = marker.replace(' ', '_')
                    current_content = [line]
                    section_found = True
                    break
            
            if not section_found:
                current_content.append(line)
        
        # Save final section
        if current_content:
            sections[current_section] = '\n'.join(current_content)
        
        return sections
    
    def extract_business_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Extract business-relevant entities from text
        This is a simple implementation that can be enhanced with NLP models later
        """
        entities = {
            'amounts': [],
            'dates': [],
            'emails': [],
            'phone_numbers': [],
            'company_names': [],
            'contract_terms': []
        }
        
        # Amount patterns (dollars, percentages)
        amount_patterns = [
            r'\$[\d,]+(?:\.\d{2})?',  # $1,000.00
            r'\d+(?:\.\d+)?%',       # 15.5%
            r'\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:dollars?|USD)',  # 1,000 dollars
        ]
        
        for pattern in amount_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            entities['amounts'].extend(matches)
        
        # Date patterns
        date_patterns = [
            r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',  # MM/DD/YYYY or MM-DD-YYYY
            r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
            r'\b\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            entities['dates'].extend(matches)
        
        # Email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        entities['emails'] = re.findall(email_pattern, text)
        
        # Phone numbers
        phone_pattern = r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b'
        entities['phone_numbers'] = re.findall(phone_pattern, text)
        
        # Contract terms
        contract_terms = [
            'effective date', 'expiration date', 'termination', 'renewal',
            'confidentiality', 'non-disclosure', 'intellectual property',
            'liability', 'indemnification', 'force majeure'
        ]
        
        for term in contract_terms:
            if term in text.lower():
                entities['contract_terms'].append(term)
        
        # Remove duplicates
        for key in entities:
            entities[key] = list(set(entities[key]))
        
        return entities
    
    def get_document_summary(self, parsed_content: ParsedContent) -> Dict[str, Any]:
        """Generate a summary of the parsed document"""
        text = parsed_content.text
        metadata = parsed_content.metadata
        
        # Basic statistics
        word_count = len(text.split())
        char_count = len(text)
        line_count = len(text.split('\n'))
        
        # Extract key sections
        sections = self.extract_key_sections(text)
        
        # Extract business entities
        entities = self.extract_business_entities(text)
        
        # Quality indicators
        has_tables = bool(parsed_content.tables)
        table_count = len(parsed_content.tables) if parsed_content.tables else 0
        
        return {
            'statistics': {
                'word_count': word_count,
                'character_count': char_count,
                'line_count': line_count,
                'table_count': table_count
            },
            'sections': list(sections.keys()),
            'entities': entities,
            'processing_info': {
                'parser': metadata.get('parser', 'unknown'),
                'pages': metadata.get('total_pages', 0),
                'has_tables': has_tables,
                'processing_method': metadata.get('processing_method', 'unknown')
            },
            'content_indicators': {
                'appears_to_be_contract': any(term in text.lower() for term in ['agreement', 'contract', 'terms and conditions']),
                'contains_financial_info': bool(entities.get('amounts')),
                'contains_dates': bool(entities.get('dates')),
                'contains_contact_info': bool(entities.get('emails') or entities.get('phone_numbers'))
            }
        } 