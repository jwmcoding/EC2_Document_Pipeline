"""
Semantic Chunker with Business Context
Intelligent text chunking for business documents without external dependencies
Uses Pinecone embedding models for semantic analysis
"""

from typing import List, Dict, Any
import re
from dataclasses import dataclass
import logging


@dataclass
class Chunk:
    """Represents a semantically coherent chunk of text"""
    text: str
    metadata: Dict[str, Any]
    start_index: int
    end_index: int


class SemanticChunker:
    """
    Business-Aware Content Chunker (Not True Semantic Chunking)
    
    IMPORTANT: Despite the name "SemanticChunker", this is NOT true semantic chunking
    based on embedding similarity. This is actually "Business-Aware Content Chunking"
    that uses:
    
    - Fixed character limits (500 chars default) with overlap (75 chars default)
    - Business document structure awareness (sections, pricing, terms, etc.)
    - Sentence boundary preservation when possible
    - Natural break point detection (tables, lists, etc.)
    - Excel sheet preservation as single chunks
    
    True semantic chunking would group content by semantic similarity using embeddings,
    creating variable-sized chunks based on topic coherence rather than character counts.
    
    This approach works well for business documents because it:
    - Maintains consistent chunk sizes for vector storage
    - Preserves business context and document structure
    - Processes quickly without additional embedding API calls
    - Handles business document formats intelligently
    """
    
    def __init__(self, max_chunk_size: int = 500, 
                 overlap_size: int = 75,
                 similarity_threshold: float = 0.7,
                 excel_sheet_max_size: int = 2000):
        
        self.max_chunk_size = max_chunk_size
        self.overlap_size = overlap_size
        self.similarity_threshold = similarity_threshold
        self.excel_sheet_max_size = excel_sheet_max_size  # Allow larger chunks for Excel sheets
        self.logger = logging.getLogger(__name__)
        
        # Business-specific separators (in order of preference)
        self.business_separators = [
            "\n\n",  # Paragraph breaks (strongest)
            "\n",    # Line breaks
            ". ",    # Sentence endings
            "; ",    # Semicolons for lists
            ", ",    # Commas for clauses (weakest)
        ]
        
        # Business document section boundaries (strong chunk boundaries)
        self.section_boundaries = [
            'executive summary', 'summary', 'overview',
            'terms and conditions', 'terms', 'conditions',
            'pricing', 'costs', 'fees', 'payment',
            'deliverables', 'scope of work', 'services',
            'timeline', 'schedule', 'milestones',
            'signatures', 'agreement', 'contract',
            'appendix', 'exhibit', 'schedule'
        ]
    
    def chunk_document(self, content: str, metadata: Dict[str, Any]) -> List[Chunk]:
        """
        Create business-aware content chunks (character-limited, not semantic similarity)
        
        This method creates chunks by:
        1. Identifying business document sections (pricing, terms, etc.)
        2. Splitting into sentences with business-aware rules
        3. Grouping sentences up to character limit (max_chunk_size)
        4. Adding character-based overlap between chunks
        5. Preserving Excel sheets as single chunks when possible
        
        Note: This is NOT semantic similarity-based chunking - it's character-limited
        chunking with business document structure awareness.
        """
        
        if not content or len(content.strip()) < 50:
            self.logger.warning("Content too short for chunking")
            return []
        
        try:
            # First, identify major section boundaries
            sections = self._identify_business_sections(content)
            
            # Process each section separately to maintain business context
            all_chunks = []
            
            for section_name, section_content in sections.items():
                if len(section_content.strip()) < 50:
                    continue
                
                # Create chunks for this section
                section_chunks = self._process_section(
                    section_content, 
                    section_name, 
                    metadata,
                    start_chunk_index=len(all_chunks)
                )
                all_chunks.extend(section_chunks)
            
            self.logger.info(f"Created {len(all_chunks)} chunks from {len(sections)} sections")
            return all_chunks
            
        except Exception as e:
            self.logger.error(f"Error in semantic chunking: {e}")
            # Fallback to simple chunking
            return self._fallback_chunking(content, metadata)
    
    def _identify_business_sections(self, content: str) -> Dict[str, str]:
        """Identify major business document sections"""
        sections = {}
        lines = content.split('\n')
        current_section = "main"
        current_content = []
        
        for line in lines:
            line_lower = line.lower().strip()
            
            # Check if this line starts a new section
            section_found = False
            for boundary in self.section_boundaries:
                if boundary in line_lower and len(line.strip()) < 100:
                    # Save previous section
                    if current_content:
                        sections[current_section] = '\n'.join(current_content)
                    
                    current_section = boundary.replace(' ', '_')
                    current_content = [line]
                    section_found = True
                    break
            
            if not section_found:
                current_content.append(line)
        
        # Save final section
        if current_content:
            sections[current_section] = '\n'.join(current_content)
        
        return sections
    
    def _process_section(self, section_content: str, section_name: str, 
                        metadata: Dict[str, Any], start_chunk_index: int = 0) -> List[Chunk]:
        """Process a single business section into semantically coherent chunks"""
        
        # First, segment content into text vs table blocks (handles mixed content)
        segments = self._split_section_into_segments(section_content)
        
        all_chunks = []
        current_chunk_index = start_chunk_index
        
        for segment_type, segment_content in segments:
            if segment_type == "table":
                # Process table block (unified format or normalized from markdown)
                table_chunks = self._process_table_block(
                    segment_content, section_name, metadata, current_chunk_index
                )
                all_chunks.extend(table_chunks)
                current_chunk_index += len(table_chunks)
            else:
                # Process normal text segment
                sentences = self._split_into_sentences(segment_content)
                if sentences:
                    text_chunks = self._create_business_chunks(
                        sentences, section_name, metadata, current_chunk_index
                    )
                    all_chunks.extend(text_chunks)
                    current_chunk_index += len(text_chunks)
        
        return all_chunks
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """Split text into sentences using business-aware rules"""
        # Clean text
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Business document sentence patterns
        sentence_endings = r'[.!?]'
        
        # Split into potential sentences
        potential_sentences = re.split(f'({sentence_endings})', text)
        
        sentences = []
        current = ""
        
        for i, part in enumerate(potential_sentences):
            current += part
            
            # If this is a sentence ending
            if re.match(sentence_endings, part):
                # Check if this is a real sentence boundary
                if self._is_sentence_boundary(current):
                    sentences.append(current.strip())
                    current = ""
        
        # Add remaining text
        if current.strip():
            sentences.append(current.strip())
        
        # Filter very short or empty sentences
        return [s for s in sentences if len(s.split()) > 3]
    
    def _is_sentence_boundary(self, text: str) -> bool:
        """Determine if this is a true sentence boundary in business context"""
        # Avoid splitting on common business abbreviations
        business_abbreviations = [
            'inc.', 'corp.', 'ltd.', 'llc.', 'co.', 'vs.', 'etc.',
            'dept.', 'div.', 'mgmt.', 'admin.', 'svcs.', 'govt.',
            'no.', 'nos.', 'vol.', 'ch.', 'sec.', 'subsec.',
            'fig.', 'tbl.', 'ref.', 'app.', 'ex.', 'exh.'
        ]
        
        text_lower = text.lower().strip()
        for abbr in business_abbreviations:
            if text_lower.endswith(abbr):
                return False
        
        # Check for numbered lists (1. 2. etc.)
        if re.search(r'\d+\.$', text.strip()):
            return False
        
        # Must have reasonable length
        return len(text.split()) > 3
    
    def _create_business_chunks(self, sentences: List[str], section_name: str, 
                               metadata: Dict[str, Any], start_chunk_index: int) -> List[Chunk]:
        """Group sentences into business-aware chunks"""
        
        chunks = []
        current_chunk_sentences = []
        current_chunk_length = 0
        
        for sentence in sentences:
            sentence_length = len(sentence)
            
            # If adding this sentence would exceed max chunk size, finalize current chunk
            if (current_chunk_length + sentence_length > self.max_chunk_size and 
                current_chunk_sentences):
                
                chunk_text = " ".join(current_chunk_sentences)
                chunks.append(self._create_chunk(
                    chunk_text, section_name, metadata, 
                    len(chunks) + start_chunk_index
                ))
                
                # Start new chunk with overlap
                overlap_sentences = self._get_overlap_sentences(current_chunk_sentences)
                current_chunk_sentences = overlap_sentences
                current_chunk_length = sum(len(s) for s in overlap_sentences)
            
            # Add current sentence
            current_chunk_sentences.append(sentence)
            current_chunk_length += sentence_length
            
            # Check for natural business breaks (lists, tables, etc.)
            if self._is_natural_break_point(sentence):
                # If we have enough content, consider ending chunk here
                if current_chunk_length > 200 and len(current_chunk_sentences) > 2:
                    chunk_text = " ".join(current_chunk_sentences)
                    chunks.append(self._create_chunk(
                        chunk_text, section_name, metadata, 
                        len(chunks) + start_chunk_index
                    ))
                    current_chunk_sentences = []
                    current_chunk_length = 0
        
        # Add final chunk
        if current_chunk_sentences:
            chunk_text = " ".join(current_chunk_sentences)
            chunks.append(self._create_chunk(
                chunk_text, section_name, metadata, 
                len(chunks) + start_chunk_index
            ))
        
        return chunks
    
    def _is_natural_break_point(self, sentence: str) -> bool:
        """Check if this sentence represents a natural break point in business documents"""
        sentence_lower = sentence.lower()
        
        # Table or list endings
        if any(marker in sentence_lower for marker in ['table', 'list', 'exhibit', 'appendix']):
            return True
        
        # Section transitions
        if any(marker in sentence_lower for marker in ['therefore', 'in conclusion', 'furthermore', 'additionally']):
            return True
        
        # Numbered items (but not at start of sentence)
        if re.search(r'\d+\)', sentence) or re.search(r'[a-z]\)', sentence):
            return True
        
        return False
    
    def _get_overlap_sentences(self, sentences: List[str]) -> List[str]:
        """Get overlap sentences for chunk continuity"""
        if not sentences:
            return []
        
        # Calculate overlap based on character count
        total_chars = sum(len(s) for s in sentences)
        target_overlap_chars = min(self.overlap_size, total_chars // 2)
        
        overlap_sentences = []
        current_chars = 0
        
        # Take sentences from the end until we reach target overlap
        for sentence in reversed(sentences):
            if current_chars + len(sentence) <= target_overlap_chars:
                overlap_sentences.insert(0, sentence)
                current_chars += len(sentence)
            else:
                break
        
        return overlap_sentences
    
    def _create_chunk(self, text: str, section_name: str, metadata: Dict[str, Any], 
                     chunk_index: int) -> Chunk:
        """Create a chunk with enhanced metadata"""
        
        chunk_metadata = {
            **metadata,
            "chunk_index": chunk_index,
            "section_name": section_name,
            "chunk_type": self._classify_chunk_type(text),
            # NOTE: chunk_length removed from metadata - can be calculated from text if needed
        }
        
        return Chunk(
            text=text,
            metadata=chunk_metadata,
            start_index=0,  # Could calculate actual position if needed
            end_index=len(text)
        )
    
    def _classify_chunk_type(self, text: str) -> str:
        """Classify the type of business content in this chunk"""
        text_lower = text.lower()
        
        # Financial content
        if any(term in text_lower for term in ['$', 'cost', 'price', 'fee', 'payment', 'invoice']):
            return "financial"
        
        # Legal/Contract content
        if any(term in text_lower for term in ['agreement', 'contract', 'terms', 'conditions', 'liability']):
            return "legal"
        
        # Timeline/Schedule content
        if any(term in text_lower for term in ['date', 'schedule', 'timeline', 'deadline', 'milestone']):
            return "schedule"
        
        # Contact information
        if any(term in text_lower for term in ['contact', 'phone', 'email', 'address']):
            return "contact"
        
        # Technical specifications
        if any(term in text_lower for term in ['specification', 'requirement', 'technical', 'system']):
            return "technical"
        
        return "general"
    
    def _fallback_chunking(self, content: str, metadata: Dict[str, Any]) -> List[Chunk]:
        """Ultimate fallback for when all sophisticated methods fail"""
        
        chunks = []
        words = content.split()
        
        # Simple word-based chunking
        current_chunk_words = []
        target_words = self.max_chunk_size // 6  # Rough estimate: 6 chars per word
        
        for word in words:
            current_chunk_words.append(word)
            
            if len(current_chunk_words) >= target_words:
                chunk_text = " ".join(current_chunk_words)
                chunks.append(self._create_chunk(
                    chunk_text, "main", metadata, len(chunks)
                ))
                
                # Simple overlap
                overlap_size = min(self.overlap_size // 6, len(current_chunk_words) // 4)
                current_chunk_words = current_chunk_words[-overlap_size:] if overlap_size > 0 else []
        
        # Add final chunk
        if current_chunk_words:
            chunk_text = " ".join(current_chunk_words)
            chunks.append(self._create_chunk(
                chunk_text, "main", metadata, len(chunks)
            ))
        
        self.logger.warning("Used fallback chunking due to processing errors")
        return chunks 

    def _is_table_section(self, content: str) -> bool:
        """
        Detect if content is a table section that should be preserved intact.
        
        Works for all document types (Excel, DOCX, PDF) using the unified table format
        from table_formatter.py. All parsers now output tables in this standard format.
        
        Expected format:
        === TableName ===
        Col1 | Col2 | Col3
        ------------------
        val1 | val2 | val3
        
        Returns:
            True if content matches the table format pattern
        """
        lines = content.split('\n')
        if len(lines) < 4:  # Need at least: header line, column headers, separator, data row
            return False
        
        # Look for standardized table pattern (works for Excel, DOCX, PDF tables):
        # Line 1: === TableName ===
        # Line 2: Column headers with |
        # Line 3: Dash separator line
        # Line 4+: Data rows with |
        
        first_line = lines[0].strip()
        
        # Check for table header format
        if not (first_line.startswith('===') and first_line.endswith('===')):
            return False
        
        # Count lines with pipe separators (indicating tabular data)
        pipe_lines = [line for line in lines[1:] if '|' in line and line.strip()]
        
        # Check for dash separator line (table formatting)
        has_separator = any(line.strip().startswith('-') and len(line.strip()) > 10 for line in lines[1:4])
        
        # Must have:
        # - At least 3 lines with pipes (headers + separator + data)
        # - A dash separator line for table formatting
        # - Table section format
        return len(pipe_lines) >= 3 and has_separator
    
    # Backward compatibility alias
    _is_excel_sheet_section = _is_table_section
    
    def _split_section_into_segments(self, content: str) -> List[tuple]:
        """
        Split section content into alternating text and table segments.
        
        Returns:
            List of (segment_type, segment_content) tuples where segment_type is "text" or "table"
        """
        segments = []
        lines = content.split('\n')

        # Guardrail: extremely large line counts (common with spreadsheet dumps) can
        # make table detection expensive. In those cases, skip table detection and
        # treat the section as plain text to ensure forward progress.
        if len(lines) > 20000:
            self.logger.warning(
                "Segmenting guardrail: section has %d lines; skipping table detection",
                len(lines),
            )
            return [("text", content)]
        i = 0
        
        while i < len(lines):
            # Check if we're at a unified table block start
            if i < len(lines) and lines[i].strip().startswith('===') and lines[i].strip().endswith('==='):
                # Found unified table block - extract it
                table_lines, next_i = self._extract_unified_table_block(lines, i)
                if table_lines:
                    segments.append(("table", "\n".join(table_lines)))
                    i = next_i
                    continue
            
            # Check if we're at a markdown pipe table start
            markdown_table_lines, next_i = self._find_markdown_table_at(lines, i)
            if markdown_table_lines:
                # Normalize markdown table to unified format
                normalized = self._normalize_markdown_table(markdown_table_lines)
                if normalized:
                    segments.append(("table", normalized))
                    i = next_i
                    continue
            
            # Collect text lines until we hit a table or end
            text_lines = []
            while i < len(lines):
                # Check if next line starts a table
                if i < len(lines) and lines[i].strip().startswith('===') and lines[i].strip().endswith('==='):
                    break
                markdown_check, _ = self._find_markdown_table_at(lines, i)
                if markdown_check:
                    break
                
                text_lines.append(lines[i])
                i += 1
            
            if text_lines:
                text_content = "\n".join(text_lines).strip()
                if text_content:
                    segments.append(("text", text_content))
        
        return segments if segments else [("text", content)]
    
    def _extract_unified_table_block(self, lines: List[str], start_i: int) -> tuple:
        """
        Extract a unified table block starting at start_i.
        
        Returns:
            (table_lines, next_index) where table_lines is the complete table block
            and next_index is where to continue scanning
        """
        if start_i >= len(lines):
            return ([], start_i)
        
        # Must start with === ... ===
        if not (lines[start_i].strip().startswith('===') and lines[start_i].strip().endswith('===')):
            return ([], start_i)
        
        table_lines = [lines[start_i]]
        i = start_i + 1
        
        # Collect header row
        if i < len(lines) and '|' in lines[i]:
            table_lines.append(lines[i])
            i += 1
        
        # Collect separator line
        if i < len(lines) and lines[i].strip().startswith('-'):
            table_lines.append(lines[i])
            i += 1
        
        # Collect data rows (until we hit non-table content)
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # Stop if we hit another table block start
            if stripped.startswith('===') and stripped.endswith('==='):
                break
            
            # Stop if we hit a markdown table (different format)
            if stripped.startswith('|') and i + 1 < len(lines):
                next_stripped = lines[i + 1].strip()
                if next_stripped.startswith('|') or (next_stripped.startswith('-') and '|' in next_stripped):
                    # Could be markdown table - let that handler deal with it
                    break
            
            # Continue if this looks like a table row
            if '|' in line:
                table_lines.append(line)
                i += 1
            elif not stripped:  # Empty line - might be part of table
                table_lines.append(line)
                i += 1
            else:
                # Non-table content - stop
                break
        
        return (table_lines, i)
    
    def _find_markdown_table_at(self, lines: List[str], start_i: int) -> tuple:
        """
        Detect if a markdown pipe table starts at start_i.
        
        Returns:
            (table_lines, next_index) if found, ([], start_i) otherwise
        """
        if start_i >= len(lines):
            return ([], start_i)
        
        # Markdown table starts with a pipe-separated header row
        header_line = lines[start_i].strip()
        if not header_line.startswith('|') or header_line.count('|') < 2:
            return ([], start_i)
        
        # Next line should be separator (|---|---| or similar)
        if start_i + 1 >= len(lines):
            return ([], start_i)
        
        separator_line = lines[start_i + 1].strip()
        if not (separator_line.startswith('|') or separator_line.startswith('-')):
            return ([], start_i)
        
        # IMPORTANT: avoid false positives on "Excel-like" table dumps that include a
        # long dashed divider line but are not markdown pipe tables.
        # A real markdown pipe table separator must include pipes.
        if '|' not in separator_line:
            return ([], start_i)
        
        # Collect header and separator
        table_lines = [lines[start_i], lines[start_i + 1]]
        i = start_i + 2
        
        # Collect data rows
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # Stop if we hit another table block start
            if stripped.startswith('===') and stripped.endswith('==='):
                break
            
            # Continue if this looks like a markdown table row
            if stripped.startswith('|') and stripped.count('|') >= 2:
                table_lines.append(line)
                i += 1
            elif not stripped:  # Empty line might separate tables
                # Check if next non-empty line is a table row
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                if j < len(lines) and lines[j].strip().startswith('|'):
                    table_lines.append(line)  # Keep empty line
                    i += 1
                else:
                    break
            else:
                # Non-table content - stop
                break
        
        # Must have at least header + separator + 1 data row
        if len(table_lines) >= 3:
            return (table_lines, i)
        
        return ([], start_i)
    
    def _normalize_markdown_table(self, markdown_lines: List[str]) -> str:
        """
        Convert markdown pipe table to unified table block format.
        
        Args:
            markdown_lines: Lines of markdown table (header, separator, data rows)
            
        Returns:
            Unified table block string or empty string if conversion fails
        """
        if len(markdown_lines) < 3:
            return ""
        
        # Parse header row
        header_line = markdown_lines[0].strip()
        header_cells = [cell.strip() for cell in header_line.strip('|').split('|')]
        header_cells = [c for c in header_cells if c]  # Remove empty cells
        
        if not header_cells:
            return ""
        
        # Generate table name (use first few header cells)
        table_name = "_".join(header_cells[:2])[:30] if len(header_cells) >= 2 else "TABLE"
        table_name = re.sub(r'[^\w\s-]', '', table_name).strip().replace(' ', '_')[:30]
        if not table_name:
            table_name = "TABLE"
        
        # Build unified format
        parts = []
        parts.append(f"=== {table_name} ===")
        parts.append(" | ".join(header_cells))
        
        # Add separator
        separator_length = max(len(" | ".join(header_cells)), 10)
        parts.append("-" * separator_length)
        
        # Add data rows
        for line in markdown_lines[2:]:
            stripped = line.strip()
            if not stripped.startswith('|'):
                continue
            
            cells = [cell.strip() for cell in stripped.strip('|').split('|')]
            cells = [c for c in cells if c]  # Remove empty cells
            
            if cells:
                # Pad to match header width if needed
                while len(cells) < len(header_cells):
                    cells.append("")
                parts.append(" | ".join(cells[:len(header_cells)]))
        
        return "\n".join(parts)
    
    def _process_table_block(self, table_content: str, section_name: str,
                            metadata: Dict[str, Any], start_chunk_index: int) -> List[Chunk]:
        """
        Process a unified table block: preserve if small, split with header repetition if large.
        
        Args:
            table_content: Unified table block content
            section_name: Section name for metadata
            metadata: Chunk metadata
            start_chunk_index: Starting chunk index
            
        Returns:
            List of Chunk objects
        """
        # Ensure it's a unified table block
        if not self._is_table_section(table_content):
            # Try normalizing if it looks like markdown
            lines = table_content.split('\n')
            markdown_table, _ = self._find_markdown_table_at(lines, 0)
            if markdown_table:
                table_content = self._normalize_markdown_table(markdown_table)
                if not table_content:
                    # Fallback: treat as text
                    return []
            else:
                return []
        
        word_count = len(table_content.split())
        
        # Small table: preserve as single chunk
        if word_count <= self.excel_sheet_max_size:
            self.logger.info(f"Table block ({word_count} words) - preserving as single chunk")
            return [self._create_chunk(
                table_content.strip(),
                section_name,
                metadata,
                start_chunk_index
            )]
        
        # Large table: split with header repetition
        self.logger.info(f"Table block ({word_count} words) exceeds max size ({self.excel_sheet_max_size}) - splitting with header repetition")
        return self._split_large_table_block(table_content, section_name, metadata, start_chunk_index)
    
    def _split_large_table_block(self, table_content: str, section_name: str,
                                metadata: Dict[str, Any], start_chunk_index: int) -> List[Chunk]:
        """
        Split a large unified table block into multiple chunks with repeated headers.
        
        Each chunk will have:
        - === TABLE_NAME (part i/N) ===
        - Header row
        - Separator line
        - Subset of data rows
        
        Returns:
            List of Chunk objects
        """
        lines = table_content.split('\n')
        if len(lines) < 4:
            # Too small to split meaningfully
            return [self._create_chunk(table_content.strip(), section_name, metadata, start_chunk_index)]
        
        # Extract table name from first line
        first_line = lines[0].strip()
        if first_line.startswith('===') and first_line.endswith('==='):
            table_name = first_line.strip('= ').strip()
        else:
            table_name = "TABLE"
        
        # Extract header row and separator
        header_row = lines[1] if len(lines) > 1 and '|' in lines[1] else ""
        separator_line = lines[2] if len(lines) > 2 and lines[2].strip().startswith('-') else "-" * 50
        
        if not header_row:
            # Fallback: can't split without header
            return [self._create_chunk(table_content.strip(), section_name, metadata, start_chunk_index)]
        
        # Extract data rows (skip header and separator)
        data_rows = []
        for line in lines[3:]:
            if line.strip() and '|' in line:
                data_rows.append(line)
        
        if not data_rows:
            return [self._create_chunk(table_content.strip(), section_name, metadata, start_chunk_index)]
        
        # Calculate rows per chunk to stay under word limit
        # Estimate: header + separator + N rows should be <= excel_sheet_max_size words
        header_words = len(header_row.split())
        separator_words = len(separator_line.split())
        avg_row_words = sum(len(row.split()) for row in data_rows[:10]) / min(10, len(data_rows)) if data_rows else 5
        
        # Reserve words for header + separator + part marker
        reserved_words = header_words + separator_words + 10  # +10 for part marker
        available_words = self.excel_sheet_max_size - reserved_words
        
        if available_words <= 0:
            # Even header alone is too large - return as-is
            return [self._create_chunk(table_content.strip(), section_name, metadata, start_chunk_index)]
        
        rows_per_chunk = max(1, int(available_words / avg_row_words)) if avg_row_words > 0 else 10
        
        # Split into chunks
        chunks = []
        total_chunks = (len(data_rows) + rows_per_chunk - 1) // rows_per_chunk
        
        for chunk_idx in range(total_chunks):
            start_row = chunk_idx * rows_per_chunk
            end_row = min(start_row + rows_per_chunk, len(data_rows))
            chunk_rows = data_rows[start_row:end_row]
            
            # Build chunk content with part marker
            chunk_lines = [
                f"=== {table_name} (part {chunk_idx + 1}/{total_chunks}) ===",
                header_row,
                separator_line
            ]
            chunk_lines.extend(chunk_rows)
            
            chunk_text = "\n".join(chunk_lines)
            chunks.append(self._create_chunk(
                chunk_text,
                section_name,
                metadata,
                start_chunk_index + chunk_idx
            ))
        
        self.logger.info(f"Split table '{table_name}' into {len(chunks)} chunks with repeated headers")
        return chunks