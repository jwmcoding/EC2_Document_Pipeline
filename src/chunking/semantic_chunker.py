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
        
        # Check if this is a table section (Excel, DOCX, or PDF table) and keep it whole
        if self._is_table_section(section_content):
            # Check if table size is within acceptable limits
            word_count = len(section_content.split())
            if word_count <= self.excel_sheet_max_size:
                self.logger.info(f"Detected table section '{section_name}' ({word_count} words) - preserving as single chunk")
                return [self._create_chunk(
                    section_content.strip(), 
                    section_name, 
                    metadata, 
                    start_chunk_index
                )]
            else:
                self.logger.warning(f"Table section '{section_name}' ({word_count} words) exceeds max size ({self.excel_sheet_max_size}) - using normal chunking")
                # Fall through to normal chunking for very large tables
        
        # Existing logic for other document types
        # Split into sentences using business-aware rules
        sentences = self._split_into_sentences(section_content)
        
        if not sentences:
            return []
        
        # Group sentences into chunks based on size constraints and business logic
        chunks = self._create_business_chunks(
            sentences, section_name, metadata, start_chunk_index
        )
        
        return chunks
    
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