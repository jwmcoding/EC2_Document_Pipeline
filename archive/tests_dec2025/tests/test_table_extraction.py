"""
Unit Tests for Table Extraction and Formatting

Tests the unified table formatter and chunker detection logic to ensure
tables are properly formatted and preserved as single chunks across all
document types (PDF, DOCX, Excel).

Run with: pytest tests/test_table_extraction.py -v
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parsers.table_formatter import (
    format_table_for_chunking,
    deduplicate_merged_cells,
    convert_docx_table_to_list,
    estimate_table_word_count,
    format_tables_inline
)
from src.chunking.semantic_chunker import SemanticChunker


class TestDeduplicateMergedCells:
    """Test merged cell deduplication logic"""
    
    def test_normal_row_unchanged(self):
        """Normal row without merged cells should be unchanged"""
        row = ["A", "B", "C", "D"]
        result = deduplicate_merged_cells(row)
        assert result == ["A", "B", "C", "D"]
    
    def test_merged_cells_deduplicated(self):
        """Merged cells (adjacent duplicates) should be replaced with empty strings"""
        row = ["Total", "Total", "Total", "$500"]
        result = deduplicate_merged_cells(row)
        assert result == ["Total", "", "", "$500"]
    
    def test_partial_merge(self):
        """Partial merge at start of row"""
        row = ["Header", "Header", "Data1", "Data2"]
        result = deduplicate_merged_cells(row)
        assert result == ["Header", "", "Data1", "Data2"]
    
    def test_multiple_merges(self):
        """Multiple separate merged regions"""
        row = ["A", "A", "B", "C", "C", "C"]
        result = deduplicate_merged_cells(row)
        assert result == ["A", "", "B", "C", "", ""]
    
    def test_empty_row(self):
        """Empty row should return empty list"""
        assert deduplicate_merged_cells([]) == []
    
    def test_single_cell(self):
        """Single cell row"""
        assert deduplicate_merged_cells(["Only"]) == ["Only"]
    
    def test_all_same(self):
        """Row with all identical values (fully merged)"""
        row = ["Merged", "Merged", "Merged"]
        result = deduplicate_merged_cells(row)
        assert result == ["Merged", "", ""]
    
    def test_empty_cells_not_deduplicated(self):
        """Empty cells should not be deduplicated (they're legitimately empty)"""
        row = ["A", "", "", "B"]
        result = deduplicate_merged_cells(row)
        assert result == ["A", "", "", "B"]
    
    def test_none_values_handled(self):
        """None values should be converted to empty string"""
        row = ["A", None, "B"]
        result = deduplicate_merged_cells(row)
        assert result == ["A", "", "B"]


class TestFormatTableForChunking:
    """Test the unified table formatter"""
    
    def test_basic_table(self):
        """Basic table formatting"""
        data = [
            ["Name", "Price", "Qty"],
            ["Widget", "$10.00", "5"],
            ["Gadget", "$25.00", "3"]
        ]
        result = format_table_for_chunking(data, "Products")
        
        assert "=== Products ===" in result
        assert "Name | Price | Qty" in result
        assert "Widget | $10.00 | 5" in result
        assert "Gadget | $25.00 | 3" in result
        # Check for dash separator
        assert "---" in result
    
    def test_table_with_page_number(self):
        """Table with page number included in header"""
        data = [["Col1", "Col2"], ["Val1", "Val2"]]
        result = format_table_for_chunking(data, "TABLE_1", page_num=5)
        
        assert "=== TABLE_1 (Page 5) ===" in result
    
    def test_empty_table_returns_empty(self):
        """Empty table should return empty string"""
        result = format_table_for_chunking([], "Empty")
        assert result == ""
    
    def test_table_with_empty_rows_filtered(self):
        """Rows with all empty cells should be filtered out"""
        data = [
            ["Header1", "Header2"],
            ["", ""],  # Empty row
            ["Value1", "Value2"]
        ]
        result = format_table_for_chunking(data, "Test")
        
        assert "Header1 | Header2" in result
        assert "Value1 | Value2" in result
        # Should not have empty row represented
        lines = [l for l in result.split('\n') if l.strip() and not l.startswith('-')]
        assert len(lines) == 3  # Header line, header row, data row
    
    def test_merged_cell_deduplication(self):
        """Merged cells should be deduplicated by default"""
        data = [
            ["Header1", "Header2", "Header3"],
            ["Merged", "Merged", "Value"]  # First two cells merged
        ]
        result = format_table_for_chunking(data, "Test", deduplicate_merged=True)
        
        assert "Merged |  | Value" in result or "Merged | | Value" in result
    
    def test_dash_separator_present(self):
        """Dash separator must be present for chunker detection"""
        data = [["A", "B"], ["1", "2"]]
        result = format_table_for_chunking(data, "Test")
        
        lines = result.split('\n')
        separator_found = any(line.strip().startswith('-') for line in lines)
        assert separator_found, "Dash separator required for chunker detection"
    
    def test_separator_length(self):
        """Separator should be at least 10 characters"""
        data = [["A", "B"], ["1", "2"]]
        result = format_table_for_chunking(data, "Test")
        
        lines = result.split('\n')
        separator_line = next(l for l in lines if l.strip().startswith('-'))
        assert len(separator_line.strip()) >= 10
    
    def test_special_characters_preserved(self):
        """Special characters in cells should be preserved"""
        data = [
            ["Term", "Value"],
            ["Price", "$1,234.56"],
            ["Discount", "15.5%"],
            ["Note", "See Sec. 3(a)"]
        ]
        result = format_table_for_chunking(data, "Test")
        
        assert "$1,234.56" in result
        assert "15.5%" in result
        assert "Sec. 3(a)" in result


class TestEstimateTableWordCount:
    """Test word count estimation"""
    
    def test_basic_count(self):
        """Basic word count"""
        data = [
            ["Hello World", "Test"],
            ["One Two Three", "Four"]
        ]
        count = estimate_table_word_count(data)
        assert count == 7  # Hello, World, Test, One, Two, Three, Four
    
    def test_empty_table(self):
        """Empty table should have 0 words"""
        assert estimate_table_word_count([]) == 0
    
    def test_empty_cells(self):
        """Empty cells should not add to count"""
        data = [["Word", "", ""], ["", "Another", ""]]
        count = estimate_table_word_count(data)
        assert count == 2  # Word, Another


class TestChunkerTableDetection:
    """Test chunker's table section detection"""
    
    @pytest.fixture
    def chunker(self):
        return SemanticChunker()
    
    def test_valid_table_detected(self, chunker):
        """Properly formatted table should be detected"""
        table_content = """=== Pricing ===
Item | Price | Qty
------------------
Widget | $10.00 | 5
Gadget | $25.00 | 3"""
        
        assert chunker._is_table_section(table_content) is True
    
    def test_table_with_page_number_detected(self, chunker):
        """Table with page number in header should be detected"""
        table_content = """=== TABLE_P3_1 (Page 3) ===
Col1 | Col2 | Col3
-----------------------
A | B | C
D | E | F"""
        
        assert chunker._is_table_section(table_content) is True
    
    def test_missing_separator_not_detected(self, chunker):
        """Table without dash separator should NOT be detected"""
        table_content = """=== Pricing ===
Item | Price | Qty
Widget | $10.00 | 5
Gadget | $25.00 | 3"""
        
        assert chunker._is_table_section(table_content) is False
    
    def test_missing_header_not_detected(self, chunker):
        """Content without === header should NOT be detected"""
        table_content = """Pricing Table
Item | Price | Qty
------------------
Widget | $10.00 | 5"""
        
        assert chunker._is_table_section(table_content) is False
    
    def test_insufficient_rows_not_detected(self, chunker):
        """Table with fewer than 4 lines should NOT be detected"""
        table_content = """=== Pricing ===
Item | Price
----------"""
        
        assert chunker._is_table_section(table_content) is False
    
    def test_no_pipe_separators_not_detected(self, chunker):
        """Content without pipe separators should NOT be detected"""
        table_content = """=== Text Section ===
This is just text
-----------------
Not a table at all"""
        
        assert chunker._is_table_section(table_content) is False
    
    def test_backward_compatibility_alias(self, chunker):
        """_is_excel_sheet_section should still work as alias"""
        table_content = """=== Sheet1 ===
A | B | C
----------
1 | 2 | 3
4 | 5 | 6"""
        
        # Both methods should return same result
        assert chunker._is_table_section(table_content) == chunker._is_excel_sheet_section(table_content)


class TestTablePreservationInChunking:
    """Test that tables are preserved as single chunks"""
    
    @pytest.fixture
    def chunker(self):
        return SemanticChunker(excel_sheet_max_size=2000)
    
    def test_small_table_preserved(self, chunker):
        """Small table should be preserved as single chunk"""
        table_content = """=== Small Table ===
Col1 | Col2
-----------
A | B
C | D"""
        
        chunks = chunker.chunk_document(table_content, {"doc_type": "test"})
        
        # Should be exactly 1 chunk
        assert len(chunks) == 1
        # Chunk should contain the full table
        assert "Col1 | Col2" in chunks[0].text
        assert "A | B" in chunks[0].text
    
    def test_large_table_logs_warning(self, chunker):
        """Table exceeding max size should log warning and fall through to normal chunking"""
        # Create a table with many rows (will exceed word limit)
        rows = [["Header1", "Header2", "Header3"]]
        for i in range(500):  # 500 rows should exceed limit
            rows.append([f"Data row {i} column 1 with extra words", 
                        f"Data row {i} column 2 with extra words",
                        f"Data row {i} column 3 with extra words"])
        
        # Format as table
        table = format_table_for_chunking(rows, "LargeTable")
        word_count = len(table.split())
        
        # Set low limit to trigger warning
        chunker_small = SemanticChunker(excel_sheet_max_size=100)
        
        # Table should be detected but exceed limit
        assert chunker_small._is_table_section(table) is True
        assert word_count > 100  # Exceeds the small limit
        
        # Chunking should still work (falls through to normal chunking)
        chunks = chunker_small.chunk_document(table, {"doc_type": "test"})
        assert len(chunks) >= 1  # At least one chunk produced


class TestFormatTablesInline:
    """Test inline table formatting"""
    
    def test_single_table_inline(self):
        """Single table inserted after page text"""
        page_text = "This is the page content."
        tables = [[["A", "B"], ["1", "2"]]]
        
        result = format_tables_inline(page_text, tables, page_num=1)
        
        # Page text should come first
        assert result.startswith("This is the page content.")
        # Table should follow
        assert "=== TABLE_P1_1 (Page 1) ===" in result
        assert "A | B" in result
    
    def test_multiple_tables_inline(self):
        """Multiple tables on same page"""
        page_text = "Page content here."
        tables = [
            [["Col1", "Col2"], ["A", "B"]],
            [["X", "Y"], ["1", "2"]]
        ]
        
        result = format_tables_inline(page_text, tables, page_num=3)
        
        assert "TABLE_P3_1" in result
        assert "TABLE_P3_2" in result
    
    def test_empty_page_text(self):
        """Tables still formatted even with empty page text"""
        tables = [[["A", "B"], ["1", "2"]]]
        result = format_tables_inline("", tables, page_num=1)
        
        assert "TABLE_P1_1" in result


class TestIntegrationPDFFormat:
    """Integration tests for PDF table format"""
    
    def test_pdf_table_format_matches_chunker(self):
        """PDF tables formatted correctly for chunker detection"""
        # Simulate what PDF parser produces
        pdf_table_data = [
            ["Payment", "Amount", "Due Date"],
            ["Initial", "$50,000", "Jan 1, 2025"],
            ["Monthly", "$10,000", "1st of month"]
        ]
        
        formatted = format_table_for_chunking(
            pdf_table_data,
            "TABLE_P5_1",
            page_num=5
        )
        
        # Verify chunker will detect it
        chunker = SemanticChunker()
        assert chunker._is_table_section(formatted) is True


class TestIntegrationDOCXFormat:
    """Integration tests for DOCX table format"""
    
    def test_docx_table_format_matches_chunker(self):
        """DOCX tables formatted correctly for chunker detection"""
        # Simulate DOCX table data (after convert_docx_table_to_list)
        docx_table_data = [
            ["Deliverable", "Timeline", "Responsible"],
            ["Requirements Doc", "Week 1", "Analyst"],
            ["Design Doc", "Week 2-3", "Architect"]
        ]
        
        formatted = format_table_for_chunking(
            docx_table_data,
            "TABLE_1"
        )
        
        # Verify chunker will detect it
        chunker = SemanticChunker()
        assert chunker._is_table_section(formatted) is True


class TestMarkdownTableNormalization:
    """Test markdown pipe table detection and normalization to unified format"""
    
    @pytest.fixture
    def chunker(self):
        return SemanticChunker()
    
    def test_markdown_table_detected(self, chunker):
        """Markdown pipe table should be detected"""
        markdown_table = """| Col1 | Col2 | Col3 |
|---|---|---|
| A | B | C |
| D | E | F |"""
        
        lines = markdown_table.split('\n')
        table_lines, next_i = chunker._find_markdown_table_at(lines, 0)
        
        assert len(table_lines) >= 3  # Header + separator + at least 1 data row
        assert table_lines[0].strip().startswith('|')
    
    def test_markdown_table_normalized(self, chunker):
        """Markdown table should normalize to unified format"""
        markdown_table = """| Product | Price | Qty |
|---|---|---|
| Widget | $10 | 5 |
| Gadget | $25 | 3 |"""
        
        lines = markdown_table.split('\n')
        table_lines, _ = chunker._find_markdown_table_at(lines, 0)
        normalized = chunker._normalize_markdown_table(table_lines)
        
        assert normalized.startswith("===")
        assert normalized.endswith("===") or "===" in normalized
        assert "Product | Price | Qty" in normalized
        assert "Widget | $10 | 5" in normalized
        assert "---" in normalized or "---" in normalized.split('\n')[2]
    
    def test_markdown_table_with_empty_cells(self, chunker):
        """Markdown table with empty cells should normalize correctly"""
        markdown_table = """| A | B | C |
|---|---|---|
| 1 | | 3 |
| | 2 | |"""
        
        lines = markdown_table.split('\n')
        table_lines, _ = chunker._find_markdown_table_at(lines, 0)
        normalized = chunker._normalize_markdown_table(table_lines)
        
        assert normalized != ""
        assert "A | B | C" in normalized
    
    def test_markdown_table_not_at_start(self, chunker):
        """Markdown table detection should work when not at line 0"""
        content = """Some text here.
More text.

| Header | Value |
|---|---|
| Data | 123 |"""
        
        lines = content.split('\n')
        table_lines, next_i = chunker._find_markdown_table_at(lines, 3)
        
        assert len(table_lines) >= 3
    
    def test_invalid_markdown_not_detected(self, chunker):
        """Invalid markdown should not be detected as table"""
        invalid = """| Just one pipe |
Not a table"""
        
        lines = invalid.split('\n')
        table_lines, _ = chunker._find_markdown_table_at(lines, 0)
        
        assert len(table_lines) == 0


class TestOversizedTableSplitting:
    """Test splitting of oversized tables with header repetition"""
    
    @pytest.fixture
    def chunker(self):
        # Set small limit to force splitting
        return SemanticChunker(excel_sheet_max_size=50)
    
    def test_small_table_not_split(self, chunker):
        """Small table should remain single chunk"""
        table_content = """=== Small Table ===
Col1 | Col2
-----------
A | B
C | D"""
        
        chunks = chunker.chunk_document(table_content, {"doc_type": "test"})
        
        assert len(chunks) == 1
        assert "Small Table" in chunks[0].text
        assert "(part" not in chunks[0].text  # No part marker
    
    def test_large_table_split_with_headers(self, chunker):
        """Large table should be split with repeated headers"""
        # Create table with many rows
        rows = ["=== Large Table ===", "Col1 | Col2 | Col3", "-" * 30]
        for i in range(100):
            rows.append(f"Row {i} col1 with many words | Row {i} col2 with many words | Row {i} col3 with many words")
        
        table_content = "\n".join(rows)
        word_count = len(table_content.split())
        
        # Should exceed small limit
        assert word_count > 50
        
        chunks = chunker.chunk_document(table_content, {"doc_type": "test"})
        
        # Should be split into multiple chunks
        assert len(chunks) > 1
        
        # Each chunk should have header
        for chunk in chunks:
            assert "Col1 | Col2 | Col3" in chunk.text
            assert "(part" in chunk.text  # Part marker present
        
        # Verify part numbering
        part_numbers = []
        for chunk in chunks:
            import re
            match = re.search(r'\(part (\d+)/(\d+)\)', chunk.text)
            if match:
                part_numbers.append((int(match.group(1)), int(match.group(2))))
        
        assert len(part_numbers) == len(chunks)
        # Verify sequential numbering
        for i, (part_num, total) in enumerate(part_numbers):
            assert part_num == i + 1
            assert total == len(chunks)
    
    def test_split_chunks_have_separator(self, chunker):
        """Each split chunk should have separator line"""
        rows = ["=== Test Table ===", "A | B", "-" * 20]
        for i in range(50):
            rows.append(f"Data {i} | Value {i}")
        
        table_content = "\n".join(rows)
        chunks = chunker.chunk_document(table_content, {"doc_type": "test"})
        
        for chunk in chunks:
            # Should have separator (dash line)
            lines = chunk.text.split('\n')
            has_separator = any(line.strip().startswith('-') for line in lines)
            assert has_separator, f"Chunk missing separator: {chunk.text[:100]}"
    
    def test_split_chunks_sequential_indices(self, chunker):
        """Split chunks should have sequential chunk indices"""
        rows = ["=== Sequential Test ===", "Col1 | Col2", "-" * 20]
        for i in range(50):
            rows.append(f"Row {i} | Data {i}")
        
        table_content = "\n".join(rows)
        chunks = chunker.chunk_document(table_content, {"doc_type": "test"})
        
        # Verify sequential indices
        for i, chunk in enumerate(chunks):
            assert chunk.metadata["chunk_index"] == i


class TestMixedContentSegmentation:
    """Test segmentation of mixed text and table content"""
    
    @pytest.fixture
    def chunker(self):
        return SemanticChunker(excel_sheet_max_size=2000)
    
    def test_text_table_text_segmentation(self, chunker):
        """Content with text-table-text should be segmented correctly"""
        content = """This is introductory text.
Some more context here.

=== Pricing Table ===
Item | Price
----------
Widget | $10
Gadget | $25

This is concluding text.
Final paragraph."""
        
        segments = chunker._split_section_into_segments(content)
        
        # Should have 3 segments: text, table, text
        assert len(segments) >= 3
        
        # First segment should be text
        assert segments[0][0] == "text"
        assert "introductory text" in segments[0][1]
        
        # Middle segment should be table
        table_segment = next((s for s in segments if s[0] == "table"), None)
        assert table_segment is not None
        assert "Pricing Table" in table_segment[1]
        
        # Last segment should be text
        assert segments[-1][0] == "text"
        assert "concluding text" in segments[-1][1]
    
    def test_markdown_table_in_mixed_content(self, chunker):
        """Markdown table in mixed content should be normalized"""
        content = """Introduction paragraph.

| Product | Price |
|---|---|
| Widget | $10 |
| Gadget | $25 |

Conclusion paragraph."""
        
        segments = chunker._split_section_into_segments(content)
        
        # Should have text-table-text segments
        assert len(segments) >= 3
        
        # Find table segment
        table_segment = next((s for s in segments if s[0] == "table"), None)
        assert table_segment is not None
        
        # Table should be normalized to unified format
        assert table_segment[1].startswith("===")
        assert "Product | Price" in table_segment[1]
    
    def test_multiple_tables_in_content(self, chunker):
        """Multiple tables should be segmented separately"""
        content = """Intro text.

=== Table 1 ===
A | B
----
1 | 2

Middle text.

=== Table 2 ===
X | Y
----
3 | 4

Conclusion."""
        
        segments = chunker._split_section_into_segments(content)
        
        # Should have multiple segments
        assert len(segments) >= 5  # text, table1, text, table2, text
        
        # Count table segments
        table_segments = [s for s in segments if s[0] == "table"]
        assert len(table_segments) == 2
        
        # Verify both tables present
        table_texts = [s[1] for s in table_segments]
        assert any("Table 1" in t for t in table_texts)
        assert any("Table 2" in t for t in table_texts)
    
    def test_mixed_content_chunking(self, chunker):
        """Mixed content should chunk correctly with tables preserved"""
        content = """Introduction paragraph with some text.

=== Small Table ===
Col1 | Col2
-----------
A | B
C | D

Conclusion paragraph."""
        
        chunks = chunker.chunk_document(content, {"doc_type": "test"})
        
        # Should have multiple chunks (text + table + text)
        assert len(chunks) >= 2
        
        # Find table chunk
        table_chunk = next((c for c in chunks if "Small Table" in c.text), None)
        assert table_chunk is not None
        
        # Table chunk should be preserved intact
        assert "Col1 | Col2" in table_chunk.text
        assert "A | B" in table_chunk.text


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v"])

