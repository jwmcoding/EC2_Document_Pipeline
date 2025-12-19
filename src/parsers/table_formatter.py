"""
Unified Table Formatter for Document Parsing

Provides consistent table formatting across all document types (PDF, DOCX, Excel)
to ensure tables are detected and preserved as single chunks during semantic chunking.

The format matches the chunker detection pattern in semantic_chunker._is_table_section():
- Header line with === delimiters
- Pipe-separated column headers
- Dash separator line (at least 10 characters)
- Pipe-separated data rows
"""

from typing import List, Optional
import logging


logger = logging.getLogger(__name__)


def deduplicate_merged_cells(row_cells: List[str]) -> List[str]:
    """
    Remove duplicate values from adjacent merged cells.
    
    DOCX and PDF tables can have merged cells that appear as duplicated
    values in adjacent positions. This function replaces duplicates with
    empty strings to maintain column structure.
    
    Args:
        row_cells: List of cell values from a table row
        
    Returns:
        List with duplicate adjacent values replaced by empty strings
        
    Example:
        Input:  ["Total", "Total", "Total", "$500"]  (merged cell spanning 3 cols)
        Output: ["Total", "", "", "$500"]
    """
    if not row_cells:
        return []
    
    result = []
    prev_cell = None
    
    for cell in row_cells:
        cell_text = str(cell).strip() if cell else ""
        
        # If same as previous cell and not empty, it's likely a merged cell
        if cell_text == prev_cell and cell_text:
            result.append("")  # Empty for merged continuation
        else:
            result.append(cell_text)
        
        prev_cell = cell_text
    
    return result


def format_table_for_chunking(
    table_data: List[List],
    table_name: str,
    page_num: Optional[int] = None,
    deduplicate_merged: bool = True
) -> str:
    """
    Format table data to match chunker detection pattern.
    
    Creates a standardized text representation that the SemanticChunker
    will detect and preserve as a single chunk (up to 2000 words).
    
    Args:
        table_data: 2D list of table cells. First row is treated as headers.
        table_name: Name/identifier for the table (e.g., "Pricing", "TABLE_1")
        page_num: Optional page number (for PDFs) - included in table header
        deduplicate_merged: Whether to deduplicate adjacent identical cells
        
    Returns:
        Formatted table text matching chunker detection pattern:
        === TableName ===
        Col1 | Col2 | Col3
        ------------------
        val1 | val2 | val3
        
    Example:
        >>> data = [["Name", "Price"], ["Widget", "$10"], ["Gadget", "$20"]]
        >>> print(format_table_for_chunking(data, "Pricing"))
        === Pricing ===
        Name | Price
        ------------
        Widget | $10
        Gadget | $20
    """
    if not table_data:
        logger.warning(f"Empty table data for '{table_name}' - skipping")
        return ""
    
    # Filter out completely empty rows
    non_empty_rows = []
    for row in table_data:
        if row and any(cell for cell in row if cell):
            non_empty_rows.append(row)
    
    if not non_empty_rows:
        logger.warning(f"Table '{table_name}' has no non-empty rows - skipping")
        return ""
    
    text_parts = []
    
    # Build table header with page number if provided
    if page_num is not None:
        header = f"=== {table_name} (Page {page_num}) ==="
    else:
        header = f"=== {table_name} ==="
    text_parts.append(header)
    
    # Process first row as column headers
    header_row = non_empty_rows[0]
    if deduplicate_merged:
        header_row = deduplicate_merged_cells(header_row)
    
    header_cells = [str(cell).strip() if cell else "" for cell in header_row]
    header_line = " | ".join(header_cells)
    text_parts.append(header_line)
    
    # Add dash separator (required for chunker detection)
    # Length should be reasonable but at least 10 characters
    separator_length = min(len(header_line), 100)
    separator_length = max(separator_length, 10)
    text_parts.append("-" * separator_length)
    
    # Add data rows
    for row in non_empty_rows[1:]:
        if deduplicate_merged:
            row = deduplicate_merged_cells(row)
        
        row_cells = [str(cell).strip() if cell else "" for cell in row]
        row_line = " | ".join(row_cells)
        text_parts.append(row_line)
    
    return "\n".join(text_parts)


def format_tables_inline(
    page_text: str,
    page_tables: List[List[List]],
    page_num: int,
    table_name_prefix: str = "TABLE"
) -> str:
    """
    Format page text with tables inserted inline.
    
    Instead of appending all tables at the end (losing context),
    this inserts each table right after the page text where it appeared.
    
    Args:
        page_text: The extracted text content from the page
        page_tables: List of tables extracted from the page (each is 2D list)
        page_num: Page number for table naming
        table_name_prefix: Prefix for table names (default: "TABLE")
        
    Returns:
        Combined text with tables inserted after page content
    """
    parts = []
    
    # Add page text first
    if page_text and page_text.strip():
        parts.append(page_text.strip())
    
    # Add each table inline
    for idx, table_data in enumerate(page_tables):
        table_name = f"{table_name_prefix}_P{page_num}_{idx + 1}"
        formatted = format_table_for_chunking(
            table_data,
            table_name,
            page_num=page_num
        )
        if formatted:
            parts.append("")  # Blank line before table
            parts.append(formatted)
    
    return "\n".join(parts)


def convert_docx_table_to_list(table) -> List[List[str]]:
    """
    Convert a python-docx Table object to a 2D list.
    
    Handles the DOCX table structure and extracts cell text.
    
    Args:
        table: A python-docx Table object
        
    Returns:
        2D list of cell text values
    """
    rows = []
    for row in table.rows:
        cells = []
        for cell in row.cells:
            cell_text = cell.text.strip() if cell.text else ""
            cells.append(cell_text)
        rows.append(cells)
    return rows


def estimate_table_word_count(table_data: List[List]) -> int:
    """
    Estimate the word count of a table.
    
    Used to check if table will exceed the chunker's word limit (default 2000).
    
    Args:
        table_data: 2D list of table cells
        
    Returns:
        Estimated total word count
    """
    word_count = 0
    for row in table_data:
        for cell in row:
            if cell:
                word_count += len(str(cell).split())
    return word_count



















