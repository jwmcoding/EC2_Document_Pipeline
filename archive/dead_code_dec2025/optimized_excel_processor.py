"""
Optimized Excel Processor - Phase 2 Performance Enhancement
Fast Excel processing with intelligent row limits and optimized engines
"""

import pandas as pd
import io
import logging
from typing import Dict, Any, List, Optional
import time


class OptimizedExcelProcessor:
    """
    High-performance Excel processor with intelligent limits and optimization
    
    Features:
    - Dynamic row limits based on file size
    - Optimized pandas engines
    - Smart sheet sampling for very large files
    - Memory-efficient processing
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Performance thresholds
        self.size_thresholds = {
            'small': 1,    # < 1MB
            'medium': 10,  # 1-10MB  
            'large': 50    # > 10MB
        }
        
        # NO ROW LIMITS - Process all data
        self.row_limits = {
            'small': None,   # NO LIMITS
            'medium': None,  # NO LIMITS
            'large': None    # NO LIMITS
        }
        
        # NO SHEET LIMITS - Process all sheets
        self.sheet_limits = {
            'small': None,   # Process ALL sheets
            'medium': None,  # Process ALL sheets
            'large': None    # Process ALL sheets
        }
    
    def process_excel_optimized(self, content: bytes, extension: str, 
                               metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process Excel/CSV files with size-based optimization
        
        Args:
            content: File content as bytes
            extension: File extension (.xlsx, .xls, .csv)
            metadata: Document metadata
            
        Returns:
            Processing result with text and metadata
        """
        start_time = time.time()
        size_mb = len(content) / (1024 * 1024)
        
        # Process complete file - no limits
        self.logger.info(f"📊 Excel Processing: COMPLETE MODE "
                        f"(size: {size_mb:.2f}MB, no truncation limits)")
        
        try:
            if extension == '.csv':
                result = self._process_csv_optimized(content, metadata)
            else:
                result = self._process_excel_optimized(content, extension, metadata)
            
            processing_time = time.time() - start_time
            result['processing_time'] = processing_time
            result['size_category'] = 'complete'  # Always complete processing
            result['size_mb'] = size_mb
            
            self.logger.info(f"⚡ Excel processed in {processing_time:.2f}s "
                           f"(complete mode - no truncation)")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Excel processing failed: {e}")
            return self._create_error_result(str(e), metadata)
    
    def _categorize_file_size(self, size_mb: float) -> str:
        """Categorize file size for processing strategy"""
        if size_mb < self.size_thresholds['small']:
            return 'small'
        elif size_mb < self.size_thresholds['medium']:
            return 'medium'
        else:
            return 'large'
    
    def _process_csv_optimized(self, content: bytes, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Complete CSV processing - NO LIMITS"""
        try:
            # Use pandas with complete processing
            df = pd.read_csv(
                io.BytesIO(content),
                # NO row limit - process all data
                low_memory=False,  # Read in one pass for speed
                dtype=str,         # Avoid type inference overhead
                na_filter=False    # Don't convert to NaN for speed
            )
            
            text_parts = ["=== CSV Data ==="]
            
            if df.empty:
                text_parts.append("(Empty CSV file)")
            else:
                # Add headers
                headers = " | ".join(str(col) for col in df.columns)
                text_parts.append(headers)
                text_parts.append("-" * min(len(headers), 100))
                
                # Add data rows
                for idx, row in df.iterrows():
                    row_text = " | ".join(str(val) for val in row.values)
                    text_parts.append(row_text)
                
                # No truncation applied - all rows processed
            
            return {
                'text': '\n'.join(text_parts),
                'metadata': {
                    **metadata,
                    'parser': 'optimized_csv',
                    'rows_processed': len(df),
                    'columns': len(df.columns) if not df.empty else 0,
                    'truncated': False  # No truncation applied
                },
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"CSV processing error: {e}")
            return self._create_error_result(f"CSV processing error: {str(e)}", metadata)
    
    def _process_excel_optimized(self, content: bytes, extension: str,
                               metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Complete Excel processing - NO LIMITS"""
        try:
            # Choose optimal engine
            engine = 'openpyxl' if extension == '.xlsx' else 'xlrd'
            
            # Read Excel file completely
            excel_file = pd.ExcelFile(io.BytesIO(content), engine=engine)
            sheet_names = excel_file.sheet_names
            
            # Process ALL sheets - no limits
            self.logger.info(f"📄 Processing ALL {len(sheet_names)} sheets")
            
            text_parts = []
            sheets_processed = 0
            total_rows_processed = 0
            
            for sheet_name in sheet_names:
                try:
                    # Read complete sheet - NO LIMITS
                    df = excel_file.parse(
                        sheet_name,
                        # NO row limit - process all data
                        dtype=str,         # Avoid type inference
                        na_filter=False    # Skip NaN conversion
                    )
                    
                    text_parts.append(f"=== {sheet_name} ===")
                    
                    if df.empty:
                        text_parts.append("(Empty sheet)")
                    else:
                        # Add headers - NO LIMITS
                        headers = " | ".join(str(col) for col in df.columns)  # NO column width limit
                        text_parts.append(headers)
                        text_parts.append("-" * min(len(headers), 100))
                        
                        # Add ALL data rows - NO LIMITS
                        for idx in range(len(df)):
                            row = df.iloc[idx]
                            row_text = " | ".join(str(val) for val in row.values)  # NO cell width limit
                            text_parts.append(row_text)
                        
                        # No truncation applied - all rows processed
                    
                    text_parts.append("")  # Empty line between sheets
                    sheets_processed += 1
                    total_rows_processed += len(df)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing sheet {sheet_name}: {e}")
                    text_parts.append(f"=== {sheet_name} ===")
                    text_parts.append(f"Error reading sheet: {str(e)}")
                    text_parts.append("")
            
            # Add summary for completed processing
            text_parts.append(f"=== PROCESSING SUMMARY ===")
            text_parts.append(f"Processed {sheets_processed}/{len(excel_file.sheet_names)} sheets")
            text_parts.append(f"Total rows processed: {total_rows_processed}")
            text_parts.append("(Complete file processed - no truncation)")
            
            return {
                'text': '\n'.join(text_parts),
                'metadata': {
                    **metadata,
                    'parser': 'optimized_excel',
                    'sheets_total': len(excel_file.sheet_names),
                    'sheets_processed': sheets_processed,
                    'rows_processed': total_rows_processed,
                    'truncated_sheets': False,  # No sheet truncation applied
                    'engine_used': engine
                },
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"Excel processing error: {e}")
            return self._create_error_result(f"Excel processing error: {str(e)}", metadata)
    
    def _create_error_result(self, error_msg: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Create standardized error result"""
        return {
            'text': f"Error processing Excel file: {error_msg}",
            'metadata': {
                **metadata,
                'parser': 'optimized_excel_error',
                'error': error_msg
            },
            'success': False,
            'error': error_msg
        }
    
    def get_optimization_info(self) -> Dict[str, Any]:
        """Get information about optimization settings"""
        return {
            'size_thresholds_mb': self.size_thresholds,
            'row_limits': self.row_limits,
            'sheet_limits': self.sheet_limits,
            'features': [
                'Dynamic row limits based on file size',
                'Optimized pandas engines (openpyxl/xlrd)',
                'Smart sheet sampling for large files',
                'Memory-efficient string processing',
                'Cell content truncation for display'
            ]
        } 