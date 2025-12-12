"""
Data Integrity Settings for Document Processing
Controls truncation limits and ensures business-critical data preservation
"""

import os
from typing import Dict, Any


class DataIntegritySettings:
    """
    Configuration for data integrity and truncation control
    
    IMPORTANT: These settings balance performance vs completeness.
    For business-critical documents, consider using CONSERVATIVE or COMPLETE modes.
    """
    
    # Data integrity modes
    MODES = {
        'AGGRESSIVE': {
            'description': 'Maximum performance, significant truncation',
            'use_case': 'Quick processing, non-critical documents'
        },
        'BALANCED': {
            'description': 'Good performance with reasonable completeness', 
            'use_case': 'Most business documents (default)'
        },
        'CONSERVATIVE': {
            'description': 'Minimal truncation, slower processing',
            'use_case': 'Important contracts, financial documents'
        },
        'COMPLETE': {
            'description': 'No truncation, full data preservation',
            'use_case': 'Legal documents, comprehensive analysis'
        }
    }
    
    def __init__(self, mode: str = None):
        """Initialize with specified data integrity mode"""
        self.mode = mode or os.getenv("DATA_INTEGRITY_MODE", "BALANCED")
        self.settings = self._get_mode_settings(self.mode)
    
    def _get_mode_settings(self, mode: str) -> Dict[str, Any]:
        """Get settings for specified mode"""
        
        if mode == "AGGRESSIVE":
            return {
                # PDF Settings
                'pdf_small_threshold_mb': 0.5,
                'pdf_medium_threshold_mb': 5.0,
                'pdf_large_page_limit': 15,
                'pdf_enable_sampling': True,
                
                # Excel Settings
                'excel_small_row_limit': 2000,
                'excel_medium_row_limit': 1000,
                'excel_large_row_limit': 500,
                'excel_sheet_limit_medium': 5,
                'excel_sheet_limit_large': 3,
                'excel_cell_char_limit': 50,
                
                # DOCX Settings
                'docx_small_paragraph_limit': 2000,
                'docx_medium_paragraph_limit': 1000,
                'docx_large_paragraph_limit': 500,
                'docx_table_limit_small': 10,
                'docx_table_limit_medium': 5,
                'docx_table_limit_large': 3,
                'docx_paragraph_char_limit': 1000,
                'docx_cell_char_limit': 100
            }
        
        elif mode == "BALANCED":
            return {
                # PDF Settings
                'pdf_small_threshold_mb': 1.0,
                'pdf_medium_threshold_mb': 10.0,
                'pdf_large_page_limit': 30,
                'pdf_enable_sampling': True,
                
                # Excel Settings
                'excel_small_row_limit': 5000,
                'excel_medium_row_limit': 2000,
                'excel_large_row_limit': 1000,
                'excel_sheet_limit_medium': 10,
                'excel_sheet_limit_large': 5,
                'excel_cell_char_limit': 200,
                
                # DOCX Settings
                'docx_small_paragraph_limit': 5000,
                'docx_medium_paragraph_limit': 2000,
                'docx_large_paragraph_limit': 1000,
                'docx_table_limit_small': 20,
                'docx_table_limit_medium': 10,
                'docx_table_limit_large': 5,
                'docx_paragraph_char_limit': 2000,
                'docx_cell_char_limit': 200
            }
        
        elif mode == "CONSERVATIVE":
            return {
                # PDF Settings
                'pdf_small_threshold_mb': 2.0,
                'pdf_medium_threshold_mb': 20.0,
                'pdf_large_page_limit': 100,  # Much higher limit
                'pdf_enable_sampling': False,  # Process all pages
                
                # Excel Settings
                'excel_small_row_limit': 20000,
                'excel_medium_row_limit': 10000,
                'excel_large_row_limit': 5000,
                'excel_sheet_limit_medium': 20,
                'excel_sheet_limit_large': 15,
                'excel_cell_char_limit': 500,
                
                # DOCX Settings
                'docx_small_paragraph_limit': 20000,
                'docx_medium_paragraph_limit': 10000,
                'docx_large_paragraph_limit': 5000,
                'docx_table_limit_small': 50,
                'docx_table_limit_medium': 30,
                'docx_table_limit_large': 20,
                'docx_paragraph_char_limit': 5000,
                'docx_cell_char_limit': 1000
            }
        
        elif mode == "COMPLETE":
            return {
                # PDF Settings
                'pdf_small_threshold_mb': 50.0,  # Very high threshold
                'pdf_medium_threshold_mb': 100.0,
                'pdf_large_page_limit': None,  # No limit
                'pdf_enable_sampling': False,  # Never sample
                
                # Excel Settings
                'excel_small_row_limit': None,  # No limits
                'excel_medium_row_limit': None,
                'excel_large_row_limit': None,
                'excel_sheet_limit_medium': None,
                'excel_sheet_limit_large': None,
                'excel_cell_char_limit': None,
                
                # DOCX Settings
                'docx_small_paragraph_limit': None,  # No limits
                'docx_medium_paragraph_limit': None,
                'docx_large_paragraph_limit': None,
                'docx_table_limit_small': None,
                'docx_table_limit_medium': None,
                'docx_table_limit_large': None,
                'docx_paragraph_char_limit': None,
                'docx_cell_char_limit': None
            }
        
        else:
            raise ValueError(f"Unknown data integrity mode: {mode}")
    
    def get_pdf_settings(self) -> Dict[str, Any]:
        """Get PDF processing settings"""
        return {k: v for k, v in self.settings.items() if k.startswith('pdf_')}
    
    def get_excel_settings(self) -> Dict[str, Any]:
        """Get Excel processing settings"""
        return {k: v for k, v in self.settings.items() if k.startswith('excel_')}
    
    def get_docx_settings(self) -> Dict[str, Any]:
        """Get DOCX processing settings"""
        return {k: v for k, v in self.settings.items() if k.startswith('docx_')}
    
    def is_truncation_enabled(self) -> bool:
        """Check if any truncation is enabled"""
        return self.mode in ["AGGRESSIVE", "BALANCED", "CONSERVATIVE"]
    
    def get_mode_description(self) -> str:
        """Get description of current mode"""
        return self.MODES.get(self.mode, {}).get('description', 'Unknown mode')
    
    def print_current_settings(self) -> None:
        """Print current settings for review"""
        print(f"\n📊 DATA INTEGRITY MODE: {self.mode}")
        print(f"Description: {self.get_mode_description()}")
        print(f"Truncation Enabled: {self.is_truncation_enabled()}")
        
        print(f"\n📄 PDF Settings:")
        for key, value in self.get_pdf_settings().items():
            print(f"  {key}: {value}")
        
        print(f"\n📊 Excel Settings:")
        for key, value in self.get_excel_settings().items():
            print(f"  {key}: {value}")
        
        print(f"\n📝 DOCX Settings:")
        for key, value in self.get_docx_settings().items():
            print(f"  {key}: {value}")


# Global instance for easy access
data_integrity = DataIntegritySettings()


def get_data_integrity_settings(mode: str = None) -> DataIntegritySettings:
    """Get data integrity settings instance"""
    if mode:
        return DataIntegritySettings(mode)
    return data_integrity


def set_global_data_integrity_mode(mode: str) -> None:
    """Set global data integrity mode"""
    global data_integrity
    data_integrity = DataIntegritySettings(mode)
    print(f"✅ Data integrity mode set to: {mode}")
    data_integrity.print_current_settings() 