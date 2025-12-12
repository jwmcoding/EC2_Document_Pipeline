"""
File Source Interface for Document Processing Pipeline

This module defines the abstract interface that all file sources must implement,
enabling seamless switching between Dropbox, local filesystem, and future sources.
"""

from abc import ABC, abstractmethod
from typing import Generator, Dict, Optional, List, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import logging


@dataclass
class FileMetadata:
    """Unified metadata structure for all file sources"""
    # Core file information
    path: str                    # Relative path from source root
    name: str                    # File name with extension
    size: int                    # Size in bytes
    modified_time: str          # ISO format timestamp
    file_type: str              # Extension (e.g., '.pdf')
    source_id: str              # Unique ID from source system
    
    # Source-specific information
    source_type: str            # 'dropbox' or 'local'
    full_source_path: str       # Complete path in source system
    
    # Optional metadata
    content_hash: Optional[str] = None     # For change detection
    is_downloadable: bool = True           # Can file be downloaded
    error_message: Optional[str] = None    # Any errors during discovery
    
    @property
    def size_mb(self) -> float:
        """Size in megabytes"""
        return round(self.size / (1024 * 1024), 2) if self.size else 0.0


class FileSourceInterface(ABC):
    """Abstract interface for file sources"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._supported_extensions = {
            '.pdf', '.docx', '.doc', '.xlsx', '.xls', '.csv', 
            '.txt', '.msg', '.png', '.jpg', '.jpeg', '.tiff',
            '.ppt', '.pptx'  # Added PowerPoint support
        }
    
    @abstractmethod
    def list_documents(self, folder_path: str, 
                      file_types: Optional[List[str]] = None,
                      batch_size: Optional[int] = None) -> Generator[FileMetadata, None, None]:
        """
        List all documents in the specified path.
        
        Args:
            folder_path: Path to scan for documents
            file_types: Optional list of file extensions to filter (e.g., ['.pdf', '.docx'])
            batch_size: If provided, yield documents in batches
            
        Yields:
            FileMetadata objects for each discovered document
        """
        pass
    
    @abstractmethod
    def download_file(self, file_path: str) -> bytes:
        """
        Download file content.
        
        Args:
            file_path: Path to the file (relative to source root)
            
        Returns:
            File content as bytes
            
        Raises:
            FileNotFoundError: If file doesn't exist
            PermissionError: If access is denied
            IOError: For other I/O errors
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate source connection/access.
        
        Returns:
            True if connection is valid and source is accessible
            
        Raises:
            ConnectionError: If connection cannot be established
            PermissionError: If authentication fails
        """
        pass
    
    @abstractmethod
    def get_file_content_hash(self, file_path: str) -> str:
        """
        Get a hash of the file content for change detection.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hash string (e.g., SHA256 hash)
        """
        pass
    
    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists in the source.
        
        Args:
            file_path: Path to check
            
        Returns:
            True if file exists
        """
        pass
    
    @abstractmethod
    def get_source_info(self) -> Dict[str, Any]:
        """
        Get information about the file source.
        
        Returns:
            Dict with source information like type, root path, etc.
        """
        pass
    
    def is_supported_file_type(self, file_name: str) -> bool:
        """
        Check if file type is supported for processing.
        
        Args:
            file_name: Name of the file
            
        Returns:
            True if file type is supported
        """
        import os
        ext = os.path.splitext(file_name)[1].lower()
        return ext in self._supported_extensions
    
    def batch_list_documents(self, folder_path: str, 
                           batch_size: int = 100,
                           file_types: Optional[List[str]] = None) -> Generator[List[FileMetadata], None, None]:
        """
        List documents in batches for memory efficiency.
        
        Args:
            folder_path: Path to scan
            batch_size: Number of documents per batch
            file_types: Optional file type filter
            
        Yields:
            Lists of FileMetadata objects
        """
        batch = []
        for doc in self.list_documents(folder_path, file_types):
            batch.append(doc)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield final partial batch
        if batch:
            yield batch


class FileSourceError(Exception):
    """Base exception for file source errors"""
    pass


class FileNotFoundError(FileSourceError):
    """Raised when a file cannot be found"""
    pass


class PermissionError(FileSourceError):
    """Raised when access is denied"""
    pass


class ConnectionError(FileSourceError):
    """Raised when connection to source fails"""
    pass 