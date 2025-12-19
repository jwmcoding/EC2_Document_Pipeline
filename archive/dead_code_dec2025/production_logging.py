"""
Production Logging Configuration for Business Document Processing Pipeline

This module provides enterprise-grade logging setup for processing 2,100+ documents
with comprehensive error tracking, performance monitoring, and progress tracking.
"""

import logging
import logging.handlers
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Import colored logging components
from config.colored_logging import ColoredFormatter, ColoredProgressFormatter, create_colored_console_handler

class ProductionLogger:
    """Enhanced logging setup for production document processing with colors"""
    
    def __init__(self, 
                 log_dir: str = "logs",
                 log_level: str = "INFO",
                 max_file_size: int = 100 * 1024 * 1024,  # 100MB
                 backup_count: int = 5,
                 use_colors: bool = True):
        """
        Initialize production logging
        
        Args:
            log_dir: Directory for log files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
            max_file_size: Maximum size per log file before rotation
            backup_count: Number of backup files to keep
            use_colors: Whether to use colored terminal output
        """
        self.log_dir = log_dir
        self.log_level = getattr(logging, log_level.upper())
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.use_colors = use_colors
        
        # Create log directory
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup loggers
        self._setup_loggers()
        
    def _setup_loggers(self):
        """Setup comprehensive logging configuration with colors"""
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Clear any existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.DEBUG)
        
        # 1. Colored Console Handler - Progress and Important Messages
        if self.use_colors:
            console_handler = create_colored_console_handler(logging.INFO)
        else:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(simple_formatter)
        
        root_logger.addHandler(console_handler)
        
        # 2. Main Pipeline Log - All Details (no colors in file)
        pipeline_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, 'production_pipeline.log'),
            maxBytes=self.max_file_size,
            backupCount=self.backup_count
        )
        pipeline_handler.setLevel(logging.DEBUG)
        pipeline_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(pipeline_handler)
        
        # 3. Error Log - Errors Only (no colors in file)
        error_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, 'production_errors.log'),
            maxBytes=self.max_file_size // 10,  # Smaller error files
            backupCount=self.backup_count
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_handler)
        
        # 4. Colored Progress Log - Progress Updates Only
        progress_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, 'production_progress.log'),
            maxBytes=self.max_file_size // 20,  # Small progress files
            backupCount=self.backup_count
        )
        progress_handler.setLevel(logging.INFO)
        progress_handler.setFormatter(logging.Formatter('%(asctime)s - PROGRESS - %(message)s'))
        
        # Create progress logger with colored console output
        progress_logger = logging.getLogger('progress')
        progress_logger.setLevel(logging.INFO)
        progress_logger.addHandler(progress_handler)  # File handler (no colors)
        
        # Add colored console handler for progress
        if self.use_colors:
            from config.colored_logging import create_colored_progress_handler
            progress_console_handler = create_colored_progress_handler(logging.INFO)
            progress_logger.addHandler(progress_console_handler)
        
        progress_logger.propagate = False  # Don't send to root logger
        
        # 5. Performance Log - Metrics Only
        performance_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, 'production_performance.log'),
            maxBytes=self.max_file_size // 20,
            backupCount=self.backup_count
        )
        performance_handler.setLevel(logging.INFO)
        performance_handler.setFormatter(detailed_formatter)
        
        # Create performance logger
        performance_logger = logging.getLogger('performance')
        performance_logger.setLevel(logging.INFO)
        performance_logger.addHandler(performance_handler)
        performance_logger.propagate = False
        
        # 6. Business Log - Metadata and Business Logic
        business_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, 'production_business.log'),
            maxBytes=self.max_file_size // 10,
            backupCount=self.backup_count
        )
        business_handler.setLevel(logging.INFO)
        business_handler.setFormatter(detailed_formatter)
        
        # Create business logger
        business_logger = logging.getLogger('business')
        business_logger.setLevel(logging.INFO)
        business_logger.addHandler(business_handler)
        business_logger.propagate = False

class ProgressTracker:
    """Track and log processing progress with detailed metrics and colors"""
    
    def __init__(self, use_colors: bool = True):
        self.start_time = datetime.now()
        self.processed = 0
        self.total = 0
        self.errors = 0
        self.skipped = 0
        self.last_update = datetime.now()
        self.use_colors = use_colors
        
        self.progress_logger = logging.getLogger('progress')
        self.performance_logger = logging.getLogger('performance')
        
        # Import colored logger if colors are enabled
        if use_colors:
            from config.colored_logging import ColoredLogger
            self.colored_logger = ColoredLogger("progress_tracker")
        else:
            self.colored_logger = None
        
    def start_processing(self, total_documents: int):
        """Initialize processing tracking with colored output"""
        self.total = total_documents
        self.start_time = datetime.now()
        self.last_update = self.start_time
        
        if self.colored_logger:
            self.colored_logger.info(f"Starting processing of {total_documents} documents")
            self.colored_logger.info(f"Estimated time: ~{total_documents // 10} minutes at 10 docs/minute")
        else:
            self.progress_logger.info(f"Starting processing of {total_documents} documents")
        
        self.performance_logger.info(f"Processing started: {self.start_time}")
        
    def update_progress(self, 
                       processed_count: Optional[int] = None,
                       error_count: Optional[int] = None,
                       skip_count: Optional[int] = None,
                       force_log: bool = False):
        """Update processing progress with colored output"""
        
        if processed_count is not None:
            self.processed = processed_count
        if error_count is not None:
            self.errors = error_count
        if skip_count is not None:
            self.skipped = skip_count
            
        # Log every 50 documents or on force
        if self.processed % 50 == 0 or force_log:
            current_time = datetime.now()
            elapsed = (current_time - self.start_time).total_seconds()
            
            if elapsed > 0:
                rate = self.processed / elapsed * 60  # docs per minute
                if rate > 0:
                    eta_seconds = (self.total - self.processed) / rate * 60
                    eta_time = current_time + timedelta(seconds=eta_seconds)
                    eta_str = eta_time.strftime("%H:%M")
                else:
                    eta_str = "calculating..."
            else:
                rate = 0
                eta_str = "calculating..."
            
            percentage = (self.processed / self.total * 100) if self.total > 0 else 0
            
            # Colored progress messages
            if self.colored_logger:
                self.colored_logger.progress(
                    f"Progress: {self.processed}/{self.total} documents processed ({percentage:.1f}%)"
                )
                self.colored_logger.progress(
                    f"Processing rate: {rate:.1f} docs/minute | ETA: {eta_str}"
                )
                success_count = self.processed - self.errors
                if self.errors > 0:
                    self.colored_logger.warning(f"Success: {success_count} | Errors: {self.errors} | Skipped: {self.skipped}")
                else:
                    self.colored_logger.success(f"Success: {success_count} | Errors: {self.errors} | Skipped: {self.skipped}")
            else:
                # Fallback to regular logging
                self.progress_logger.info(
                    f"Progress: {self.processed}/{self.total} documents processed ({percentage:.1f}%)"
                )
                self.progress_logger.info(
                    f"Processing rate: {rate:.1f} docs/minute | ETA: {eta_str}"
                )
                self.progress_logger.info(
                    f"Success: {self.processed - self.errors} | Errors: {self.errors} | Skipped: {self.skipped}"
                )
            
            # Performance log (no colors in file)
            self.performance_logger.info(
                f"Performance metrics: {rate:.1f} docs/min, {percentage:.1f}% complete, "
                f"{self.errors} errors ({(self.errors/max(self.processed,1)*100):.2f}% error rate)"
            )
    
    def log_milestone(self, milestone_type: str, details: Dict[str, Any]):
        """Log processing milestones with colored output"""
        if self.colored_logger:
            self.colored_logger.milestone(f"{milestone_type} - {details}")
        else:
            self.progress_logger.info(f"🎉 Milestone: {milestone_type} - {details}")
        
    def complete_processing(self):
        """Log completion statistics with colored output"""
        end_time = datetime.now()
        total_time = end_time - self.start_time
        
        if self.colored_logger:
            self.colored_logger.success(f"Processing completed at {end_time}")
            self.colored_logger.info(f"Total time: {total_time}")
            self.colored_logger.success(f"Final stats: {self.processed} processed, {self.errors} errors, {self.skipped} skipped")
        else:
            self.progress_logger.info(f"Processing completed at {end_time}")
            self.progress_logger.info(f"Total time: {total_time}")
            self.progress_logger.info(f"Final stats: {self.processed} processed, {self.errors} errors, {self.skipped} skipped")
        
        self.performance_logger.info(
            f"Final performance: {total_time.total_seconds():.1f}s total, "
            f"{self.processed / total_time.total_seconds() * 60:.1f} docs/minute average"
        )

class ErrorTracker:
    """Track and categorize errors for analysis with colored output"""
    
    def __init__(self, use_colors: bool = True):
        self.errors = {
            'file_access': [],
            'parsing_failures': [],
            'api_timeouts': [],
            'validation_warnings': [],
            'rate_limits': [],
            'unknown': []
        }
        self.business_logger = logging.getLogger('business')
        self.use_colors = use_colors
        
        # Import colored logger if colors are enabled
        if use_colors:
            from config.colored_logging import ColoredLogger
            self.colored_logger = ColoredLogger("error_tracker")
        else:
            self.colored_logger = None
        
    def log_error(self, 
                  error_type: str, 
                  error_message: str, 
                  document_path: str = None,
                  additional_context: Dict[str, Any] = None):
        """Log and categorize an error with colored output"""
        
        error_entry = {
            'timestamp': datetime.now(),
            'type': error_type,
            'message': error_message,
            'document': document_path,
            'context': additional_context or {}
        }
        
        # Categorize error
        if error_type in self.errors:
            self.errors[error_type].append(error_entry)
        else:
            self.errors['unknown'].append(error_entry)
        
        # Log with colors if available
        if self.colored_logger:
            if error_type in ['file_access', 'parsing_failures', 'api_timeouts']:
                self.colored_logger.error(f"[{error_type}] {error_message}")
            else:
                self.colored_logger.warning(f"[{error_type}] {error_message}")
        
        # Also log to business logger for analysis (no colors in file)
        self.business_logger.error(
            f"Error [{error_type}]: {error_message} | Document: {document_path} | Context: {additional_context}"
        )
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get comprehensive error summary"""
        summary = {}
        total_errors = 0
        
        for category, error_list in self.errors.items():
            count = len(error_list)
            summary[category] = {
                'count': count,
                'percentage': 0,  # Will be calculated below
                'recent_errors': [e['message'] for e in error_list[-3:]]  # Last 3 errors
            }
            total_errors += count
        
        # Calculate percentages
        for category in summary:
            if total_errors > 0:
                summary[category]['percentage'] = (summary[category]['count'] / total_errors) * 100
        
        summary['total_errors'] = total_errors
        return summary

def setup_production_logging(log_dir: str = "logs", log_level: str = "INFO", use_colors: bool = True) -> tuple:
    """
    Setup production logging with colors and return tracker instances
    
    Args:
        log_dir: Directory for log files
        log_level: Logging level
        use_colors: Whether to use colored terminal output
    
    Returns:
        tuple: (ProductionLogger, ProgressTracker, ErrorTracker)
    """
    
    # Setup logging
    prod_logger = ProductionLogger(log_dir=log_dir, log_level=log_level, use_colors=use_colors)
    
    # Create trackers with color support
    progress_tracker = ProgressTracker(use_colors=use_colors)
    error_tracker = ErrorTracker(use_colors=use_colors)
    
    # Log startup with colors
    if use_colors:
        from config.colored_logging import ColoredLogger
        startup_logger = ColoredLogger("production_startup")
        startup_logger.success("Production logging initialized with colors")
        startup_logger.info(f"Log directory: {log_dir}")
        startup_logger.info(f"Log level: {log_level}")
    else:
        main_logger = logging.getLogger(__name__)
        main_logger.info("Production logging initialized")
        main_logger.info(f"Log directory: {log_dir}")
        main_logger.info(f"Log level: {log_level}")
    
    return prod_logger, progress_tracker, error_tracker

# Convenience function for quick setup with colors
def init_production_logging(use_colors: bool = True):
    """Quick initialization for production logging with colors"""
    return setup_production_logging(use_colors=use_colors) 