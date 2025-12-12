"""
Processing Progress Logger - Real-Time Progress Tracking with ETA Calculations

This module provides comprehensive progress logging for long-running document processing operations.
Transforms monitoring from "impossible" to "dead simple" by providing:

1. Real-time progress updates with ETA calculations
2. Structured log files with clear naming
3. Easy monitoring commands (tail -f, cat)
4. Completion summaries without JSON archaeology
5. Progress tracking with rate calculations

Features:
- Live progress updates every 30 seconds
- ETA calculations based on processing rate
- Clear log file naming with operation and timestamp
- Completion summary in easy-to-find location
- Console progress bar for immediate feedback
- Error tracking and statistics
"""

import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict


@dataclass
class ProcessingStats:
    """Statistics for processing operations"""
    total_items: int = 0
    processed: int = 0
    failed: int = 0
    skipped: int = 0
    chunks_created: int = 0
    processing_rate: float = 0.0  # items per minute
    estimated_time_remaining: Optional[str] = None


class ProcessingProgressLogger:
    """
    Real-time progress logger for document processing operations
    
    Provides:
    - Real-time progress tracking with ETA
    - Structured log files with clear naming
    - Easy monitoring commands
    - Completion summaries
    """
    
    def __init__(self, operation_name: str, total_items: int, 
                 dataset_name: str = "documents", enable_console: bool = True):
        """
        Initialize progress logger
        
        Args:
            operation_name: Name of the operation (e.g., "2023_documents")
            total_items: Total number of items to process
            dataset_name: Type of data being processed (e.g., "documents", "batches")
            enable_console: Whether to show console progress
        """
        self.operation_name = operation_name
        self.dataset_name = dataset_name
        self.total_items = total_items
        self.enable_console = enable_console
        
        # Initialize timing
        self.start_time = datetime.now()
        self.last_update_time = self.start_time
        self.last_processed_count = 0
        
        # Initialize statistics
        self.stats = ProcessingStats(total_items=total_items)
        
        # Create timestamp for file naming
        self.timestamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        
        # Setup log directory structure
        self.log_dir = Path("logs")
        self.progress_dir = self.log_dir / "progress"
        self.processing_dir = self.log_dir / "processing" 
        self.system_dir = self.log_dir / "system"
        
        # Create directories
        for directory in [self.log_dir, self.progress_dir, self.processing_dir, self.system_dir]:
            directory.mkdir(exist_ok=True)
        
        # Setup log files
        self.progress_log_file = self.progress_dir / f"{operation_name}_progress_{self.timestamp}.log"
        self.processing_log_file = self.processing_dir / f"{operation_name}_{self.timestamp}_ACTIVE.log"
        self.status_json_file = self.progress_dir / "current_processing_status.json"
        self.completion_summary_file = self.progress_dir / "latest_completion_summary.txt"
        
        # Initialize logging
        self._log_start()
        self._update_status_json()
        
        # Console progress setup
        if self.enable_console:
            print(f"\n🚀 PROCESSING STARTED: {operation_name} ({total_items:,} total {dataset_name})")
            print(f"📊 Monitor progress: tail -f {self.progress_log_file}")
            print(f"📈 Live status: cat {self.status_json_file}")
            print("-" * 80)
    
    def update_progress(self, increment: int = 1, failed: int = 0, skipped: int = 0, 
                       chunks_created: int = 0, custom_message: Optional[str] = None):
        """
        Update progress and calculate ETA
        
        Args:
            increment: Number of items processed (default: 1)
            failed: Number of items that failed
            skipped: Number of items that were skipped
            chunks_created: Number of chunks created from processed items
            custom_message: Optional custom message to include
        """
        # Update statistics
        self.stats.processed += increment
        self.stats.failed += failed
        self.stats.skipped += skipped
        self.stats.chunks_created += chunks_created
        
        # Calculate processing rate and ETA
        self._calculate_rate_and_eta()
        
        # Update status files
        self._update_status_json()
        
        # Log progress update
        self._log_progress_update(custom_message)
        
        # Console progress (every 10 items or major milestones)
        if self.enable_console and (self.stats.processed % 10 == 0 or 
                                   self.stats.processed in [1, 5, 25, 50, 100] or
                                   self.stats.processed == self.total_items):
            self._print_console_progress()
    
    def log_processing_detail(self, message: str, level: str = "INFO"):
        """Log detailed processing information to processing log"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} - {level} - {message}\n"
        
        with open(self.processing_log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
    
    def log_error(self, message: str, error_details: Optional[str] = None):
        """Log error with details"""
        full_message = message
        if error_details:
            full_message += f" | Details: {error_details}"
        
        self.log_processing_detail(full_message, "ERROR")
        
        # Also log to daily error log
        today = datetime.now().strftime("%Y%m%d")
        error_log = self.system_dir / f"errors_{today}.log"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(error_log, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} - {self.operation_name} - {full_message}\n")
    
    def log_completion_summary(self, additional_stats: Optional[Dict[str, Any]] = None):
        """Create comprehensive completion summary"""
        end_time = datetime.now()
        total_duration = end_time - self.start_time
        
        # Calculate final statistics
        success_rate = (self.stats.processed / self.total_items * 100) if self.total_items > 0 else 0
        final_rate = self.stats.processed / (total_duration.total_seconds() / 60) if total_duration.total_seconds() > 0 else 0
        
        # Create summary content
        summary_content = f"""🎉 PROCESSING COMPLETE - {self.operation_name}
========================================
📅 Started:     {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
📅 Completed:   {end_time.strftime('%Y-%m-%d %H:%M:%S')}
⏱️  Duration:    {self._format_duration(total_duration)}
📊 Documents:   {self.stats.processed:,}/{self.total_items:,} processed ({success_rate:.1f}% success)
🧩 Chunks:      {self.stats.chunks_created:,} created and uploaded
❌ Failures:    {self.stats.failed:,} documents (see error log)
⚠️  Skipped:     {self.stats.skipped:,} documents
🎯 Rate:        {final_rate:.1f} docs/minute average
💰 Cost:        See processing log for cost details
📁 Logs:        
   Progress: {self.progress_log_file}
   Processing: {self.processing_log_file}
   Errors: {self.system_dir}/errors_{datetime.now().strftime('%Y%m%d')}.log
"""

        # Add additional stats if provided
        if additional_stats:
            summary_content += "\n📈 Additional Statistics:\n"
            for key, value in additional_stats.items():
                summary_content += f"   {key}: {value}\n"
        
        summary_content += f"\n✅ Monitor next operation: tail -f logs/progress/*_progress_*.log\n"
        
        # Write to completion summary file
        with open(self.completion_summary_file, "w", encoding="utf-8") as f:
            f.write(summary_content)
        
        # Also create operation-specific summary
        operation_summary_file = self.processing_dir / f"{self.operation_name}_{self.timestamp}_SUMMARY.txt"
        with open(operation_summary_file, "w", encoding="utf-8") as f:
            f.write(summary_content)
        
        # Log to progress log
        self._log_to_progress_file(f"✅ PROCESSING COMPLETE: {self.stats.processed:,}/{self.total_items:,} ({success_rate:.1f}%) | Total: {self.stats.chunks_created:,} chunks")
        
        # Console output
        if self.enable_console:
            print("\n" + "=" * 80)
            print(summary_content)
            print("=" * 80)
            print(f"📄 Results saved to: {self.completion_summary_file}")
    
    def _calculate_rate_and_eta(self):
        """Calculate processing rate and estimated time remaining"""
        current_time = datetime.now()
        elapsed_seconds = (current_time - self.start_time).total_seconds()
        
        if elapsed_seconds > 0:
            # Calculate overall rate (items per minute)
            self.stats.processing_rate = (self.stats.processed / elapsed_seconds) * 60
            
            # Calculate ETA
            remaining_items = self.total_items - self.stats.processed
            if self.stats.processing_rate > 0:
                eta_minutes = remaining_items / self.stats.processing_rate
                eta_delta = timedelta(minutes=eta_minutes)
                self.stats.estimated_time_remaining = self._format_duration(eta_delta)
            else:
                self.stats.estimated_time_remaining = "calculating..."
        else:
            self.stats.estimated_time_remaining = "calculating..."
    
    def _log_start(self):
        """Log operation start to progress file"""
        start_message = f"🚀 PROCESSING STARTED: {self.operation_name} ({self.total_items:,} total {self.dataset_name})"
        self._log_to_progress_file(start_message)
        self.log_processing_detail(f"Started processing {self.operation_name} with {self.total_items:,} {self.dataset_name}")
    
    def _log_progress_update(self, custom_message: Optional[str] = None):
        """Log progress update to progress file"""
        percentage = (self.stats.processed / self.total_items * 100) if self.total_items > 0 else 0
        
        message = (f"📊 Progress: {self.stats.processed:,}/{self.total_items:,} ({percentage:.1f}%) | "
                  f"Rate: {self.stats.processing_rate:.1f} docs/min | "
                  f"ETA: {self.stats.estimated_time_remaining}")
        
        if self.stats.failed > 0:
            message += f" | Failures: {self.stats.failed:,}"
        
        if custom_message:
            message += f" | {custom_message}"
        
        self._log_to_progress_file(message)
    
    def _log_to_progress_file(self, message: str):
        """Write message to progress log file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} - {message}\n"
        
        with open(self.progress_log_file, "a", encoding="utf-8") as f:
            f.write(log_entry)
    
    def _update_status_json(self):
        """Update live status JSON file for programmatic monitoring"""
        current_time = datetime.now()
        elapsed_seconds = (current_time - self.start_time).total_seconds()
        
        status = {
            "operation_name": self.operation_name,
            "dataset_name": self.dataset_name,
            "status": "in_progress" if self.stats.processed < self.total_items else "completed",
            "start_time": self.start_time.isoformat(),
            "last_update": current_time.isoformat(),
            "elapsed_seconds": elapsed_seconds,
            "statistics": asdict(self.stats),
            "completion_percentage": (self.stats.processed / self.total_items * 100) if self.total_items > 0 else 0,
            "log_files": {
                "progress_log": str(self.progress_log_file),
                "processing_log": str(self.processing_log_file),
                "completion_summary": str(self.completion_summary_file)
            }
        }
        
        with open(self.status_json_file, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2)
    
    def _print_console_progress(self):
        """Print progress to console"""
        percentage = (self.stats.processed / self.total_items * 100) if self.total_items > 0 else 0
        
        # Create progress bar
        bar_width = 40
        filled_width = int(bar_width * percentage / 100)
        bar = "█" * filled_width + "▒" * (bar_width - filled_width)
        
        print(f"\r📊 [{bar}] {self.stats.processed:,}/{self.total_items:,} ({percentage:.1f}%) | "
              f"Rate: {self.stats.processing_rate:.1f}/min | ETA: {self.stats.estimated_time_remaining}", 
              end="", flush=True)
        
        # New line for milestones
        if self.stats.processed % 50 == 0 or self.stats.processed == self.total_items:
            print()  # New line
    
    def _format_duration(self, duration: timedelta) -> str:
        """Format duration in human-readable format"""
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"


def demo_progress_logger():
    """Demonstrate the progress logger functionality"""
    print("🎨 PROCESSING PROGRESS LOGGER DEMONSTRATION")
    print("=" * 60)
    
    # Create a demo logger
    logger = ProcessingProgressLogger("demo_operation", 100, "test_documents")
    
    # Simulate processing
    for i in range(1, 101):
        time.sleep(0.1)  # Simulate work
        
        # Simulate occasional failures
        failed = 1 if i % 25 == 0 else 0
        chunks = 3 if i % 10 == 0 else 2
        
        logger.update_progress(
            increment=1,
            failed=failed,
            chunks_created=chunks,
            custom_message=f"Processing document_{i:03d}.pdf"
        )
        
        # Log some processing details
        if i % 20 == 0:
            logger.log_processing_detail(f"Milestone: {i} documents processed")
    
    # Complete the operation
    logger.log_completion_summary({
        "Demo Mode": "Success",
        "Total Demo Time": "10 seconds",
        "Demo Files Created": "5 log files"
    })
    
    print(f"\n✅ Demo complete! Check logs directory for output files.")
    print(f"📊 Monitor command: tail -f {logger.progress_log_file}")
    print(f"📄 Results: cat {logger.completion_summary_file}")


if __name__ == "__main__":
    demo_progress_logger() 