"""
Colored Logging Configuration for Business Document Processing Pipeline

This module provides colored terminal output for better readability:
- RED for errors
- YELLOW for warnings  
- GREEN for success messages
- BLUE for info messages
- CYAN for progress updates
- MAGENTA for milestones
"""

import logging
import sys
from datetime import datetime
from typing import Dict, Any, Optional

try:
    from colorama import Fore, Back, Style, init
    # Initialize colorama for Windows compatibility
    init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    # Fallback if colorama is not available
    class _MockColorama:
        RED = YELLOW = GREEN = BLUE = CYAN = MAGENTA = ""
        BRIGHT = DIM = ""
        RESET_ALL = ""
    
    Fore = Back = Style = _MockColorama()
    COLORAMA_AVAILABLE = False


class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels"""
    
    # Color mapping for different log levels
    COLORS = {
        'DEBUG': Fore.CYAN + Style.DIM,
        'INFO': Fore.BLUE,
        'WARNING': Fore.YELLOW + Style.BRIGHT,
        'ERROR': Fore.RED + Style.BRIGHT,
        'CRITICAL': Fore.RED + Back.YELLOW + Style.BRIGHT,
    }
    
    # Special colors for business content
    BUSINESS_COLORS = {
        'SUCCESS': Fore.GREEN + Style.BRIGHT,
        'PROGRESS': Fore.CYAN + Style.BRIGHT,
        'MILESTONE': Fore.MAGENTA + Style.BRIGHT,
        'VENDOR': Fore.BLUE + Style.BRIGHT,
        'CLIENT': Fore.GREEN,
        'DEAL': Fore.YELLOW,
        'METADATA': Fore.CYAN,
    }
    
    def __init__(self, fmt=None, datefmt=None, use_colors=True):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and COLORAMA_AVAILABLE
    
    def format(self, record):
        if not self.use_colors:
            return super().format(record)
        
        # Get the original formatted message
        original_format = super().format(record)
        
        # Apply color based on log level
        level_color = self.COLORS.get(record.levelname, '')
        
        if level_color:
            # Color the entire message
            colored_message = f"{level_color}{original_format}{Style.RESET_ALL}"
            
            # Add special highlighting for business terms
            colored_message = self._highlight_business_terms(colored_message)
            
            return colored_message
        
        return original_format
    
    def _highlight_business_terms(self, message: str) -> str:
        """Add special highlighting for business-related terms"""
        
        # Highlight success indicators
        if "✅" in message or "SUCCESS" in message.upper() or "PROCESSED" in message.upper():
            message = message.replace("✅", f"{self.BUSINESS_COLORS['SUCCESS']}✅{Style.RESET_ALL}")
        
        # Highlight progress indicators  
        if "Progress:" in message or "%" in message:
            # Keep original level color but add progress highlighting
            pass
        
        # Highlight milestones
        if "🎉" in message or "Milestone" in message:
            message = message.replace("🎉", f"{self.BUSINESS_COLORS['MILESTONE']}🎉{Style.RESET_ALL}")
        
        # Highlight vendors (common ones)
        vendor_terms = ["Salesforce", "Microsoft", "IBM", "Oracle", "Atlan", "DocuSign", "Cisco"]
        for vendor in vendor_terms:
            if vendor in message:
                message = message.replace(vendor, f"{self.BUSINESS_COLORS['VENDOR']}{vendor}{Style.RESET_ALL}")
        
        # Highlight deal numbers
        import re
        deal_pattern = r'\b(\d{5,6})\b'  # 5-6 digit numbers (deal IDs)
        message = re.sub(deal_pattern, f"{self.BUSINESS_COLORS['DEAL']}\\1{Style.RESET_ALL}", message)
        
        return message


class ColoredProgressFormatter(logging.Formatter):
    """Special formatter for progress messages with enhanced colors"""
    
    def __init__(self):
        super().__init__('%(asctime)s - %(message)s')
        self.use_colors = COLORAMA_AVAILABLE
    
    def format(self, record):
        if not self.use_colors:
            return super().format(record)
        
        message = record.getMessage()
        timestamp = self.formatTime(record)
        
        # Color code different types of progress messages
        if "Progress:" in message:
            # Extract percentage for color coding
            if "%" in message:
                try:
                    pct_str = message.split("(")[1].split("%")[0]
                    percentage = float(pct_str)
                    
                    if percentage < 25:
                        color = Fore.RED
                    elif percentage < 50:
                        color = Fore.YELLOW  
                    elif percentage < 75:
                        color = Fore.BLUE
                    else:
                        color = Fore.GREEN
                    
                    message = f"{color}{Style.BRIGHT}{message}{Style.RESET_ALL}"
                except:
                    message = f"{Fore.CYAN}{message}{Style.RESET_ALL}"
            else:
                message = f"{Fore.CYAN}{message}{Style.RESET_ALL}"
        
        elif "ETA:" in message or "docs/minute" in message:
            message = f"{Fore.BLUE}{message}{Style.RESET_ALL}"
        
        elif "Success:" in message:
            message = f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        
        elif "Errors:" in message and "0" not in message.split("Errors:")[1].split()[0]:
            message = f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        
        elif "🎉" in message:
            message = f"{Fore.MAGENTA}{Style.BRIGHT}{message}{Style.RESET_ALL}"
        
        else:
            message = f"{Fore.CYAN}{message}{Style.RESET_ALL}"
        
        return f"{Fore.WHITE}{Style.DIM}{timestamp}{Style.RESET_ALL} - {message}"


def create_colored_console_handler(log_level=logging.INFO):
    """Create a console handler with colored output"""
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    # Use colored formatter
    formatter = ColoredFormatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    handler.setFormatter(formatter)
    
    return handler


def create_colored_progress_handler(log_level=logging.INFO):
    """Create a progress handler with special colored formatting"""
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    # Use special progress formatter
    formatter = ColoredProgressFormatter()
    handler.setFormatter(formatter)
    
    return handler


def setup_colored_logging():
    """Setup colored logging for production pipeline"""
    
    # Clear existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG)
    
    # Add colored console handler
    console_handler = create_colored_console_handler(logging.INFO)
    root_logger.addHandler(console_handler)
    
    # Create special progress logger with colored output
    progress_logger = logging.getLogger('progress')
    progress_logger.handlers.clear()
    progress_logger.setLevel(logging.INFO)
    progress_logger.propagate = False
    
    progress_handler = create_colored_progress_handler(logging.INFO)
    progress_logger.addHandler(progress_handler)
    
    return root_logger, progress_logger


class ColoredLogger:
    """Enhanced logger with colored output methods"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.progress_logger = logging.getLogger('progress')
    
    def success(self, message: str):
        """Log success message in green"""
        self.logger.info(f"✅ {message}")
    
    def error(self, message: str):
        """Log error message in red"""
        self.logger.error(f"❌ {message}")
    
    def warning(self, message: str):
        """Log warning message in yellow"""
        self.logger.warning(f"⚠️  {message}")
    
    def info(self, message: str):
        """Log info message in blue"""
        self.logger.info(f"ℹ️  {message}")
    
    def debug(self, message: str):
        """Log debug message with special formatting"""
        self.logger.debug(f"🔍 {message}")
    
    def progress(self, message: str):
        """Log progress message with special formatting"""
        self.progress_logger.info(f"📊 {message}")
    
    def milestone(self, message: str):
        """Log milestone message in magenta"""
        self.progress_logger.info(f"🎉 {message}")
    
    def business(self, message: str):
        """Log business-related message with metadata highlighting"""
        self.logger.info(f"🏢 {message}")
    
    def processing(self, document_name: str, vendor: str = None, client: str = None):
        """Log document processing with business context"""
        context = ""
        if vendor:
            context += f" | Vendor: {vendor}"
        if client:
            context += f" | Client: {client}"
        
        self.logger.info(f"📄 Processing: {document_name}{context}")


def demo_colored_logging():
    """Demonstrate colored logging output"""
    
    print(f"\n{Fore.CYAN}{Style.BRIGHT}🎨 COLORED LOGGING DEMONSTRATION{Style.RESET_ALL}")
    print("=" * 60)
    
    # Setup colored logging
    setup_colored_logging()
    colored_logger = ColoredLogger("demo")
    
    # Demo different log types
    colored_logger.info("Pipeline initialization started")
    colored_logger.success("Dropbox connection established")
    colored_logger.warning("OpenAI rate limit at 85%")
    colored_logger.error("Failed to parse corrupted PDF file")
    
    # Demo progress messages
    colored_logger.progress("Progress: 150/2,087 documents processed (7.2%)")
    colored_logger.progress("Processing rate: 9.4 docs/minute | ETA: 18:42")
    colored_logger.progress("Success: 147 | Errors: 3 | Skipped: 0")
    
    # Demo milestones
    colored_logger.milestone("500 documents processed (24% complete)")
    colored_logger.milestone("Week 10 processing complete")
    
    # Demo business context
    colored_logger.business("Extracted metadata: Vendor: Salesforce, Client: Capital One, Deal: 56870")
    colored_logger.processing("56870-IDD-Salesforce.xlsx", "Salesforce", "Capital One Financial")
    
    print(f"\n{Fore.GREEN}{Style.BRIGHT}✅ Colored logging is ready for production!{Style.RESET_ALL}")


if __name__ == "__main__":
    demo_colored_logging() 