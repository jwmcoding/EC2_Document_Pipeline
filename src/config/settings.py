"""
Enhanced configuration settings for Document Processing Pipeline
"""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Settings:
    # Dropbox Configuration
    DROPBOX_ACCESS_TOKEN: str = os.getenv("DROPBOX_ACCESS_TOKEN")
    DROPBOX_REFRESH_TOKEN: str = os.getenv("DROPBOX_REFRESH_TOKEN")
    DROPBOX_APP_KEY: str = os.getenv("DROPBOX_APP_KEY")
    DROPBOX_APP_SECRET: str = os.getenv("DROPBOX_APP_SECRET")
    
    # Pinecone Configuration
    PINECONE_API_KEY: str = os.getenv("PINECONE_API_KEY")
    PINECONE_INDEX_NAME: str = os.getenv("PINECONE_INDEX_NAME", "business-documents")
    PINECONE_ENVIRONMENT: str = os.getenv("PINECONE_ENVIRONMENT", "us-east-1")
    
    # LLM Provider Configuration
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")  # "openai" or "ollama"
    
    # OpenAI Configuration
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
    
    # LangSmith Configuration (for user interaction tracing)
    LANGSMITH_API_KEY: str = os.getenv("LANGSMITH_API_KEY")
    LANGSMITH_PROJECT: str = os.getenv("LANGSMITH_PROJECT", "business-document-search")
    LANGSMITH_ENABLED: bool = os.getenv("LANGSMITH_ENABLED", "false").lower() == "true"
    
    # Ollama Configuration
    OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "phi4")
    OLLAMA_TIMEOUT: int = int(os.getenv("OLLAMA_TIMEOUT", "60"))  # seconds
    
    # LLM Context Window Configuration
    PHI4_MAX_TOKENS: int = int(os.getenv("PHI4_MAX_TOKENS", "12000"))  # Safe limit for Phi-4 (16K context)
    GPT4_MAX_TOKENS: int = int(os.getenv("GPT4_MAX_TOKENS", "24000"))  # Safe limit for GPT-4.1-mini
    
    # Processing Configuration
    MAX_CHUNK_SIZE: int = int(os.getenv("MAX_CHUNK_SIZE", "500"))
    CHUNK_OVERLAP: int = int(os.getenv("CHUNK_OVERLAP", "75"))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "10"))
    
    # Parallel Processing Configuration (Optimized for 14-core system)
    PARALLEL_PROCESSING_ENABLED: bool = os.getenv("PARALLEL_PROCESSING_ENABLED", "true").lower() == "true"
    MAX_PARALLEL_WORKERS: int = int(os.getenv("MAX_PARALLEL_WORKERS", "8"))  # Optimized for 14-core/36GB system
    PARALLEL_BATCH_SIZE: int = int(os.getenv("PARALLEL_BATCH_SIZE", "24"))   # Increased for better throughput
    WORKER_MEMORY_LIMIT_MB: int = int(os.getenv("WORKER_MEMORY_LIMIT_MB", "1024"))  # Increased for 36GB system
    
    # Phase 2: Parsing Optimization Configuration (NO TRUNCATION)
    OPTIMIZED_PARSING_ENABLED: bool = os.getenv("OPTIMIZED_PARSING_ENABLED", "true").lower() == "true"
    NO_TRUNCATION_MODE: bool = os.getenv("NO_TRUNCATION_MODE", "true").lower() == "true"
    
    # PDF Optimization Settings
    PDF_SMALL_THRESHOLD_MB: float = float(os.getenv("PDF_SMALL_THRESHOLD_MB", "1.0"))
    PDF_MEDIUM_THRESHOLD_MB: float = float(os.getenv("PDF_MEDIUM_THRESHOLD_MB", "10.0"))
    PDF_LARGE_PAGE_LIMIT: int = int(os.getenv("PDF_LARGE_PAGE_LIMIT", "20"))
    
    # Excel Optimization Settings  
    EXCEL_SMALL_THRESHOLD_MB: float = float(os.getenv("EXCEL_SMALL_THRESHOLD_MB", "1.0"))
    EXCEL_MEDIUM_THRESHOLD_MB: float = float(os.getenv("EXCEL_MEDIUM_THRESHOLD_MB", "10.0"))
    EXCEL_LARGE_ROW_LIMIT: int = int(os.getenv("EXCEL_LARGE_ROW_LIMIT", "1000"))
    
    # DOCX Optimization Settings
    DOCX_SMALL_THRESHOLD_MB: float = float(os.getenv("DOCX_SMALL_THRESHOLD_MB", "2.0"))
    DOCX_MEDIUM_THRESHOLD_MB: float = float(os.getenv("DOCX_MEDIUM_THRESHOLD_MB", "10.0"))
    DOCX_LARGE_PARAGRAPH_LIMIT: int = int(os.getenv("DOCX_LARGE_PARAGRAPH_LIMIT", "1000"))
    
    # Storage Configuration
    S3_BUCKET: Optional[str] = os.getenv("S3_BUCKET")
    REDIS_URL: Optional[str] = os.getenv("REDIS_URL")
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE")
    
    def validate(self) -> None:
        """Validate required configuration settings"""
        if not self.DROPBOX_ACCESS_TOKEN:
            raise ValueError("DROPBOX_ACCESS_TOKEN is required")
        if not self.PINECONE_API_KEY:
            raise ValueError("PINECONE_API_KEY is required")
        
        # Validate LLM provider configuration
        if self.LLM_PROVIDER.lower() == "openai":
            if not self.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY is required when LLM_PROVIDER=openai")
        elif self.LLM_PROVIDER.lower() == "ollama":
            # Ollama doesn't require API key, but validate URL format
            if not self.OLLAMA_BASE_URL.startswith(('http://', 'https://')):
                raise ValueError("OLLAMA_BASE_URL must be a valid URL")
        else:
            raise ValueError(f"LLM_PROVIDER must be 'openai' or 'ollama', got: {self.LLM_PROVIDER}")
    
    def get_llm_config(self) -> dict:
        """Get LLM configuration for the selected provider"""
        if self.LLM_PROVIDER.lower() == "openai":
            return {
                "provider": "openai",
                "api_key": self.OPENAI_API_KEY,
                "model": self.OPENAI_MODEL
            }
        elif self.LLM_PROVIDER.lower() == "ollama":
            return {
                "provider": "ollama", 
                "base_url": self.OLLAMA_BASE_URL,
                "model": self.OLLAMA_MODEL,
                "timeout": self.OLLAMA_TIMEOUT
            }
        else:
            raise ValueError(f"Unsupported LLM provider: {self.LLM_PROVIDER}")

settings = Settings() 