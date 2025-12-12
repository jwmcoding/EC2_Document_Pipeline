# Dockerfile for Document Processing Pipeline with Docling + CUDA + EasyOCR
# 
# Build: docker build -t docling-processor .
# Run:   docker run --gpus all -v /data:/data --env-file .env docling-processor
#
# EasyOCR adds ~1.5GB to image size but provides significantly better English OCR
# than Tesseract, especially for scanned business documents.

# Use PyTorch base image with CUDA support (well-maintained, includes Python 3.11)
FROM pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# System dependencies for document processing
RUN apt-get update && apt-get install -y \
    # PDF processing
    poppler-utils \
    # OCR support (Tesseract as fallback)
    tesseract-ocr \
    tesseract-ocr-eng \
    # Image processing (required by EasyOCR and Docling)
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    # General utilities
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create work directory
WORKDIR /app

# Copy requirements first (Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r requirements.txt

# Install EasyOCR for improved English OCR on scanned documents
# EasyOCR provides significantly better accuracy than Tesseract for:
# - Scanned business documents
# - Complex layouts and tables
# - Low-quality scans
RUN python -m pip install --no-cache-dir easyocr>=1.7.0

# Pre-download EasyOCR English model during build (~1.5GB)
# This avoids runtime download delays and ensures reproducible builds
RUN python -c "import easyocr; reader = easyocr.Reader(['en'], gpu=False, download_enabled=True); print('EasyOCR English model downloaded successfully')"

# Copy application code
COPY src/ ./src/
COPY process_discovered_documents.py .
COPY discover_documents.py .
COPY config.py .

# Create data directory for volume mount
RUN mkdir -p /data /logs

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

# Healthcheck - verify Docling, EasyOCR, and CUDA
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s \
    CMD python -c "import docling; import easyocr; import torch; assert torch.cuda.is_available(), 'CUDA not available'" || exit 1

# Default: show help
CMD ["python", "process_discovered_documents.py", "--help"]

