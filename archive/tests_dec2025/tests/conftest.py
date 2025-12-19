import sys
from pathlib import Path


# Ensure repository root is on sys.path so `import src...` works in tests
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def pytest_configure(config):
    # Register custom markers used by this repo's tests
    config.addinivalue_line(
        "markers",
        "llm: live tests that call an LLM API (skipped if OPENAI_API_KEY is missing)",
    )

"""
Pytest configuration for Salesforce integration tests
"""

import pytest

def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (requires actual export directory)"
    )

