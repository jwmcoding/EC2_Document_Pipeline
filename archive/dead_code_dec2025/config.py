"""
Configuration settings for Dropbox API project.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Dropbox API Configuration
DROPBOX_ACCESS_TOKEN = os.getenv('DROPBOX_ACCESS_TOKEN')

if not DROPBOX_ACCESS_TOKEN:
    raise ValueError(
        "DROPBOX_ACCESS_TOKEN environment variable is required. "
        "Please create a .env file with your Dropbox access token."
    ) 