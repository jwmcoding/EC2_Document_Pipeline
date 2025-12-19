"""
Quick test to verify our initialization works
"""
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(env_path)
    print(f"📁 Loaded .env from: {env_path}")
except ImportError:
    print("⚠️  python-dotenv not available, using system env vars")

# Get environment variables
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "business-documents")

print("\n🔧 Configuration Check:")
print("=" * 50)
print(f"✅ PINECONE_API_KEY: {'Set' if PINECONE_API_KEY else 'NOT SET'}")
print(f"✅ OPENAI_API_KEY: {'Set' if OPENAI_API_KEY else 'NOT SET'}")
print(f"✅ PINECONE_INDEX_NAME: {PINECONE_INDEX_NAME}")

# Test imports
try:
    from src.connectors.pinecone_client import PineconeDocumentClient
    print("✅ PineconeDocumentClient import successful")
    
    # Test initialization
    if PINECONE_API_KEY:
        print("🧪 Testing PineconeDocumentClient initialization...")
        client = PineconeDocumentClient(
            api_key=PINECONE_API_KEY,
            index_name=PINECONE_INDEX_NAME
        )
        print("✅ PineconeDocumentClient initialized successfully!")
    else:
        print("❌ Cannot test initialization - PINECONE_API_KEY not set")
        
except Exception as e:
    print(f"❌ Error: {e}")

# Test OpenAI
try:
    from openai import OpenAI
    if OPENAI_API_KEY:
        client = OpenAI(api_key=OPENAI_API_KEY)
        print("✅ OpenAI client initialized successfully!")
    else:
        print("❌ Cannot test OpenAI - OPENAI_API_KEY not set")
except Exception as e:
    print(f"❌ OpenAI Error: {e}")

print("\n🎉 All tests completed!")