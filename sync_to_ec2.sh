#!/bin/bash
# Sync latest code changes to EC2 instance
# Includes: Docling Auto-OCR, text field in metadata, and related updates

set -e

EC2_IP="18.221.163.252"
SSH_KEY="$HOME/Downloads/docking.pem"
REMOTE_USER="ec2-user"
REMOTE_APP_DIR="/home/ec2-user/app"

echo "============================================================"
echo "  Syncing Code Changes to EC2"
echo "============================================================"
echo ""
echo "Instance: $EC2_IP"
echo "App Directory: $REMOTE_APP_DIR"
echo ""

# Wait for instance to be running
echo "⏳ Waiting for EC2 instance to be ready..."
aws ec2 wait instance-running --instance-ids i-0c55fdf7fb3f660d8 --region us-east-2
sleep 10  # Give SSH a moment to start

# Test SSH connection
echo "🔌 Testing SSH connection..."
ssh -i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$REMOTE_USER@$EC2_IP" "echo 'SSH connection successful'" || {
    echo "❌ SSH connection failed. Instance may still be starting..."
    exit 1
}

echo "✅ Instance is ready"
echo ""

# Function to sync file
sync_file() {
    local_file="$1"
    remote_file="$2"
    
    if [ ! -f "$local_file" ]; then
        echo "⚠️  Warning: Local file not found: $local_file"
        return 1
    fi
    
    echo "📤 Syncing: $local_file -> $remote_file"
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=no "$local_file" "$REMOTE_USER@$EC2_IP:$remote_file"
    return $?
}

# Function to ensure remote directory exists
ensure_dir() {
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$REMOTE_USER@$EC2_IP" "mkdir -p $1"
}

echo "📁 Creating remote directories..."
ensure_dir "$REMOTE_APP_DIR/src/parsers"
ensure_dir "$REMOTE_APP_DIR/src/pipeline"
ensure_dir "$REMOTE_APP_DIR/src/connectors"
ensure_dir "$REMOTE_APP_DIR/src/utils"
echo ""

echo "🔄 Syncing core files..."
echo ""

# 1. Docling Parser (Auto-OCR implementation)
sync_file "src/parsers/docling_parser.py" "$REMOTE_APP_DIR/src/parsers/docling_parser.py"
echo "   ✅ Docling Auto-OCR parser"

# 2. Document Processor (serial path)
sync_file "src/pipeline/document_processor.py" "$REMOTE_APP_DIR/src/pipeline/document_processor.py"
echo "   ✅ Document processor (serial)"

# 3. Parallel Processor (parallel path)
sync_file "src/pipeline/parallel_processor.py" "$REMOTE_APP_DIR/src/pipeline/parallel_processor.py"
echo "   ✅ Parallel processor"

# 4. Pinecone Client (text field in metadata)
sync_file "src/connectors/pinecone_client.py" "$REMOTE_APP_DIR/src/connectors/pinecone_client.py"
echo "   ✅ Pinecone client (text field support)"

# 5. Discovery Persistence (Set import fix)
sync_file "src/utils/discovery_persistence.py" "$REMOTE_APP_DIR/src/utils/discovery_persistence.py"
echo "   ✅ Discovery persistence"

# 6. Main processing script
sync_file "process_discovered_documents.py" "$REMOTE_APP_DIR/process_discovered_documents.py"
echo "   ✅ Process discovered documents script"

# 7. Test script (updated with docling args)
sync_file "test_10_files_metadata.py" "$REMOTE_APP_DIR/test_10_files_metadata.py"
echo "   ✅ Test script"

# 8. Discovery script (if updated)
if [ -f "discover_documents.py" ]; then
    sync_file "discover_documents.py" "$REMOTE_APP_DIR/discover_documents.py"
    echo "   ✅ Discovery script"
fi

echo ""
echo "============================================================"
echo "✅ Code sync complete!"
echo "============================================================"
echo ""
echo "📋 Summary of changes synced:"
echo "   1. Docling OCR Always ON (reverted from auto mode - quality-first approach)"
echo "   2. TableFormer ACCURATE mode always enabled"
echo "   3. Text field in Pinecone metadata (truncated to 37KB)"
echo "   4. Updated CLI defaults for Docling (ocr_mode='on')"
echo "   5. Simplified metadata (removed pass_used tracking)"
echo ""
echo "🚀 Next steps:"
echo "   1. SSH into EC2: ssh -i $SSH_KEY $REMOTE_USER@$EC2_IP"
echo "   2. Test with: docker run --rm --gpus all -v /data:/data -v $REMOTE_APP_DIR:/app -w /app --env-file $REMOTE_APP_DIR/.env docling-processor:gpu python3 test_10_files_metadata.py --max-docs 5 --limit 5 --workers 1"
echo ""



