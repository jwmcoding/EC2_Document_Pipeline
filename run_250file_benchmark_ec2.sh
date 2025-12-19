#!/bin/bash
#
# Run 250-file benchmark test on EC2 instance
# Tests metadata extraction, processing time, and Pinecone upload
#

set -e

# Configuration
EC2_IP="18.221.163.252"
SSH_KEY="$HOME/Downloads/docking.pem"
EC2_USER="ec2-user"
EC2_APP_DIR="/home/ec2-user/app"
EC2_DATA_DIR="/data/august-2024"
NAMESPACE="benchmark-250docs-docling-auto-ocr-2025-12-12"
DISCOVERY_FILE="benchmark_250docs_discovery_2025_12_12.json"
MAX_DOCS=250

echo "🚀 250-File Benchmark Test on EC2"
echo "=================================="
echo ""
echo "Configuration:"
echo "  EC2 Instance: ${EC2_USER}@${EC2_IP}"
echo "  Data Directory: ${EC2_DATA_DIR}"
echo "  Namespace: ${NAMESPACE}"
echo "  Max Documents: ${MAX_DOCS}"
echo "  Discovery File: ${DISCOVERY_FILE}"
echo ""

# Check if EC2 is accessible
echo "📡 Checking EC2 connectivity..."
if ! ssh -i "$SSH_KEY" -o ConnectTimeout=5 "${EC2_USER}@${EC2_IP}" "echo 'Connected'" > /dev/null 2>&1; then
    echo "❌ Cannot connect to EC2 instance. Is it running?"
    echo "   Start with: aws ec2 start-instances --instance-ids i-0c55fdf7fb3f660d8 --region us-east-2"
    exit 1
fi
echo "✅ EC2 accessible"
echo ""

# Sync latest code to EC2
echo "📤 Syncing latest code to EC2..."
scp -i "$SSH_KEY" \
    test_10_files_metadata.py \
    discover_documents.py \
    "${EC2_USER}@${EC2_IP}:${EC2_APP_DIR}/"

# Also sync the connector
scp -i "$SSH_KEY" \
    src/connectors/raw_salesforce_export_connector.py \
    "${EC2_USER}@${EC2_IP}:${EC2_APP_DIR}/src/connectors/" 2>/dev/null || echo "⚠️  Connector path may need manual sync"
echo "✅ Code synced"
echo ""

# Run the test on EC2 using Docker
echo "🧪 Running 250-file benchmark test on EC2 (Docker)..."
echo ""
echo "Command:"
echo "  docker run --rm --gpus all \\"
echo "    -v ${EC2_DATA_DIR}:${EC2_DATA_DIR} \\"
echo "    -v ${EC2_APP_DIR}:/app \\"
echo "    -w /app \\"
echo "    --env-file ${EC2_APP_DIR}/.env \\"
echo "    docling-processor:gpu \\"
echo "    python3 test_10_files_metadata.py \\"
echo "      --export-dir ${EC2_DATA_DIR} \\"
echo "      --discovery-file ${DISCOVERY_FILE} \\"
echo "      --namespace ${NAMESPACE} \\"
echo "      --max-docs ${MAX_DOCS} \\"
echo "      --limit ${MAX_DOCS} \\"
echo "      --workers 6"
echo ""

ssh -i "$SSH_KEY" "${EC2_USER}@${EC2_IP}" << EOF
cd ${EC2_APP_DIR}

# Run the test in Docker
docker run --rm --gpus all \
    -v ${EC2_DATA_DIR}:${EC2_DATA_DIR} \
    -v ${EC2_APP_DIR}:/app \
    -w /app \
    --env-file ${EC2_APP_DIR}/.env \
    docling-processor:gpu \
    python3 test_10_files_metadata.py \
        --export-dir ${EC2_DATA_DIR} \
        --discovery-file ${DISCOVERY_FILE} \
        --namespace ${NAMESPACE} \
        --max-docs ${MAX_DOCS} \
        --limit ${MAX_DOCS} \
        --workers 6

echo ""
echo "✅ Test complete!"
echo ""
echo "To verify results, run:"
echo "  docker run --rm --env-file ${EC2_APP_DIR}/.env docling-processor:gpu python3 test_10_files_metadata.py --skip-discovery --skip-processing --namespace ${NAMESPACE}"
EOF

echo ""
echo "✅ Benchmark test completed!"
echo ""
echo "Next steps:"
echo "  1. Verify metadata:"
echo "     ssh -i $SSH_KEY ${EC2_USER}@${EC2_IP}"
echo "     cd ${EC2_APP_DIR}"
echo "     docker run --rm --env-file ${EC2_APP_DIR}/.env docling-processor:gpu python3 test_10_files_metadata.py --skip-discovery --skip-processing --namespace ${NAMESPACE}"
echo ""
echo "  2. Check Pinecone namespace: ${NAMESPACE}"
echo "  3. Review discovery file: ${EC2_APP_DIR}/${DISCOVERY_FILE}"
echo "  4. Analyze results: Use src/metadata_mgmt/analyze_metadata_coverage.py --namespace ${NAMESPACE}"

