#!/bin/bash
# ==============================================================================
# SPACE CLOUD V6 - ARTIFACT PRE-BAKING (BASELINE EXTRACTOR)
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [ ! -d "./infrastructure" ]; then
    echo -e "${RED}❌ Error: Run this script from the project root!${NC}"
    exit 1
fi

echo -e "${BLUE}>>> 💾 BASELINE EXTRACTOR <<<${NC}"

# 1. Find the active space-mission pod
POD_NAME=$(kubectl get pod -l app=space-mission -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)

if [ -z "$POD_NAME" ]; then
    echo -e "${RED}❌ Error: No running space-mission pod found!${NC}"
    exit 1
fi

echo -e "${YELLOW}📡 Target Pod: $POD_NAME${NC}"

# 2. Trigger the serialization on the python worker
echo -e "${YELLOW}⚙️  Triggering memory serialization on Phoenix worker...${NC}"
kubectl exec $POD_NAME -c payload-phoenix -- curl -s -X POST http://localhost:9000/extract_baseline

# 3. Copy the artifact from the container to the host
echo -e "${YELLOW}📥 Downloading baseline.npz to src/training-workload/...${NC}"
kubectl cp $POD_NAME:/app/baseline.npz ./src/training-workload/baseline.npz -c payload-phoenix

if [ -f "./src/training-workload/baseline.npz" ]; then
    echo -e "${GREEN}✅ Extraction complete! Artifact saved to src/training-workload/baseline.npz${NC}"
    echo -e "   Run ./ops/build_and_inject.sh to bake this memory bank into the next Docker build."
else
    echo -e "${RED}❌ Error: Failed to extract baseline.npz from the container.${NC}"
fi
