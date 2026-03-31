#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6 - MULTI-HOP RELAY TRANSFER
# ==============================================================================
# Role: Moves a CRIU checkpoint TAR file hop-by-hop across the satellite
#       constellation using SSH pipes between Minikube nodes.
#       Simulates CCSDS File Delivery Protocol (CFDP) with integrity verification.
#
# Usage: bash relay_transfer.sh <checkpoint_path> <manifest_path>
#   checkpoint_path: local path to the .tar checkpoint file (e.g. /tmp/cp.tar)
#   manifest_path:   JSON file with {"route": ["m02","m03"], "type": "thermal"}
#
# How it works:
#   For each hop in the route, the TAR is staged on the intermediate node via
#   an SSH pipe, then SHA256-verified on both ends before proceeding.
#   Only after successful delivery to the FINAL node does the script write
#   /tmp/relay_complete — the atomic trigger the receiving Node Agent polls for.
# ==============================================================================

set -e  # Abort on any error

# ANSI color codes for readable console output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

# ---- Argument Parsing ----
CHECKPOINT_PATH="$1"
MANIFEST_PATH="$2"

if [[ -z "$CHECKPOINT_PATH" || -z "$MANIFEST_PATH" ]]; then
    echo -e "${RED}ERROR: Missing arguments.${NC}"
    echo "Usage: $0 <checkpoint_path> <manifest_path>"
    exit 1
fi

if [[ ! -f "$CHECKPOINT_PATH" ]]; then
    echo -e "${RED}ERROR: Checkpoint file not found: $CHECKPOINT_PATH${NC}"
    exit 1
fi

if [[ ! -f "$MANIFEST_PATH" ]]; then
    echo -e "${RED}ERROR: Manifest file not found: $MANIFEST_PATH${NC}"
    exit 1
fi

# ---- Parse Manifest ----
# Extract the route array from the JSON manifest using Python (stdlib, no jq needed)
ROUTE_JSON=$(python3 -c "
import json, sys
manifest = json.load(open('$MANIFEST_PATH'))
# Output one node name per line for easy bash processing
print('\n'.join(manifest['route']))
")

# Build an array of hop nodes from the manifest
mapfile -t HOPS <<< "$ROUTE_JSON"
MIG_TYPE=$(python3 -c "import json; print(json.load(open('$MANIFEST_PATH'))['type'])")
TOTAL_HOPS=${#HOPS[@]}

echo -e "${BLUE}>>> V6 RELAY TRANSFER <<< Type=$MIG_TYPE | Hops=$TOTAL_HOPS${NC}"
echo -e "${BLUE}>>> Route: ${HOPS[*]}${NC}"

# ---- Compute source SHA256 ----
# This is the ground truth we verify against at every hop
SOURCE_SHA=$(sha256sum "$CHECKPOINT_PATH" | cut -d' ' -f1)
FILE_SIZE=$(du -h "$CHECKPOINT_PATH" | cut -f1)
echo -e "${GREEN}[SRC] Checkpoint: $CHECKPOINT_PATH ($FILE_SIZE) SHA256: ${SOURCE_SHA:0:16}...${NC}"

t_start=$(date +%s%N)  # Nanosecond precision for timing measurements

# ---- Multi-Hop Relay Loop ----
PREV_NODE="localhost"   # Starting point is the current sender node

for i in "${!HOPS[@]}"; do
    DEST_NODE="${HOPS[$i]}"
    HOP_NUM=$((i + 1))
    IS_FINAL=false
    if [[ $HOP_NUM -eq $TOTAL_HOPS ]]; then
        IS_FINAL=true
    fi

    echo ""
    echo -e "${GREEN}[HOP $HOP_NUM/$TOTAL_HOPS] ${PREV_NODE} → ${DEST_NODE}${NC}"

    # Create the staging directory on the destination node
    minikube ssh -n "$DEST_NODE" "mkdir -p /tmp/relay/" 2>/dev/null

    t_hop_start=$(date +%s%N)

    if [[ "$PREV_NODE" == "localhost" ]]; then
        # First hop: pipe directly from the local filesystem into the remote node
        echo -e "  → Piping checkpoint to ${DEST_NODE}..."
        cat "$CHECKPOINT_PATH" | minikube ssh -n "$DEST_NODE" "cat > /tmp/relay/checkpoint.tar"
    else
        # Intermediate hop: pipe from the previous node's staging area to the next node
        # This simulates ISL store-and-forward relay (DTN node behaviour)
        echo -e "  → Relaying from ${PREV_NODE} staging area to ${DEST_NODE}..."
        minikube ssh -n "$PREV_NODE" "cat /tmp/relay/checkpoint.tar" | \
            minikube ssh -n "$DEST_NODE" "cat > /tmp/relay/checkpoint.tar"
    fi

    t_hop_end=$(date +%s%N)
    t_hop_ms=$(( (t_hop_end - t_hop_start) / 1000000 ))
    echo -e "  ⏱️  Transfer time: ${t_hop_ms}ms"

    # ---- SHA256 Integrity Verification ----
    # Compute the hash on the destination node and compare with the source hash.
    # If they don't match, a bit-flip or truncation occurred → abort the transfer.
    echo -e "  🔍 Verifying integrity on ${DEST_NODE}..."
    DEST_SHA=$(minikube ssh -n "$DEST_NODE" "sha256sum /tmp/relay/checkpoint.tar | cut -d' ' -f1" 2>/dev/null)

    if [[ "$DEST_SHA" != "$SOURCE_SHA" ]]; then
        echo -e "${RED}  ❌ INTEGRITY FAILURE on ${DEST_NODE}!${NC}"
        echo -e "${RED}     Expected: $SOURCE_SHA${NC}"
        echo -e "${RED}     Got:      $DEST_SHA${NC}"
        exit 2  # Exit with error code 2 so the Node Agent knows it was an integrity failure
    fi

    echo -e "  ${GREEN}✅ SHA256 verified: ${DEST_SHA:0:16}...${NC}"

    # Clean up the staging file on the PREVIOUS intermediate node (not the source)
    # This frees disk space on intermediate satellites after delivery
    if [[ "$PREV_NODE" != "localhost" ]]; then
        echo -e "  🧹 Cleaning staging area on ${PREV_NODE}..."
        minikube ssh -n "$PREV_NODE" "rm -f /tmp/relay/checkpoint.tar" 2>/dev/null || true
    fi

    PREV_NODE="$DEST_NODE"
done

# ---- Final Node: Promote TAR and write atomic trigger ----
# The TAR must be moved to /tmp/checkpoint.tar (the standard path expected by
# the receiving Node Agent's rebuild_and_deploy() function) BEFORE the trigger
# is written. This guarantees the file is fully written before the agent sees it.
FINAL_NODE="${HOPS[$((TOTAL_HOPS-1))]}"
echo ""
echo -e "${GREEN}[FINAL] Promoting checkpoint on ${FINAL_NODE}...${NC}"

# Move from staging to the standard restore path
minikube ssh -n "$FINAL_NODE" "mv /tmp/relay/checkpoint.tar /tmp/checkpoint.tar"

# Compute total wall-clock transfer time for thesis timing measurements
t_end=$(date +%s%N)
t_total_ms=$(( (t_end - t_start) / 1000000 ))
echo -e "⏱️  Total relay time: ${t_total_ms}ms (${TOTAL_HOPS} hops)"
echo -e "📊 Expected at 50 Mbps + 40ms latency: ~$((TOTAL_HOPS * 4000))ms per hop"

# ---- Write the Atomic Trigger File ----
# This is the ONLY signal the receiving Node Agent trusts to start restoration.
# It is written AFTER SHA256 verification succeeds on the final node —
# never before. This eliminates the TOCTOU race where the agent could
# attempt to restore a partially-written or corrupted checkpoint.
echo -e "${GREEN}✅ Writing /tmp/relay_complete on ${FINAL_NODE}...${NC}"
minikube ssh -n "$FINAL_NODE" "touch /tmp/relay_complete"

echo -e "${BLUE}>>> RELAY COMPLETE ✓ Checkpoint delivered to ${FINAL_NODE} in ${t_total_ms}ms${NC}"
