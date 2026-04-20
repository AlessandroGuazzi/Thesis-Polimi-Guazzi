#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6 - MULTI-HOP RELAY TRANSFER (DYNAMIC IP MESH)
# ==============================================================================

set -e  # Abort on any error

# ANSI color codes for readable console output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'  # No Color

# ---- Dynamic Internal IP Routing Table ----
# Reads the live IPs injected by Earth during cluster setup
declare -A NODE_IPS

# Look in /tmp, because that is where the host's /var/lib/space_cloud is mounted inside this container
if [ -f /tmp/routing_table.sh ]; then
    source /tmp/routing_table.sh
else
    echo -e "${RED}ERROR: Routing table missing at /tmp/routing_table.sh!${NC}"
    exit 1
fi

# ---- Argument Parsing ----
CHECKPOINT_PATH="$1"
MANIFEST_PATH="$2"

if [[ -z "$CHECKPOINT_PATH" || -z "$MANIFEST_PATH" ]]; then
    echo -e "${RED}ERROR: Missing arguments.${NC}"
    echo "Usage: $0 <checkpoint_path> <manifest_path>"
    exit 1
fi

# ---- Parse Manifest ----
ROUTE_JSON=$(python3 -c "
import json, sys
manifest = json.load(open('$MANIFEST_PATH'))
print('\n'.join(manifest['route']))
")

mapfile -t HOPS <<< "$ROUTE_JSON"
MIG_TYPE=$(python3 -c "import json; print(json.load(open('$MANIFEST_PATH'))['type'])")
TOTAL_HOPS=${#HOPS[@]}

echo -e "${BLUE}>>> V6 RELAY TRANSFER <<< Type=$MIG_TYPE | Hops=$TOTAL_HOPS${NC}"
echo -e "${BLUE}>>> Route: ${HOPS[*]}${NC}"

# ---- Compute source SHA256 ----
SOURCE_SHA=$(sha256sum "$CHECKPOINT_PATH" | cut -d' ' -f1)
FILE_SIZE=$(du -h "$CHECKPOINT_PATH" | cut -f1)
echo -e "${GREEN}[SRC] Checkpoint: $CHECKPOINT_PATH ($FILE_SIZE) SHA256: ${SOURCE_SHA:0:16}...${NC}"

t_start=$(date +%s%N)

# ---- Multi-Hop Relay Loop ----
PREV_NODE="localhost"

for i in "${!HOPS[@]}"; do
    DEST_NODE="${HOPS[$i]}"
    DEST_IP="${NODE_IPS[$DEST_NODE]}"
    HOP_NUM=$((i + 1))

    if [[ -z "$DEST_IP" ]]; then
        echo -e "${RED}ERROR: Could not resolve IP for $DEST_NODE in routing table!${NC}"
        exit 1
    fi

    echo ""
    echo -e "${GREEN}[HOP $HOP_NUM/$TOTAL_HOPS] ${PREV_NODE} → ${DEST_NODE} (${DEST_IP})${NC}"

    # Create the staging directory on the destination node
    ssh -o StrictHostKeyChecking=no root@$DEST_IP "mkdir -p /tmp/relay"

    t_hop_start=$(date +%s%N)

    if [[ "$PREV_NODE" == "localhost" ]]; then
        echo -e "  → Piping checkpoint to ${DEST_NODE}..."
        scp -o StrictHostKeyChecking=no "$CHECKPOINT_PATH" root@$DEST_IP:/tmp/relay/checkpoint.tar
    else
        PREV_IP="${NODE_IPS[$PREV_NODE]}"
        echo -e "  → Relaying from ${PREV_NODE} (${PREV_IP}) to ${DEST_NODE}..."
        ssh -o StrictHostKeyChecking=no root@$PREV_IP "scp -o StrictHostKeyChecking=no /tmp/relay/checkpoint.tar root@$DEST_IP:/tmp/relay/checkpoint.tar"
    fi

    t_hop_end=$(date +%s%N)
    t_hop_ms=$(( (t_hop_end - t_hop_start) / 1000000 ))
    echo -e "  ⏱️  Transfer time: ${t_hop_ms}ms"

    # ---- SHA256 Integrity Verification ----
    echo -e "  🔍 Verifying integrity on ${DEST_NODE}..."
    DEST_SHA=$(ssh -o StrictHostKeyChecking=no root@$DEST_IP "sha256sum /tmp/relay/checkpoint.tar" | awk '{print $1}')

    if [[ "$DEST_SHA" != "$SOURCE_SHA" ]]; then
        echo -e "${RED}  ❌ INTEGRITY FAILURE on ${DEST_NODE}!${NC}"
        echo -e "${RED}     Expected: $SOURCE_SHA${NC}"
        echo -e "${RED}     Got:      $DEST_SHA${NC}"
        exit 2
    fi

    echo -e "  ${GREEN}✅ SHA256 verified: ${DEST_SHA:0:16}...${NC}"

    # Clean up the staging file on the PREVIOUS intermediate node
    if [[ "$PREV_NODE" != "localhost" ]]; then
        echo -e "  🧹 Cleaning staging area on ${PREV_NODE}..."
        ssh -o StrictHostKeyChecking=no root@$PREV_IP "rm -f /tmp/relay/checkpoint.tar" 2>/dev/null || true
    fi

    PREV_NODE="$DEST_NODE"
done

# ---- Final Node: Promote TAR and write atomic trigger ----
FINAL_NODE="${HOPS[$((TOTAL_HOPS-1))]}"
FINAL_IP="${NODE_IPS[$FINAL_NODE]}"
echo ""
echo -e "${GREEN}[FINAL] Promoting checkpoint on ${FINAL_NODE} (${FINAL_IP})...${NC}"

# Move from staging to the absolute, permanent restore path
ssh -o StrictHostKeyChecking=no root@$FINAL_IP "mkdir -p /var/lib/space_cloud"
ssh -o StrictHostKeyChecking=no root@$FINAL_IP "mv /tmp/relay/checkpoint.tar /var/lib/space_cloud/checkpoint.tar"

# Compute total wall-clock transfer time
t_end=$(date +%s%N)
t_total_ms=$(( (t_end - t_start) / 1000000 ))
echo -e "⏱️  Total relay time: ${t_total_ms}ms (${TOTAL_HOPS} hops)"
echo -e "📊 Expected at 50 Mbps + 40ms latency: ~$((TOTAL_HOPS * 4000))ms per hop"

# Write the Atomic Trigger File directly to the permanent mailbox
echo -e "${GREEN}✅ Writing trigger on ${FINAL_NODE}...${NC}"
ssh -o StrictHostKeyChecking=no root@$FINAL_IP "touch /var/lib/space_cloud/relay_complete"

echo -e "${BLUE}>>> RELAY COMPLETE ✓ Checkpoint delivered to ${FINAL_NODE} in ${t_total_ms}ms${NC}"