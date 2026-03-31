#!/bin/bash

# ==============================================================================
# SPACE CLOUD V6 - ISL BANDWIDTH & LATENCY THROTTLING
# ==============================================================================
# Role: Applies Linux Traffic Control (tc) rules to all satellite Minikube nodes
#       to simulate realistic Inter-Satellite Link (ISL) constraints.
#
# Constraints enforced:
#   Bandwidth: 50 Mbps   → limits relay transfer to ~4.0s per 25MB checkpoint hop
#   Latency:   40 ms     → adds round-trip delay to simulate LEO orbital distance
#
# Expected transfer time with SAMKNN checkpoint (≤25 MB):
#   T = (25 MB × 8 bits/byte) / 50 Mbps = 4.0 seconds per hop
#
# This script is called by setup_nodes.sh during cluster provisioning, or can
# be run standalone to re-apply throttling after a node restart.
# ==============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Only throttle the satellite nodes (not the ground station)
SATELLITE_NODES=("minikube-m02" "minikube-m03" "minikube-m04")

echo -e "${BLUE}>>> V6 ISL SIMULATION: Applying tc throttling to satellite nodes${NC}"

for NODE in "${SATELLITE_NODES[@]}"; do
    echo -e "${GREEN}[tc] Configuring node: ${NODE}${NC}"

    minikube ssh -n "$NODE" -p minikube "sudo -i <<'EOF'
        # ---- Remove any existing tc rules first (idempotent) ----
        tc qdisc del dev eth0 root 2>/dev/null || true

        # ---- LAYER 1: Token Bucket Filter (TBF) → 50 Mbps bandwidth cap ----
        # The TBF limits the outgoing bit rate of the interface.
        # burst=32kbit allows short bursts above the rate limit (normal for TCP)
        # latency=400ms is the maximum time a packet can wait in the queue
        tc qdisc add dev eth0 root handle 1: tbf \
            rate 50mbit \
            burst 32kbit \
            latency 400ms

        # ---- LAYER 2: Network Emulator (netem) → 40ms propagation delay ----
        # Stacked inside the TBF as a child queue discipline.
        # This adds a fixed 40ms delay to every outgoing packet (one-way),
        # simulating the radio propagation time over a LEO ISL.
        tc qdisc add dev eth0 parent 1:1 handle 10: netem \
            delay 40ms

        echo "tc rules applied to eth0 on $(hostname)"
        tc qdisc show dev eth0
EOF"
    echo -e "${GREEN}  ✅ ${NODE} throttled to 50Mbps / 40ms latency${NC}"
done

echo -e "${BLUE}>>> ISL THROTTLING COMPLETE.${NC}"
echo -e "    Checkpoint transfer estimate (SAMKNN ≤25MB): ~4.0s per hop"
echo -e "    Verify with: minikube ssh -n minikube-m02 'tc qdisc show dev eth0'"
