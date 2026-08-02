#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Demo: Replay Scaling (bonus, standalone)
# Scales replay-agent from 1 → 5 instances using KIP-932 Share Groups and
# watches cooperative consumption of 'replay-tasks'.
# Assumes the pipeline is already running (./scripts/demo-diag.sh, or at
# least `make test-stack` + `make app`).
# =============================================================================

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 1: Current replay-agent containers                                   ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose -f docker-compose.app.yml ps replay-agent

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 2: Scaling replay-agent to 5 instances                               ║"
echo "║           (docker compose -f docker-compose.app.yml up -d --scale ...)     ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose -f docker-compose.app.yml up -d --scale replay-agent=5

echo ""
echo "  ⏳ Waiting 5s for new instances to join the share group..."
sleep 5

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 3: Scaled replay-agent containers (now ×5)                           ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose -f docker-compose.app.yml ps replay-agent

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 4: Watching replay-agent logs for 15 seconds...                      ║"
echo "║           Look for ACK / DEAD-LETTER / 'enrichi' across replicas.          ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

timeout 15s docker compose -f docker-compose.app.yml logs replay-agent -f --tail=30 || true

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  🎉 REPLAY SCALING DEMO COMPLETE!                                          ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║                                                                              ║"
echo "║  What you just saw:                                                         ║"
echo "║    1. replay-agent scaled to 5 replicas with a single command               ║"
echo "║    2. All replicas cooperatively consume 'replay-tasks' (KIP-932)          ║"
echo "║    3. Each faulty message is enriched, published, or dead-lettered         ║"
echo "║                                                                              ║"
echo "║  Scale down when done:                                                     ║"
echo "║    docker compose -f docker-compose.app.yml up -d --scale replay-agent=3   ║"
echo "║                                                                              ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
