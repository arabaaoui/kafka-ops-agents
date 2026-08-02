#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Agentic Ops — Diagnostic Demo ==="
echo ""

# 1. Lancer la stack test
make test-stack
sleep 10

# 2. Injecter le problème
echo "→ Injection du scénario problème..."
docker compose -f docker-compose.app.yml run --rm problem-injector

# 3. Lancer les agents
echo "→ Démarrage des agents..."
docker compose -f docker-compose.app.yml up -d diagnostic-agent remediation-agent
docker compose -f docker-compose.app.yml up -d --scale replay-agent=3

# 4. Attendre le diagnostic
echo "→ Attente du diagnostic..."
sleep 15
echo ""
echo "=== Diagnostic ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep -A5 "diagnostic"

# 5. Attendre la remédiation
echo ""
echo "=== Remédiation ==="
sleep 10
docker compose -f docker-compose.app.yml logs remediation-agent | grep -A10 "filter"

# 6. Attendre le replay
echo ""
echo "=== Replay ==="
sleep 15
docker compose -f docker-compose.app.yml logs replay-agent | grep -E "ACK|DEAD|enrichi"

# 7. Bilan
echo ""
echo "=== Bilan ==="
echo "Messages dans facturation-corrige :"
docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:9093 --topic facturation-corrige --time -1 2>/dev/null || echo "  (topic non créé — le replay n'a pas encore publié)"
