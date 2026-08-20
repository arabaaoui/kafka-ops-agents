#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Agentic Ops — Diagnostic + Remédiation Demo ==="
echo ""

# 1. Lancer la stack test
make test-stack
sleep 10

# 2. Injecter le problème : pic de facturation de fin de mois, lag sur 'facturation'
echo "→ Injection du scénario (pic de facturation, lag sur le consumer group 'facturation')..."
docker compose -f docker-compose.app.yml run --rm problem-injector

# 3. Lancer les agents
echo "→ Démarrage des agents..."
docker compose -f docker-compose.app.yml up -d mcp-confluent
sleep 3
docker compose -f docker-compose.app.yml up -d diagnostic-agent remediation-agent

# 4. Attendre le diagnostic
echo "→ Attente du diagnostic..."
sleep 20
echo ""
echo "=== Diagnostic ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep -A2 "Incident produced" || echo "  (pas encore de diagnostic — augmente le délai si besoin)"

# 5. Attendre la proposition de remédiation
echo ""
echo "=== Proposition de remédiation ==="
sleep 10
docker compose -f docker-compose.app.yml logs remediation-agent | grep "PROPOSAL" || echo "  (pas encore de proposition)"

# 6. Porte de confirmation — étape manuelle
echo ""
echo "=== Confirmation requise ==="
echo "L'agent de remédiation attend une confirmation humaine avant d'exécuter la correction."
echo "Confirme avec : make confirm"
echo "(ou : ./scripts/confirm-remediation.sh <incident_id>)"
echo ""
echo "=== Bilan ==="
echo "Propositions et exécutions dans 'audit' :"
docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9093 --topic audit --from-beginning --timeout-ms 5000 2>/dev/null || echo "  (topic audit vide pour l'instant)"
