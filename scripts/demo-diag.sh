#!/usr/bin/env bash
set -euo pipefail

echo "=== Kafka Agentic Ops — Demo Diagnostic (poison message) ==="
echo ""

# 1. Lancer la stack test
make test-stack
sleep 10

# 2. Injecter le scénario : poison message qui bloque le consumer group 'facturation'
echo "→ Injection du scénario (poison message sur 'factures', consumer group 'facturation' bloqué)..."
docker compose -f docker-compose.app.yml run --rm problem-injector

# 3. Lancer le MCP Confluent puis l'agent de diagnostic
echo "→ Démarrage de MCP Confluent..."
docker compose -f docker-compose.app.yml up -d mcp-confluent
sleep 3
echo "→ Démarrage de l'agent de diagnostic..."
docker compose -f docker-compose.app.yml up -d diagnostic-agent

# 4. Attendre le diagnostic, le fix simulé et la vérification (boucle auto-déclenchée au démarrage)
echo "→ Attente du diagnostic → fix → vérification (environ 30s)..."
sleep 30
echo ""
echo "=== Diagnostic ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep -A2 "Incident produced" || echo "  (pas encore de diagnostic — augmente le délai si besoin)"
echo ""
echo "=== Fix simulé ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep "SIMULATED" || echo "  (pas encore de fix appliqué)"
echo ""
echo "=== Vérification post-fix ==="
docker compose -f docker-compose.app.yml logs diagnostic-agent | grep "Vérification post-fix" || echo "  (pas encore de vérification)"
echo ""
echo "=== Bilan ==="
echo "Diagnostic et vérification publiés sur 'incidents' :"
docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9093 --topic incidents --from-beginning --timeout-ms 5000 2>/dev/null || echo "  (topic incidents vide pour l'instant)"
