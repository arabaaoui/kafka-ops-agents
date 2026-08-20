#!/usr/bin/env bash
set -euo pipefail

INCIDENT_ID="${1:-}"

if [ -z "$INCIDENT_ID" ]; then
  echo "→ Recherche de la dernière proposition en attente sur 'audit'..."
  INCIDENT_ID=$(docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9093 --topic audit --from-beginning --timeout-ms 5000 2>/dev/null \
    | grep '"status": "pending_confirmation"' \
    | tail -1 \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["incident_id"])' 2>/dev/null || true)
fi

if [ -z "$INCIDENT_ID" ]; then
  echo "Aucune proposition en attente trouvée sur 'audit'."
  echo "Passe l'incident_id explicitement : ./scripts/confirm-remediation.sh <incident_id>"
  exit 1
fi

echo "→ Confirmation de la remédiation pour l'incident '$INCIDENT_ID'..."
echo "{\"incident_id\": \"$INCIDENT_ID\", \"confirm\": true}" | \
  docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9093 --topic remediation-confirmations

echo "Confirmation envoyée. Suis les logs avec : docker compose -f docker-compose.app.yml logs -f remediation-agent"
