# SPEC — PoC 2 : Kafka agent-native platform (diagnostic → correctif → replay)

**Target repo** : `arabaaoui/kafka-agentic-ops` (repo à créer)
**Source d'inspiration** : `arabaaoui/kafka-for-agents` (PoC 1 supply chain)
**Article associé** : [[kafka-plateforme-agent-native]]

---

## Objectif

Démontrer la thèse de l'article : Kafka est devenu une plateforme agent-native. Un agent peut **diagnostiquer** un problème sur un cluster Kafka (MCP), **générer** un correctif (Agent Skills), et **rejouer** les messages problématiques (KIP-932). Aucun humain n'intervient.

Le PoC illustre la séquence : consumer group `facturation` avec lag → diagnostic via MCP → génération d'un filtre Kafka Streams → replay des messages fautifs avec enrichissement.

---

## Réutilisation du PoC 1

**À COPIER TEL QUEL depuis `kafka-for-agents` :**

| Fichier/Dossier | Notes |
|---|---|
| `docker-compose.test.yml` | Kafka 4.2.1 KRaft, Kafka UI :8081, kcat, kafka-init |
| `mcp-confluent/` | Serveur MCP Node.js (4 tools) |
| `agents/Dockerfile.agent` | Image Python commune |
| `agents/common/adk_factory.py` | AdkAgentRunner + create_llm (LiteLLM unifié) |
| `agents/common/share_group_client.py` | Émulateur KIP-932 |
| `agents/common/config.py` | Adapter : renommer DETECTION_LLM_* → DIAGNOSTIC_LLM_*, DECISION_LLM_* → REMEDIATION_LLM_*, EXECUTION_LLM_* → REPLAY_LLM_* |
| `agents/common/requirements.txt` | google-adk, litellm, confluent-kafka, python-dotenv, httpx |
| `Makefile` | Adapté : targets demo-diag, check, etc. |
| `.env.example` | Adapté : variables LLM renommées |

**À NE PAS COPIER :** `agents/detection/`, `agents/decision/`, `agents/execution/`, `agents/simulator/`, `skills/supply-chain-replenishment/`

---

## Nouveaux fichiers à créer

### 1. `agents/problem-injector/app.py`

Script qui injecte un scénario problème dans le cluster Kafka :

- Crée le topic `facturation` (s'il n'existe pas déjà, via AdminClient)
- Produit 500 messages valides : `{"id": N, "siret": "12345678901234", "montant": X, "timestamp": ...}`
- Produit 50 messages invalides : `{"id": N, "siret": null, "montant": X, "timestamp": ...}` — simule le bug d'un producer legacy
- Affiche un résumé : "550 messages dans facturation (dont 50 avec siret=null)"
- Tourne une seule fois (pas de boucle infinie, c'est un injecteur one-shot)

### 2. `agents/diagnostic/agent.py` + `prompts.py`

Agent ADK qui diagnostique un problème Kafka.

**Tools ADK :**
- `get_consumer_lag(group: str)` → appelle MCP Confluent `get-consumer-group-lag`
- `read_messages(topic: str, partition: int, count: int)` → appelle MCP Confluent `consume-messages`
- `diagnose(lag_data, sample_messages)` → tool interne qui produit le diagnostic (pas d'appel externe)

**Prompt :** « Tu es un agent de diagnostic Kafka. On te signale un consumer group en retard. Utilise les tools MCP pour identifier la cause racine. »

**Flow :**
1. Poll un topic `alerts` (ou démarre sur signal)
2. Appelle `get_consumer_lag("facturation")` → lag détecté
3. Appelle `read_messages("facturation", 0, 5)` → trouve les messages avec `siret=null`
4. Produit un diagnostic JSON dans le topic `incidents` : `{"consumer_group": "facturation", "cause": "siret null", "messages_affected": 50, "timestamp": ...}`

### 3. `agents/remediation/agent.py` + `prompts.py`

Agent ADK qui génère un correctif.

**Tools ADK :**
- `read_incident()` → lit le topic `incidents`
- `generate_filter(incident)` → génère le code du filtre (via LLM, pas de code pré-écrit)
- `deploy_filter(code)` → log le correctif (dans le PoC, c'est un log + écriture dans `audit`)

**Prompt :** « Tu es un agent de remédiation Kafka. Pour chaque incident, génère le code d'un filtre Kafka Streams qui écarte ou corrige les messages problématiques. »

**Flow :**
1. Poll le topic `incidents`
2. Pour chaque incident : utilise le LLM pour générer un filtre Kafka Streams
3. Log le code généré dans `audit`
4. Produit une tâche de replay dans `replay-tasks` : `{"incident_id": ..., "topic": "facturation", "filter": "siret is null", "count": 50}`

### 4. `agents/replay/agent.py` + `prompts.py`

Agent ADK qui rejoue les messages via KIP-932 share groups.

**Tools ADK :**
- `enrich_message(message)` → tente de retrouver le siret manquant (simulé : 80% succès)
- `publish_corrected(message)` → publie dans `facturation-corrige`

**Flow (utilise ShareGroupClient du PoC 1) :**
1. Consomme `replay-tasks` via share group
2. Pour chaque tâche : lit les messages fautifs, tente enrichissement
3. Si enrichi → ACK + publie dans `facturation-corrige`
4. Si impossible après 3 tentatives → RELEASE → dead-letter

### 5. `skills/kafka-streams-filter/SKILL.md`

Skill Confluent authentique pour générer un filtre Kafka Streams :

```
SKILL: kafka-streams-filter
1. Identifier le champ problématique et la condition de filtrage
2. Générer une topologie Kafka Streams avec filter()
3. Configurer le Serde approprié (String, Avro, JSON)
4. Ajouter la gestion d'erreurs (DLT)
5. Générer le docker-compose de test local
6. Valider avec un test unitaire sur des messages synthétiques
```

### 6. `docker-compose.app.yml`

Adapté du PoC 1 : remplacer les 3 agents supply chain par diagnostic-agent, remediation-agent, replay-agent + ajouter problem-injector.

```yaml
services:
  problem-injector:
    build: { context: ./agents, dockerfile: Dockerfile.agent }
    command: python -m problem_injector.app
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
    depends_on: [kafka-init]

  diagnostic-agent:
    build: { context: ./agents, dockerfile: Dockerfile.agent }
    command: python -m diagnostic.agent
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
      MCP_CONFLUENT_URL: http://mcp-confluent:3000
      DIAGNOSTIC_LLM_PROVIDER: ${DIAGNOSTIC_LLM_PROVIDER:-openai}
      DIAGNOSTIC_LLM_MODEL: ${DIAGNOSTIC_LLM_MODEL:-gpt-4o}
      DIAGNOSTIC_LLM_API_KEY: ${DIAGNOSTIC_LLM_API_KEY}
    depends_on: [mcp-confluent]

  remediation-agent:
    build: { context: ./agents, dockerfile: Dockerfile.agent }
    command: python -m remediation.agent
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
      REMEDIATION_LLM_PROVIDER: ${REMEDIATION_LLM_PROVIDER:-anthropic}
      REMEDIATION_LLM_MODEL: ${REMEDIATION_LLM_MODEL:-claude-sonnet-4}
      REMEDIATION_LLM_API_KEY: ${REMEDIATION_LLM_API_KEY}
    volumes: [./skills:/app/skills:ro]

  replay-agent:
    build: { context: ./agents, dockerfile: Dockerfile.agent }
    command: python -m replay.agent
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
      REPLAY_LLM_PROVIDER: ${REPLAY_LLM_PROVIDER:-}
      REPLAY_LLM_MODEL: ${REPLAY_LLM_MODEL:-}
      REPLAY_LLM_API_KEY: ${REPLAY_LLM_API_KEY:-}
    deploy:
      replicas: 3
```

### 7. `scripts/demo-diag.sh`

Scénario de démonstration (5 min) :

```bash
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
```

### 8. `scripts/demo-replay.sh`

Démo standalone du replay scaling (optionnel, bonus).

### 9. `README.md`

Documentation complète : use case, architecture Mermaid, les 3 briques expliquées, quickstart, demo, config reference, lien vers l'article.

### 10. `tests/test_deterministic_flow.py`

Test local sans Docker/LLM (même principe que PoC 1) : vérifie les imports, les prompts, les fonctions déterministes.

---

## Règles de design

- **ADK obligatoire.** Zéro `httpx.post()` vers API LLM.
- **LiteLLM unifié.** Tous les providers passent par `LiteLlm`.
- **Fallback déterministe.** Sans clé LLM : l'agent Diagnostic utilise des règles fixes (pattern siret=null), l'agent Remediation génère un filtre template, l'agent Replay fait du retry simple.
- **Multi-modèle par agent.** DIAGNOSTIC_LLM_*, REMEDIATION_LLM_*, REPLAY_LLM_*.
- **ShareGroupClient réel** pour le replay (même émulateur KIP-932 que PoC 1).
- **Graceful shutdown**, code en anglais, prompts en français.
- **Ne pas toucher à l'infra héritée** (mcp-confluent, Dockerfile.agent, Kafka).

---

## Différence clé avec le PoC 1

Le PoC 1 utilise Kafka comme **infrastructure de coordination** pour des agents métier (supply chain). Le PoC 2 utilise Kafka comme **cible des opérations** — les agents diagnostiquent et réparent Kafka lui-même. MCP n'est plus un outil auxiliaire mais l'interface primaire. Agent Skills génère du code Kafka, pas des règles métier. KIP-932 fait du replay avec enrichissement, pas de la distribution de tâches homogènes.