# SPEC — PoC 2 : control plane Kafka agent-native (diagnostic → remédiation)

**Target repo** : `arabaaoui/kafka-ops-agents`
**Source d'inspiration** : `arabaaoui/kafka-for-agents` (PoC 1 supply chain)
**Article associé** : https://kafblog.dolizone.com/blog/kafka-agents-ops-loop/

---

## Objectif

Démontrer la thèse de l'article : un agent peut **diagnostiquer** un problème de control plane Kafka via MCP (lecture seule), et un second agent peut **proposer puis exécuter** une correction sur la configuration d'un topic — après confirmation humaine explicite. Pas d'Agent Skills, pas de génération de code, pas de replay.

Le PoC illustre la séquence de l'article : un consumer group `facturation` accuse 1452 messages de retard sur le topic `factures` → l'agent diagnostic interroge le cluster via MCP et découvre que `compression.type` est resté à sa valeur par défaut `producer` sur un topic à fort débit → l'agent remédiation propose de passer `compression.type` à `lz4`, attend une confirmation humaine, puis exécute la correction.

---

## Réutilisation du PoC 1

**À COPIER TEL QUEL depuis `kafka-for-agents` :**

| Fichier/Dossier | Notes |
|---|---|
| `docker-compose.test.yml` | Kafka 4.2.x KRaft, KIP-932 broker config, Kafka UI :8081, kcat, kafka-init |
| `mcp-confluent/` | Serveur MCP Node.js (infra héritée — ne pas toucher) |
| `agents/Dockerfile.agent` | Image Python commune (infra héritée — ne pas toucher) |
| `agents/common/adk_factory.py` | AdkAgentRunner + create_llm (LiteLLM unifié) |
| `agents/common/share_group_client.py` | Wrapper autour du `ShareConsumer` natif KIP-932 (confluent-kafka >= 2.15.0 Preview) |
| `agents/common/config.py` | Adapté : DIAGNOSTIC_LLM_*, REMEDIATION_LLM_* (pas de REPLAY_LLM_*) |
| `agents/common/requirements.txt` | google-adk, litellm, confluent-kafka>=2.15.0, python-dotenv, httpx |
| `Makefile` | Adapté : targets demo-diag, check, etc. |
| `.env.example` | Adapté : variables LLM renommées |

**À NE PAS COPIER / SUPPRIMÉ :** `agents/replay/`, `skills/kafka-streams-filter/` (Agent Skills et replay ne sont plus dans l'article 2).

---

## Contraintes techniques réelles (vérifiées sur confluentinc/mcp-confluent)

- Sur un Kafka local (`bootstrap_servers`), le MCP Confluent expose : `consume-messages`, `list-topics`, `create-topics`, `delete-topics`, `produce-message`, `list-consumer-groups`, `describe-consumer-group`, `get-consumer-group-lag`.
- `get-topic-config` et `alter-topic-config` ne s'activent que derrière un bloc `kafka.rest_endpoint` + authentification (ou OAuth) — en pratique un cluster Confluent Cloud. Ils ne sont donc **pas disponibles** contre le Kafka local du `docker compose up`.
- Le serveur MCP maison de ce repo (`mcp-confluent/server.js`) n'implémente qu'un sous-ensemble local (`consume-messages`, `list-topics`, `get-topic-config` simplifié, `get-consumer-group-lag`) et ne doit pas être modifié (infra héritée).
- Conséquence pour le PoC : `get_consumer_lag` et `read_messages` (agent diagnostic) sont de vrais appels MCP contre le Kafka local. `get-topic-config` et `alter-topic-config` (agents diagnostic ET remédiation) sont **simulés** en Python — voir `agents/common/simulated_control_plane.py` — avec un log explicite (`SIMULATED: alter-topic-config(factures, compression.type=lz4)`) plutôt qu'un appel à un tool absent de ce cluster.

---

## Nouveaux fichiers à créer

### 1. `agents/common/simulated_control_plane.py`

Module partagé par les deux agents. Simule `get-topic-config` et `alter-topic-config` (pas d'appel réseau) :

- `get_topic_config_simulated(topic)` → retourne un snapshot déterministe (`compression.type=producer`, `retention.ms=604800000`), logue `SIMULATED: get-topic-config(...)`.
- `alter_topic_config_simulated(topic, key, value)` → met à jour le snapshot en mémoire, logue `SIMULATED: alter-topic-config(topic, key=value)`.

### 2. `agents/problem_injector/app.py`

Script one-shot qui simule un pic de facturation de fin de mois :

- Crée le topic `factures` (1 partition, via AdminClient, si absent).
- Produit un volume de messages de facturation (`{"id": N, "montant": X, "client_id": Y, "timestamp": ...}`) — pas de champ défectueux, le problème est un topic jamais retaillé pour son débit, pas une anomalie de contenu.
- Fait consommer et committer une partie de ces messages par un consumer du groupe `facturation`, en laissant délibérément un retard de 1452 messages (le chiffre de l'article) — c'est ce lag que l'agent diagnostic découvrira via `get-consumer-group-lag`.
- Tourne une seule fois (pas de boucle infinie).

### 3. `agents/diagnostic/agent.py` + `prompts.py`

Agent ADK qui diagnostique le lag du consumer group `facturation`.

**Tools ADK :**
- `get_consumer_lag(group)` → MCP Confluent `get-consumer-group-lag` (réel).
- `read_messages(topic, partition, count)` → MCP Confluent `consume-messages` (réel) — sert à écarter une anomalie de contenu (l'article : "payloads normaux, aucune anomalie de contenu").
- `get_topic_config(topic)` → **simulé** (`simulated_control_plane.get_topic_config_simulated`).
- `diagnose(lag_data_json, sample_messages_json, topic_config_json)` → tool interne déterministe : si `compression.type == "producer"`, la cause est "topic jamais retaillé pour son débit réel" ; publie l'incident sur `incidents` avec un `recommended_config`.

**Flow :** poll `alerts` (ou self-trigger après un délai de grâce) → `get_consumer_lag("facturation")` → `read_messages("factures", 0, N)` → `get_topic_config("factures")` → `diagnose(...)`.

### 4. `agents/remediation/agent.py` + `prompts.py`

Agent ADK qui propose puis exécute (après confirmation) la correction de configuration.

**Tools ADK :**
- `get_topic_config(topic)` → simulé, relit la config avant de proposer.
- `propose_change(topic, key, current_value, new_value)` → enregistre une proposition en attente, logue et publie sur `audit` (`status: pending_confirmation`). Appelé une fois par incident.
- `execute_change(topic, key, new_value)` → **simulé** (`simulated_control_plane.alter_topic_config_simulated`) ; refuse d'agir si aucune proposition n'est en attente pour ce topic/incident (porte de confirmation appliquée côté code, pas seulement côté prompt).

**Flow :**
1. Poll `incidents` (consumer group classique) → phase *propose* : `get_topic_config` puis `propose_change("factures", "compression.type", "producer", "lz4")`.
2. Poll `remediation-confirmations` via **ShareGroupClient** (KIP-932, ACK explicite — une confirmation n'est traitée qu'une fois, avec redélivraison automatique en cas de crash) → phase *execute* si `confirm: true` : `execute_change(...)`. Si `confirm: false`, annule la proposition sans exécuter.

### 5. `docker-compose.app.yml`

Adapté du PoC 1 : `problem-injector`, `diagnostic-agent`, `remediation-agent`. Pas de `replay-agent`.

### 6. `scripts/demo-diag.sh`

Scénario de démonstration : lance la stack, injecte le lag, démarre les deux agents, affiche le diagnostic puis la proposition, envoie une confirmation via un producer Kafka one-shot, affiche l'exécution simulée.

### 7. `README.md`

Documentation complète : use case, architecture Mermaid, quickstart, demo, config reference, lien vers l'article https://kafblog.dolizone.com/blog/kafka-agents-ops-loop/.

### 8. `tests/test_deterministic_flow.py`

Test local sans Docker/LLM : imports, prompts, fonctions déterministes (`build_diagnostic`, `simulated_control_plane`, flow propose→confirm→execute).

---

## Règles de design

- **ADK obligatoire.** Zéro `httpx.post()` vers une API LLM (les appels MCP en HTTP JSON-RPC restent autorisés, ce n'est pas un appel LLM).
- **LiteLLM unifié.** Tous les providers passent par `LiteLlm`.
- **Fallback déterministe.** Sans clé LLM : diagnostic utilise le pattern-matching fixe sur `compression.type`, remédiation utilise le template de proposition/exécution fixe.
- **Multi-modèle par agent.** `DIAGNOSTIC_LLM_*`, `REMEDIATION_LLM_*`.
- **ShareGroupClient réel** pour les confirmations de remédiation (même wrapper natif KIP-932 que PoC 1).
- **Porte de confirmation explicite**, appliquée côté code (pas seulement dans le prompt) : `execute_change` refuse d'agir sans proposition en attente.
- **Graceful shutdown**, code en anglais, prompts en français.
- **Ne pas toucher à l'infra héritée** (`mcp-confluent/`, `Dockerfile.agent`, Kafka).

---

## Différence clé avec le PoC 1

Le PoC 1 utilise Kafka comme **infrastructure de coordination** pour des agents métier (supply chain). Le PoC 2 utilise Kafka comme **cible des opérations** — les agents diagnostiquent et corrigent la configuration de Kafka lui-même. MCP est l'interface primaire (lecture réelle, écriture simulée faute d'accès Confluent Cloud). Il n'y a plus de génération de code (Agent Skills) ni de replay KIP-932 des messages — le KIP-932 sert ici à sécuriser le canal de confirmation humaine, pas à distribuer des tâches de traitement.
