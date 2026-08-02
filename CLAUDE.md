# Kafka Agentic Ops — PoC 2

## Context
Ce repo implémente le PoC 2 de la série d'articles sur Kafka comme plateforme agent-native.
Il démontre la boucle ops : diagnostic → remédiation → replay, avec de vrais agents ADK.

## Source d'inspiration
Le PoC 1 (`kafka-for-agents`) est à `/tmp/kafka-retail-agents-poc/`.
Copier et adapter ses fichiers communs : docker-compose.test.yml, mcp-confluent/, agents/Dockerfile.agent,
agents/common/adk_factory.py, agents/common/share_group_client.py, agents/common/config.py,
agents/common/requirements.txt, Makefile, .env.example.

## Spécification complète
Le fichier SPEC.md à la racine contient la spec détaillée. SUIVRE EXACTEMENT.

## Règles absolues
- ADK obligatoire. Zéro httpx.post() vers API LLM.
- LiteLLM unifié pour tous les providers.
- Fallback déterministe si clé LLM absente.
- Multi-modèle par agent : DIAGNOSTIC_LLM_*, REMEDIATION_LLM_*, REPLAY_LLM_*.
- ShareGroupClient réel pour le replay.
- Code en anglais, prompts en français.
- Ne pas toucher à l'infra héritée (mcp-confluent, Dockerfile.agent, Kafka).
