# Kafka Agentic Ops — PoC 2

## Context
Ce repo implémente le PoC 2 de la série d'articles sur Kafka comme plateforme agent-native.
Il démontre la boucle control plane : diagnostic → remédiation (avec confirmation humaine), avec de vrais agents ADK.

## Source d'inspiration
Le PoC 1 (`kafka-for-agents`) est à `/opt/data/kafka-for-agents/`.
Copier et adapter ses fichiers communs : docker-compose.test.yml, mcp-confluent/, agents/Dockerfile.agent,
agents/common/adk_factory.py, agents/common/share_group_client.py, agents/common/config.py,
agents/common/requirements.txt, Makefile, .env.example.

## Spécification complète
Le fichier SPEC.md à la racine contient la spec détaillée. SUIVRE EXACTEMENT.

## Règles absolues
- ADK obligatoire. Zéro httpx.post() vers API LLM.
- LiteLLM unifié pour tous les providers.
- Fallback déterministe si clé LLM absente.
- Multi-modèle par agent : DIAGNOSTIC_LLM_*, REMEDIATION_LLM_*.
- ShareGroupClient réel (KIP-932 natif) pour le canal de confirmation de remédiation.
- get-topic-config et alter-topic-config sont SIMULÉS en local (voir agents/common/simulated_control_plane.py) :
  le MCP Confluent réel ne les expose que derrière un endpoint Kafka REST authentifié (Confluent Cloud),
  pas contre un bootstrap_servers local. Chaque appel simulé logue explicitement "SIMULATED: ...".
- La porte de confirmation humaine avant remédiation est appliquée côté code (pas seulement dans le prompt).
- Code en anglais, prompts en français.
- Ne pas toucher à l'infra héritée (mcp-confluent, Dockerfile.agent, Kafka).
