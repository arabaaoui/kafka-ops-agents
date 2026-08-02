"""
Configuration loader for all agents.
Reads from environment variables (set via Docker Compose or .env).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env if present (local dev)
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# MCP Confluent
MCP_CONFLUENT_URL = os.getenv("MCP_CONFLUENT_URL", "http://mcp-confluent:3000")

# --- LLM config, one block per agent ---
# Each agent can run a different provider/model. Supported providers: openai, anthropic, gemini
# (see agents/common/adk_factory.py — all providers route through LiteLLM).

DIAGNOSTIC_LLM_PROVIDER = os.getenv("DIAGNOSTIC_LLM_PROVIDER", "openai")
DIAGNOSTIC_LLM_MODEL = os.getenv("DIAGNOSTIC_LLM_MODEL", "gpt-4o")
DIAGNOSTIC_LLM_API_KEY = os.getenv("DIAGNOSTIC_LLM_API_KEY", "")

REMEDIATION_LLM_PROVIDER = os.getenv("REMEDIATION_LLM_PROVIDER", "anthropic")
REMEDIATION_LLM_MODEL = os.getenv("REMEDIATION_LLM_MODEL", "claude-sonnet-4-20250514")
REMEDIATION_LLM_API_KEY = os.getenv("REMEDIATION_LLM_API_KEY", "")

# Replay LLM is optional: if REPLAY_LLM_API_KEY is empty, the replay agent
# runs fully deterministic (no ADK agent instantiated at all).
REPLAY_LLM_PROVIDER = os.getenv("REPLAY_LLM_PROVIDER", "")
REPLAY_LLM_MODEL = os.getenv("REPLAY_LLM_MODEL", "")
REPLAY_LLM_API_KEY = os.getenv("REPLAY_LLM_API_KEY", "")

# Share Group
SHARE_GROUP_LOCK_DURATION_MS = int(os.getenv("SHARE_GROUP_LOCK_DURATION_MS", "30000"))
SHARE_GROUP_MAX_DELIVERY_ATTEMPTS = int(os.getenv("SHARE_GROUP_MAX_DELIVERY_ATTEMPTS", "5"))

# Skill path
SKILL_PATH = os.getenv("SKILL_PATH", "/app/skills/kafka-streams-filter/SKILL.md")

# Enrichment — simulated success rate when the replay agent tries to recover a missing siret
ENRICHMENT_SUCCESS_RATE = float(os.getenv("ENRICHMENT_SUCCESS_RATE", "0.8"))

# Topic names
TOPIC_FACTURATION = "facturation"
TOPIC_FACTURATION_CORRIGE = "facturation-corrige"
TOPIC_FACTURATION_DEAD_LETTER = "facturation-dead-letter"
TOPIC_ALERTS = "alerts"
TOPIC_INCIDENTS = "incidents"
TOPIC_REPLAY_TASKS = "replay-tasks"
TOPIC_AUDIT = "audit"

# Consumer group monitored by the diagnostic agent
FACTURATION_CONSUMER_GROUP = "facturation"


def validate() -> list[str]:
    """Validate required config. Returns list of missing items."""
    missing = []
    if not DIAGNOSTIC_LLM_API_KEY:
        missing.append("DIAGNOSTIC_LLM_API_KEY (set in .env or environment)")
    if not REMEDIATION_LLM_API_KEY:
        missing.append("REMEDIATION_LLM_API_KEY (set in .env or environment)")
    # REPLAY_LLM_API_KEY is optional — empty means deterministic replay.
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    return missing
