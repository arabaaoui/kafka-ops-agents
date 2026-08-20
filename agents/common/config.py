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

# Share Group (KIP-932) — broker/group settings, used by the native ShareConsumer
# for the remediation-confirmations channel. Not passed to the Python client
# directly; read here to log them and to feed the broker config in
# docker-compose.test.yml (group.share.record.lock.duration.ms / group.share.delivery.count.limit).
SHARE_GROUP_LOCK_DURATION_MS = int(os.getenv("SHARE_GROUP_LOCK_DURATION_MS", "30000"))
SHARE_GROUP_DELIVERY_COUNT_LIMIT = int(os.getenv("SHARE_GROUP_DELIVERY_COUNT_LIMIT", "5"))

# Topic names
TOPIC_FACTURES = "factures"
TOPIC_ALERTS = "alerts"
TOPIC_INCIDENTS = "incidents"
TOPIC_REMEDIATION_CONFIRMATIONS = "remediation-confirmations"
TOPIC_AUDIT = "audit"

# Consumer group monitored by the diagnostic agent (deliberately lagging — see problem_injector)
FACTURATION_CONSUMER_GROUP = "facturation"

# Lag manufactured by the problem injector, matching the article's incident (1452 messages).
TARGET_LAG = 1452


def validate() -> list[str]:
    """Validate required config. Returns list of missing items."""
    missing = []
    if not DIAGNOSTIC_LLM_API_KEY:
        missing.append("DIAGNOSTIC_LLM_API_KEY (set in .env or environment)")
    if not REMEDIATION_LLM_API_KEY:
        missing.append("REMEDIATION_LLM_API_KEY (set in .env or environment)")
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    return missing
