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

# Topic names
TOPIC_FACTURES = "factures"
TOPIC_ALERTS = "alerts"
TOPIC_INCIDENTS = "incidents"

# Consumer group monitored by the diagnostic agent (stuck on a poison message — see problem_injector)
FACTURATION_CONSUMER_GROUP = "facturation"

# 'factures' is a single partition so the offset arithmetic in the article stays exact.
FACTURATION_PARTITION = 0

# Offset of the poison message manufactured by the problem injector — the exact figure
# from the article's incident. The message at this offset is missing its 'siret' field,
# which crashes the (simulated) 'facturation' consumer before it commits past it.
POISON_OFFSET = 1452

# Valid messages produced after the poison offset, so the diagnostic agent can scan a
# window past it and confirm a single isolated bad message rather than a burst.
MESSAGES_AFTER_POISON = 30

# How many messages the diagnostic agent scans past the poison offset to rule out a burst.
DIAGNOSTIC_SCAN_COUNT = 5

# Delay before re-checking the lag after the simulated manual fix, so the
# catch-up consumer has time to drain the backlog.
VERIFY_FIX_DELAY_S = 3

# How long the simulated catch-up consumer (standing in for the operator
# restarting the real 'facturation' service once the poison message is
# skipped) is allowed to run before it's considered stalled.
CATCHUP_TIMEOUT_S = 15


def validate() -> list[str]:
    """Validate required config. Returns list of missing items."""
    missing = []
    if not DIAGNOSTIC_LLM_API_KEY:
        missing.append("DIAGNOSTIC_LLM_API_KEY (set in .env or environment)")
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    return missing
