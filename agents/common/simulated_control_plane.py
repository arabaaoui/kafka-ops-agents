"""
Simulated Kafka REST control-plane calls — get-topic-config / alter-topic-config.

The real MCP Confluent server only exposes these two tools behind a
`kafka.rest_endpoint` + authentication block (or OAuth) — in practice a
Confluent Cloud cluster, not a local `bootstrap_servers` connection (see
CONFIGURATION.md in confluentinc/mcp-confluent: the "Kafka REST" tool
category, which includes exactly these two tools, requires
`rest_endpoint` + auth, unlike the "local deployments" tool set).

Against the local Docker Kafka used by this PoC, that endpoint does not
exist, so both the diagnostic and remediation agents simulate these two
calls here instead of making a real MCP request — every simulated call is
logged explicitly so it is never mistaken for an actual broker write.
"""

import logging

logger = logging.getLogger(__name__)

# Snapshot of the 'factures' topic config, as it would come back from a real
# get-topic-config call against Confluent Cloud. Mutated in place by
# alter_topic_config_simulated() so a diagnostic run after a remediation
# reflects the change.
_TOPIC_CONFIG: dict[str, dict[str, str]] = {
    "factures": {
        "compression.type": "producer",
        "retention.ms": "604800000",
    },
}


def get_topic_config_simulated(topic: str) -> dict[str, str]:
    """SIMULATED get-topic-config — no network call, see module docstring."""
    logger.info(f"SIMULATED: get-topic-config({topic})")
    return dict(_TOPIC_CONFIG.get(topic, {}))


def alter_topic_config_simulated(topic: str, key: str, value: str) -> dict:
    """SIMULATED alter-topic-config — no network call, see module docstring."""
    logger.info(f"SIMULATED: alter-topic-config({topic}, {key}={value})")
    _TOPIC_CONFIG.setdefault(topic, {})[key] = value
    return {"topic": topic, key: value, "status": "simulated_applied"}
