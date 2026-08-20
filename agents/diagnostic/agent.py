"""
Diagnostic Agent — Diagnoses Kafka consumer group lag via MCP Confluent, in
read-only mode.

Polls the 'alerts' topic (or simply runs at startup, acting as its own
trigger) and diagnoses the 'facturation' consumer group: fetches lag via MCP,
samples the 'factures' topic to rule out a content problem, inspects the
topic's configuration, and publishes a structured diagnostic to 'incidents'.

get_consumer_lag and read_messages are real MCP Confluent calls against the
local Kafka. get_topic_config is SIMULATED (see
common/simulated_control_plane.py): the real MCP tool only works against a
Confluent Cloud cluster, not this local bootstrap_servers connection.

The diagnostic LLM is OPTIONAL, same principle as the other agent. If
DIAGNOSTIC_LLM_API_KEY is empty, no ADK agent is ever instantiated: the
Python loop calls get_consumer_lag / read_messages / get_topic_config /
diagnose directly, producing the exact same deterministic result.
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone

import httpx
from confluent_kafka import Consumer, Producer, KafkaError

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MCP_CONFLUENT_URL,
    TOPIC_ALERTS,
    TOPIC_INCIDENTS,
    TOPIC_FACTURES,
    FACTURATION_CONSUMER_GROUP,
    DIAGNOSTIC_LLM_PROVIDER,
    DIAGNOSTIC_LLM_MODEL,
    DIAGNOSTIC_LLM_API_KEY,
)
from common.simulated_control_plane import get_topic_config_simulated
from common.adk_factory import AdkAgentRunner
from diagnostic.prompts import SYSTEM_PROMPT, DIAGNOSTIC_USER_PROMPT

# Logging with [DIAGNOSTIC] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DIAGNOSTIC] %(levelname)s %(message)s",
)
logger = logging.getLogger("diagnostic")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# Startup grace period: wait this long for a message on 'alerts' before
# running the diagnostic pass on its own (the demo has no alerting producer,
# so the agent acts as its own trigger).
STARTUP_GRACE_S = 15
# How long to wait, once diagnosed, before allowing another diagnostic pass.
REDIAGNOSE_COOLDOWN_S = 60


def call_mcp(tool_name: str, arguments: dict) -> dict | list:
    """Call an MCP Confluent tool (consume-messages, get-consumer-group-lag, ...) via HTTP JSON-RPC."""
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
            "id": int(time.time() * 1000),
        }
        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{MCP_CONFLUENT_URL}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            result = response.json()

            if "result" in result and "content" in result["result"]:
                content = result["result"]["content"]
                if isinstance(content, list) and len(content) > 0:
                    return json.loads(content[0].get("text", "{}"))
            return {}
    except Exception as e:
        logger.warning(f"MCP Confluent call failed ({tool_name}): {e}")
        return {}


def build_diagnostic(consumer_group: str, lag_data: dict, topic_config: dict) -> dict:
    """
    Deterministic root-cause analysis: a topic left at its default
    compression.type ('producer', i.e. no broker-side compression) on a
    high-throughput topic saturates broker disk I/O and network during
    traffic spikes, which is exactly what slows down fetches for
    'facturation'. This is the actual diagnosis — no LLM required.
    """
    compression = topic_config.get("compression.type") if isinstance(topic_config, dict) else None

    if compression == "producer":
        cause = "factures n'a jamais ete retaille pour son debit reel (compression.type=producer)"
        recommended_config = {"compression.type": "lz4"}
    else:
        cause = "cause inconnue"
        recommended_config = {}

    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "incident_id": f"incident-{consumer_group}-{int(time.time() * 1000)}",
        "consumer_group": consumer_group,
        "topic": TOPIC_FACTURES,
        "cause": cause,
        "current_config": topic_config if isinstance(topic_config, dict) else {},
        "recommended_config": recommended_config,
        "lag_state": lag_data.get("state") if isinstance(lag_data, dict) else None,
        "timestamp": timestamp,
    }


def produce_incident(producer: Producer, incident: dict) -> None:
    """Publish the diagnostic to the 'incidents' topic."""
    incident_json = json.dumps(incident, ensure_ascii=False)
    producer.produce(
        TOPIC_INCIDENTS,
        key=incident.get("consumer_group"),
        value=incident_json.encode("utf-8"),
        on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None,
    )
    producer.flush(timeout=5)
    logger.info(
        f"Incident produced: consumer_group={incident.get('consumer_group')} "
        f"cause={incident.get('cause')} recommended_config={incident.get('recommended_config')}"
    )


class DiagnosticTools:
    """Tools exposed to the diagnostic ADK agent. Bound to a live Kafka producer."""

    def __init__(self, producer: Producer):
        self._producer = producer
        self.incident_produced = False

    def get_consumer_lag(self, group: str) -> dict:
        """Fetch consumer group lag/state via MCP Confluent (get-consumer-group-lag)."""
        return call_mcp("get-consumer-group-lag", {"group": group})

    def read_messages(self, topic: str, partition: int = 0, count: int = 5) -> list:
        """Read a few messages from a Kafka topic via MCP Confluent (consume-messages), to rule out a content anomaly."""
        result = call_mcp(
            "consume-messages",
            {"topic": topic, "partition": partition, "max_messages": count, "start": "end"},
        )
        if isinstance(result, dict):
            return result.get("messages", [])
        return []

    def get_topic_config(self, topic: str) -> dict:
        """SIMULATED get-topic-config — the real tool requires a Confluent Cloud endpoint, unavailable on this local Kafka."""
        return get_topic_config_simulated(topic)

    def diagnose(self, lag_data_json: str, sample_messages_json: str, topic_config_json: str) -> str:
        """
        Deterministic tool: analyzes the lag, sample messages and topic config
        already fetched by the other tools (no external call) and publishes
        the diagnostic to the 'incidents' topic. Call exactly once.
        """
        try:
            lag_data = json.loads(lag_data_json) if isinstance(lag_data_json, str) else (lag_data_json or {})
        except json.JSONDecodeError:
            lag_data = {}
        try:
            topic_config = json.loads(topic_config_json) if isinstance(topic_config_json, str) else (topic_config_json or {})
        except json.JSONDecodeError:
            topic_config = {}

        incident = build_diagnostic(FACTURATION_CONSUMER_GROUP, lag_data, topic_config)
        produce_incident(self._producer, incident)
        self.incident_produced = True
        return f"OK: diagnostic publié — cause={incident['cause']}"


def deterministic_diagnose(tools: DiagnosticTools) -> None:
    """No-LLM diagnostic path: call the same tools directly, in order."""
    logger.info("No DIAGNOSTIC_LLM_API_KEY configured — running deterministic diagnosis")
    lag_data = tools.get_consumer_lag(FACTURATION_CONSUMER_GROUP)
    tools.read_messages(TOPIC_FACTURES, 0, 5)  # sampled only to rule out a content anomaly, not used in the cause
    topic_config = tools.get_topic_config(TOPIC_FACTURES)
    incident = build_diagnostic(FACTURATION_CONSUMER_GROUP, lag_data, topic_config)
    produce_incident(tools._producer, incident)
    tools.incident_produced = True


def run_diagnosis(agent_runner: AdkAgentRunner | None, tools: DiagnosticTools) -> None:
    """Run one diagnostic pass, guaranteeing an incident always gets published."""
    tools.incident_produced = False

    if agent_runner is not None:
        user_prompt = DIAGNOSTIC_USER_PROMPT.format(
            consumer_group=FACTURATION_CONSUMER_GROUP,
            topic=TOPIC_FACTURES,
        )
        try:
            response = agent_runner.run(user_prompt)
            logger.info(f"Agent response: {response}")
        except Exception as e:
            logger.error(f"Diagnostic agent LLM call failed: {e}")

    if not tools.incident_produced:
        if agent_runner is not None:
            logger.warning("No incident produced by the agent — falling back to deterministic diagnosis")
        deterministic_diagnose(tools)


def main():
    """Main loop — waits for an 'alerts' signal (or self-triggers at startup), diagnoses, repeats on cooldown."""
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "diagnostic-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_ALERTS])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    tools = DiagnosticTools(producer)

    agent_runner = None
    if DIAGNOSTIC_LLM_API_KEY:
        agent_runner = AdkAgentRunner(
            name="diagnostic-agent",
            description="Diagnostique la cause racine d'un consumer group Kafka en retard.",
            instruction=SYSTEM_PROMPT,
            tools=[tools.get_consumer_lag, tools.read_messages, tools.get_topic_config, tools.diagnose],
            provider=DIAGNOSTIC_LLM_PROVIDER,
            model=DIAGNOSTIC_LLM_MODEL,
            api_key=DIAGNOSTIC_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={DIAGNOSTIC_LLM_PROVIDER} model={DIAGNOSTIC_LLM_MODEL}")
    else:
        logger.info("No DIAGNOSTIC_LLM_API_KEY configured — running deterministic diagnosis")

    logger.info(f"Diagnostic agent started. Listening on topic '{TOPIC_ALERTS}'")

    started_at = time.time()
    self_triggered = False
    last_diagnosis_at = 0.0

    try:
        while not shutdown_event:
            msg = consumer.poll(timeout=1.0)

            triggered = False
            if msg is not None and not msg.error():
                logger.info("Alert received on 'alerts' — triggering diagnosis")
                triggered = True
            elif msg is not None and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error(f"Kafka error: {msg.error()}")
            elif not self_triggered and (time.time() - started_at) > STARTUP_GRACE_S:
                logger.info(f"No alert received within {STARTUP_GRACE_S}s — self-triggering diagnosis")
                self_triggered = True
                triggered = True

            if triggered and (time.time() - last_diagnosis_at) > REDIAGNOSE_COOLDOWN_S:
                run_diagnosis(agent_runner, tools)
                last_diagnosis_at = time.time()

            time.sleep(1)

    except Exception as e:
        logger.exception(f"Fatal error in diagnostic loop: {e}")
    finally:
        logger.info("Closing diagnostic agent...")
        consumer.close()
        producer.flush(timeout=10)
        logger.info("Diagnostic agent stopped.")


if __name__ == "__main__":
    main()
