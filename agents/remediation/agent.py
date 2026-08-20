"""
Remediation Agent — Proposes a topic configuration fix for each diagnosed
incident, then executes it ONLY after an explicit human confirmation.

Two independent channels drive the two phases:
  - 'incidents' (regular consumer group) triggers the PROPOSE phase as soon
    as the diagnostic agent publishes an incident.
  - 'remediation-confirmations' (native KIP-932 ShareConsumer) triggers the
    EXECUTE phase. Using a share group here — not a plain consumer — means a
    confirmation is acquired with a broker-side lock, acknowledged exactly
    once, and automatically redelivered if this agent crashes mid-execution
    instead of silently dropping a human decision.

get_topic_config and execute_change (alter-topic-config) are SIMULATED (see
common/simulated_control_plane.py): the real MCP tool only works against a
Confluent Cloud cluster, not this local bootstrap_servers connection.

The confirmation gate is enforced in code, not just in the prompt:
execute_change refuses to act unless a matching proposal is pending.

The remediation LLM is OPTIONAL, same principle as the diagnostic agent. If
REMEDIATION_LLM_API_KEY is empty, no ADK agent is ever instantiated: every
phase goes straight through the deterministic path.
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_INCIDENTS,
    TOPIC_REMEDIATION_CONFIRMATIONS,
    TOPIC_AUDIT,
    SHARE_GROUP_LOCK_DURATION_MS,
    REMEDIATION_LLM_PROVIDER,
    REMEDIATION_LLM_MODEL,
    REMEDIATION_LLM_API_KEY,
)
from common.simulated_control_plane import get_topic_config_simulated, alter_topic_config_simulated
from common.share_group_client import ShareGroupClient, AcknowledgeType
from common.adk_factory import AdkAgentRunner
from remediation.prompts import SYSTEM_PROMPT, REMEDIATION_PROPOSE_PROMPT, REMEDIATION_EXECUTE_PROMPT

# Logging with [REMEDIATION] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [REMEDIATION] %(levelname)s %(message)s",
)
logger = logging.getLogger("remediation")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def produce_audit_log(producer: Producer, entry: dict) -> None:
    """Log a proposal or execution event to the 'audit' topic."""
    audit = {**entry, "timestamp": datetime.now(timezone.utc).isoformat()}
    producer.produce(
        TOPIC_AUDIT,
        key=str(entry.get("incident_id")),
        value=json.dumps(audit, ensure_ascii=False).encode("utf-8"),
    )
    producer.flush(timeout=5)


class RemediationTools:
    """
    Tools exposed to the remediation ADK agent.

    `pending` holds proposals awaiting confirmation, keyed by incident_id —
    the only source of truth execute_change consults before acting. A
    proposal is removed once executed or cancelled.
    """

    def __init__(self, producer: Producer):
        self._producer = producer
        self.pending: dict[str, dict] = {}
        self.proposal_recorded = False
        self.change_executed = False
        self._current_incident_id = ""

    def get_topic_config(self, topic: str) -> dict:
        """SIMULATED get-topic-config — the real tool requires a Confluent Cloud endpoint, unavailable on this local Kafka."""
        return get_topic_config_simulated(topic)

    def propose_change(self, topic: str, key: str, current_value: str, new_value: str, incident_id: str = "") -> str:
        """Record a pending proposal and log it as awaiting confirmation. Call exactly once per incident."""
        incident_id = incident_id or self._current_incident_id
        self.pending[incident_id] = {"topic": topic, "key": key, "current_value": current_value, "new_value": new_value}
        logger.info(f"PROPOSAL: {topic} {key} {current_value} -> {new_value} — en attente de confirmation (incident={incident_id})")
        produce_audit_log(self._producer, {
            "incident_id": incident_id,
            "action": "propose_change",
            "topic": topic,
            "key": key,
            "current_value": current_value,
            "new_value": new_value,
            "status": "pending_confirmation",
        })
        self.proposal_recorded = True
        return f"OK: proposition enregistree pour {topic}.{key}, en attente de confirmation"

    def execute_change(self, topic: str, key: str, new_value: str, incident_id: str = "") -> str:
        """
        Execute a previously proposed change (SIMULATED alter-topic-config).
        Refuses to act if no matching proposal is pending — the confirmation
        gate lives here, not just in the prompt.
        """
        incident_id = incident_id or self._current_incident_id
        proposal = self.pending.get(incident_id)
        if not proposal or proposal["topic"] != topic or proposal["key"] != key:
            logger.warning(f"execute_change refused: no pending proposal for incident={incident_id} topic={topic} key={key}")
            return "REFUS: aucune proposition en attente pour cet incident — execute_change annule"

        alter_topic_config_simulated(topic, key, new_value)
        produce_audit_log(self._producer, {
            "incident_id": incident_id,
            "action": "execute_change",
            "topic": topic,
            "key": key,
            "new_value": new_value,
            "status": "simulated_applied",
        })
        del self.pending[incident_id]
        self.change_executed = True
        return f"OK: {topic}.{key}={new_value} applique (simule)"


def deterministic_propose(tools: RemediationTools, incident: dict) -> None:
    """No-LLM propose path: read the config, propose the recommended change directly."""
    logger.info("No REMEDIATION_LLM_API_KEY configured — running deterministic proposal")
    topic = incident.get("topic", "")
    recommended = incident.get("recommended_config") or {}
    if not recommended:
        logger.warning(f"Incident {incident.get('incident_id')}: no recommended_config — nothing to propose")
        return
    key, new_value = next(iter(recommended.items()))
    current_config = tools.get_topic_config(topic)
    current_value = current_config.get(key, "unknown")
    tools._current_incident_id = incident.get("incident_id", "")
    tools.propose_change(topic, key, current_value, new_value, incident_id=incident.get("incident_id", ""))


def deterministic_execute(tools: RemediationTools, incident_id: str) -> None:
    """No-LLM execute path: apply the pending proposal directly."""
    logger.info("No REMEDIATION_LLM_API_KEY configured — running deterministic execution")
    proposal = tools.pending.get(incident_id)
    if not proposal:
        logger.warning(f"No pending proposal for incident={incident_id} — nothing to execute")
        return
    tools._current_incident_id = incident_id
    tools.execute_change(proposal["topic"], proposal["key"], proposal["new_value"], incident_id=incident_id)


def run_propose(agent_runner: AdkAgentRunner | None, tools: RemediationTools, incident: dict) -> None:
    """Run the PROPOSE phase for a freshly diagnosed incident, guaranteeing a proposal always gets recorded."""
    tools.proposal_recorded = False
    tools._current_incident_id = incident.get("incident_id", "")
    recommended = incident.get("recommended_config") or {}
    key, new_value = next(iter(recommended.items()), ("", ""))

    if agent_runner is not None and recommended:
        user_prompt = REMEDIATION_PROPOSE_PROMPT.format(
            incident_id=incident.get("incident_id", ""),
            consumer_group=incident.get("consumer_group", ""),
            cause=incident.get("cause", ""),
            topic=incident.get("topic", ""),
            recommended_config=json.dumps(recommended, ensure_ascii=False),
            config_key=key,
            current_value=incident.get("current_config", {}).get(key, "unknown"),
            new_value=new_value,
            timestamp=incident.get("timestamp", ""),
        )
        try:
            response = agent_runner.run(user_prompt)
            logger.info(f"Agent response: {response}")
        except Exception as e:
            logger.error(f"Remediation agent LLM call failed (propose): {e}")

    if not tools.proposal_recorded:
        if agent_runner is not None:
            logger.warning("No proposal recorded by the agent — falling back to deterministic proposal")
        deterministic_propose(tools, incident)


def run_execute(agent_runner: AdkAgentRunner | None, tools: RemediationTools, incident_id: str) -> None:
    """Run the EXECUTE phase after a human confirmation, guaranteeing the change always gets applied."""
    tools.change_executed = False
    tools._current_incident_id = incident_id
    proposal = tools.pending.get(incident_id)
    if not proposal:
        logger.warning(f"Confirmation received for unknown/already-handled incident={incident_id} — ignoring")
        return

    if agent_runner is not None:
        user_prompt = REMEDIATION_EXECUTE_PROMPT.format(
            incident_id=incident_id,
            topic=proposal["topic"],
            config_key=proposal["key"],
            new_value=proposal["new_value"],
        )
        try:
            response = agent_runner.run(user_prompt)
            logger.info(f"Agent response: {response}")
        except Exception as e:
            logger.error(f"Remediation agent LLM call failed (execute): {e}")

    if not tools.change_executed:
        if agent_runner is not None:
            logger.warning("No change executed by the agent — falling back to deterministic execution")
        deterministic_execute(tools, incident_id)


def main():
    """Main loop — proposes on 'incidents', executes on confirmed 'remediation-confirmations'."""
    incidents_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "remediation-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    incidents_consumer.subscribe([TOPIC_INCIDENTS])

    confirmations_client = ShareGroupClient(
        group_id="remediation-confirmations-group",
        topics=[TOPIC_REMEDIATION_CONFIRMATIONS],
        consumer_id="remediation-agent",
        lock_duration_ms=SHARE_GROUP_LOCK_DURATION_MS,
    )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    tools = RemediationTools(producer)

    agent_runner = None
    if REMEDIATION_LLM_API_KEY:
        agent_runner = AdkAgentRunner(
            name="remediation-agent",
            description="Propose puis exécute une correction de configuration de topic Kafka, après confirmation humaine.",
            instruction=SYSTEM_PROMPT,
            tools=[tools.get_topic_config, tools.propose_change, tools.execute_change],
            provider=REMEDIATION_LLM_PROVIDER,
            model=REMEDIATION_LLM_MODEL,
            api_key=REMEDIATION_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={REMEDIATION_LLM_PROVIDER} model={REMEDIATION_LLM_MODEL}")
    else:
        logger.info("No REMEDIATION_LLM_API_KEY configured — running deterministic propose/execute")

    confirmations_client.start()
    logger.info(
        f"Remediation agent started. Proposing on '{TOPIC_INCIDENTS}', "
        f"executing on confirmed '{TOPIC_REMEDIATION_CONFIRMATIONS}'"
    )

    try:
        while not shutdown_event:
            # ---- PROPOSE phase: react to newly diagnosed incidents ----
            msg = incidents_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                try:
                    incident = json.loads(msg.value().decode("utf-8"))
                    logger.info(
                        f"Incident received: {incident.get('incident_id')} cause={incident.get('cause')}"
                    )
                    run_propose(agent_runner, tools, incident)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"Failed to parse incident message: {e}")
            elif msg is not None and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error(f"Kafka error (incidents): {msg.error()}")

            # ---- EXECUTE phase: react to human confirmations ----
            confirmations = confirmations_client.poll(timeout=1.0)
            for confirmation in confirmations:
                try:
                    payload = json.loads(confirmation.value) if confirmation.value else {}
                except json.JSONDecodeError:
                    payload = {}

                incident_id = payload.get("incident_id", "")
                confirmed = bool(payload.get("confirm"))

                if confirmed:
                    logger.info(f"Confirmation received for incident={incident_id} — executing")
                    run_execute(agent_runner, tools, incident_id)
                else:
                    logger.info(f"Remediation cancelled by operator for incident={incident_id}")
                    tools.pending.pop(incident_id, None)

                confirmations_client.acknowledge(confirmation, AcknowledgeType.ACK)

            if confirmations:
                confirmations_client.commit_sync()

            time.sleep(0.5)

    except Exception as e:
        logger.exception(f"Fatal error in remediation loop: {e}")
    finally:
        logger.info("Closing remediation agent...")
        incidents_consumer.close()
        confirmations_client.stop()
        producer.flush(timeout=10)
        logger.info("Remediation agent stopped.")


if __name__ == "__main__":
    main()
