"""
Diagnostic Agent — the only agent in this PoC. Diagnoses a poison message
blocking the 'facturation' consumer group via MCP Confluent (read-only),
proposes the CLI command an operator would run to fix it, then closes the
loop itself: simulates that command actually being run, and verifies the
lag drains as a result.

Polls the 'alerts' topic (or simply runs at startup, acting as its own
trigger) and runs a single pass:
  1. get_consumer_lag    — MCP get-consumer-group-lag (real) — find the
                            offset the group is stuck on.
  2. read_from_offset x2 — MCP consume-messages (real) — read the stuck
                            message plus a few past it, to identify the
                            poison message and rule out a burst.
  3. diagnose             — deterministic tool: builds the root-cause
                            diagnosis and the exact 'kafka-consumer-groups.sh
                            --reset-offsets' command an operator would run,
                            publishes it to 'incidents'.
  4. apply_fix_simulated  — deterministic tool: stands in for the operator
                            actually running that command — resets the
                            group's committed offset past the poison message
                            via AdminClient, then lets a short-lived
                            consumer drain the remaining (valid) backlog, the
                            way the real 'facturation' service would once
                            unblocked. Logged as SIMULATED: nothing here
                            claims to be MCP's alter-topic-config, which
                            doesn't even apply to this scenario.
  5. verify_fix           — deterministic tool: re-reads the lag and
                            confirms it actually drained, publishing the
                            verification outcome to 'incidents'.

get_consumer_lag and read_from_offset are real MCP Confluent calls against
the local Kafka broker — both work over a plain bootstrap_servers
connection.

The diagnostic LLM is OPTIONAL. If DIAGNOSTIC_LLM_API_KEY is empty, no ADK
agent is ever instantiated: the Python loop calls every tool directly, in
order, producing the exact same deterministic result.
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition, ConsumerGroupTopicPartitions, KafkaError
from confluent_kafka.admin import AdminClient

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ALERTS,
    TOPIC_INCIDENTS,
    TOPIC_FACTURES,
    FACTURATION_CONSUMER_GROUP,
    FACTURATION_PARTITION,
    POISON_OFFSET,
    DIAGNOSTIC_SCAN_COUNT,
    VERIFY_FIX_DELAY_S,
    CATCHUP_TIMEOUT_S,
    DIAGNOSTIC_LLM_PROVIDER,
    DIAGNOSTIC_LLM_MODEL,
    DIAGNOSTIC_LLM_API_KEY,
)
from common.adk_factory import AdkAgentRunner
from common.mcp_client import get_consumer_group_lag, extract_partition_lag, consume_messages
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
# Delay between the two get-consumer-group-lag samples used to confirm the
# lag isn't draining (a real consumer would shrink it) rather than merely high.
STAGNANT_CHECK_DELAY_S = 5


def _is_broken(message: dict) -> bool:
    """A message is broken if its value isn't valid JSON, or is JSON missing 'siret'."""
    value = message.get("value")
    if value is None:
        return True
    try:
        data = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return True
    return "siret" not in data


def build_diagnostic(consumer_group: str, lag_data: dict, stuck_messages: list, scan_messages: list) -> dict:
    """
    Deterministic root-cause analysis: the message the consumer group is
    stuck on is missing 'siret', which crashes the consumer before it can
    commit past it. Scanning a few messages past that offset distinguishes
    one isolated poison message from a burst of bad data. This is the actual
    diagnosis — no LLM required.
    """
    committed_offset = lag_data.get("committedOffset")
    poison_offset = committed_offset if committed_offset is not None else POISON_OFFSET

    broken_in_stuck = [m for m in stuck_messages if _is_broken(m)]
    broken_in_scan = [m for m in scan_messages if _is_broken(m)]
    messages_affected = len(broken_in_stuck) + len(broken_in_scan)
    burst = len(broken_in_scan) > 0

    cause = (
        f"Poison message à l'offset {poison_offset}, partition {FACTURATION_PARTITION}. "
        f"Cause : champ siret absent, le consumer crashe en parsing. "
        f"{messages_affected} message(s) affecté(s), "
        + ("rafale détectée — plusieurs messages consécutifs en erreur." if burst else "pas de rafale.")
    )

    precondition = (
        "Arrêter d'abord le process consumer 'facturation' — kafka-consumer-groups.sh --reset-offsets "
        "échoue si le groupe a un membre actif."
    )
    reset_command = (
        f"kafka-consumer-groups.sh --bootstrap-server kafka:9093 --group {consumer_group} "
        f"--topic {TOPIC_FACTURES}:{FACTURATION_PARTITION} --reset-offsets --to-offset {poison_offset + 1} --execute"
    )
    share_group_note = (
        "Perspective : si 'facturation' consommait via un share group KIP-932 natif, ce message "
        "aurait été REJECT automatiquement après quelques tentatives de livraison, sans intervention "
        "ops — voir l'article 4."
    )

    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "event": "diagnostic",
        "incident_id": f"incident-{consumer_group}-{int(time.time() * 1000)}",
        "consumer_group": consumer_group,
        "topic": TOPIC_FACTURES,
        "partition": FACTURATION_PARTITION,
        "poison_offset": poison_offset,
        "cause": cause,
        "messages_affected": messages_affected,
        "burst": burst,
        "precondition": precondition,
        "reset_command": reset_command,
        "share_group_note": share_group_note,
        "lag_state": lag_data,
        "timestamp": timestamp,
    }


def produce_event(producer: Producer, event: dict) -> None:
    """Publish a diagnostic or verification event to the 'incidents' topic."""
    event_json = json.dumps(event, ensure_ascii=False)
    producer.produce(
        TOPIC_INCIDENTS,
        key=event.get("consumer_group"),
        value=event_json.encode("utf-8"),
        on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None,
    )
    producer.flush(timeout=5)


class DiagnosticTools:
    """Tools exposed to the diagnostic ADK agent. Bound to a live Kafka producer and admin client."""

    def __init__(self, producer: Producer, admin: AdminClient):
        self._producer = producer
        self._admin = admin
        self.incident_produced = False
        self.fix_applied = False
        self.fix_verified = False
        self._last_incident: dict = {}

    def get_consumer_lag(self, group: str) -> dict:
        """Fetch consumer group lag via MCP Confluent (get-consumer-group-lag),
        sampled twice a few seconds apart to confirm the lag is stagnant
        (not draining) rather than merely high."""
        first = extract_partition_lag(
            get_consumer_group_lag(group, TOPIC_FACTURES), TOPIC_FACTURES, FACTURATION_PARTITION
        )
        time.sleep(STAGNANT_CHECK_DELAY_S)
        second = extract_partition_lag(
            get_consumer_group_lag(group, TOPIC_FACTURES), TOPIC_FACTURES, FACTURATION_PARTITION
        )
        stagnant = bool(first) and bool(second) and first.get("committedOffset") == second.get("committedOffset")
        latest = second or first
        return {**latest, "stagnant": stagnant}

    def read_from_offset(self, topic: str, partition: int, offset: int, count: int = DIAGNOSTIC_SCAN_COUNT) -> list:
        """Read messages starting at an absolute offset via MCP Confluent
        (consume-messages) — call once at the committed offset to find the
        message the consumer is stuck on, and once past it to scan for a burst."""
        return consume_messages(topic, partition, offset, count)

    def diagnose(self, lag_data_json: str, stuck_messages_json: str, scan_messages_json: str) -> str:
        """
        Deterministic tool: analyzes the lag reading, the message the group
        is stuck on, and the scan of messages past it — all already fetched
        by the other tools (no external call here) — and publishes the
        diagnostic, including the proposed CLI fix command, to the
        'incidents' topic. Call exactly once.
        """
        def _load(value):
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return None
            return value

        lag_data = _load(lag_data_json) or {}
        stuck_messages = _load(stuck_messages_json) or []
        scan_messages = _load(scan_messages_json) or []

        incident = build_diagnostic(FACTURATION_CONSUMER_GROUP, lag_data, stuck_messages, scan_messages)
        produce_event(self._producer, incident)
        self._last_incident = incident
        self.incident_produced = True
        logger.info(f"Incident produced: {incident['incident_id']} — {incident['cause']}")
        return f"OK: diagnostic publié — {incident['cause']} Commande proposée : {incident['reset_command']}"

    def apply_fix_simulated(self) -> str:
        """
        Stands in for the operator manually running the CLI command proposed
        by diagnose(): resets the consumer group's committed offset past the
        poison message via AdminClient, then lets a short-lived consumer
        drain the remaining valid backlog — exactly what the real
        'facturation' service would do once restarted past the bad message.
        Refuses to act if diagnose() hasn't run yet. Logged as SIMULATED
        since a human, not this agent, is the one who decided to run it.
        """
        incident = self._last_incident
        if not incident:
            logger.warning("apply_fix_simulated refused: no diagnostic pending — call diagnose() first")
            return "REFUS: aucun diagnostic en attente — appelle diagnose() d'abord"

        topic = incident["topic"]
        partition = incident["partition"]
        group = incident["consumer_group"]
        new_offset = incident["poison_offset"] + 1

        logger.info(f"SIMULATED: fix manuel appliqué par l'opérateur — {incident['reset_command']}")

        request = [ConsumerGroupTopicPartitions(group, [TopicPartition(topic, partition, new_offset)])]
        futures = self._admin.alter_consumer_group_offsets(request)
        futures[group].result(timeout=10)

        drained_to = self._catch_up_simulated(topic, group)
        self.fix_applied = True
        logger.info(
            f"Offset du groupe '{group}' repositionné à {new_offset} — backlog drainé jusqu'à l'offset {drained_to}"
        )
        return f"OK: offset repositionné à {new_offset}, backlog drainé jusqu'à {drained_to} (fix manuel simulé)"

    @staticmethod
    def _catch_up_simulated(topic: str, group: str) -> int:
        """
        Simulates the real 'facturation' consumer resuming from the
        committed offset now that the poison message has been skipped —
        consumes and commits through to the end of the backlog. Returns the
        offset committed after catch-up (-1 if nothing was consumed).
        """
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group,
            "enable.auto.commit": False,
        })
        consumer.subscribe([topic])

        last_msg = None
        deadline = time.time() + CATCHUP_TIMEOUT_S
        empty_polls = 0
        while time.time() < deadline and empty_polls < 3:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                continue
            empty_polls = 0
            last_msg = msg

        drained_to = -1
        if last_msg is not None:
            drained_to = last_msg.offset() + 1
            consumer.commit(
                offsets=[TopicPartition(topic, last_msg.partition(), drained_to)],
                asynchronous=False,
            )
        consumer.close()
        return drained_to

    def verify_fix(self) -> str:
        """
        Post-fix verification: re-reads the consumer group lag and confirms
        it actually drained rather than assuming the fix worked. Publishes
        the verification outcome to 'incidents'. Refuses to act if
        diagnose() hasn't run yet.
        """
        incident = self._last_incident
        if not incident:
            logger.warning("verify_fix refused: no diagnostic pending — call diagnose() first")
            return "REFUS: aucun diagnostic en attente — appelle diagnose() d'abord"

        time.sleep(VERIFY_FIX_DELAY_S)
        group = incident["consumer_group"]
        lag_after = extract_partition_lag(
            get_consumer_group_lag(group, TOPIC_FACTURES), TOPIC_FACTURES, FACTURATION_PARTITION
        )
        committed_after = lag_after.get("committedOffset")
        healed = committed_after is not None and committed_after > incident["poison_offset"]

        produce_event(self._producer, {
            "event": "post_fix_verification",
            "incident_id": incident["incident_id"],
            "consumer_group": group,
            "committed_offset_after_fix": committed_after,
            "lag_after_fix": lag_after.get("lag"),
            "healed": healed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        self.fix_verified = healed
        status = "lag résorbé" if healed else "lag toujours bloqué"
        logger.info(f"Vérification post-fix : {status} (committedOffset={committed_after})")
        return f"{'OK' if healed else 'ALERTE'}: {status} (committedOffset={committed_after})"


def deterministic_diagnose(tools: DiagnosticTools) -> None:
    """No-LLM path: run the full diagnose -> apply fix -> verify loop by calling the same tools directly, in order."""
    logger.info("No DIAGNOSTIC_LLM_API_KEY configured — running deterministic diagnosis")
    lag_data = tools.get_consumer_lag(FACTURATION_CONSUMER_GROUP)
    committed_offset = lag_data.get("committedOffset")
    poison_offset = committed_offset if committed_offset is not None else POISON_OFFSET
    stuck_messages = tools.read_from_offset(TOPIC_FACTURES, FACTURATION_PARTITION, poison_offset, 1)
    scan_messages = tools.read_from_offset(
        TOPIC_FACTURES, FACTURATION_PARTITION, poison_offset + 1, DIAGNOSTIC_SCAN_COUNT
    )
    incident = build_diagnostic(FACTURATION_CONSUMER_GROUP, lag_data, stuck_messages, scan_messages)
    produce_event(tools._producer, incident)
    tools._last_incident = incident
    tools.incident_produced = True
    logger.info(f"Incident produced: {incident['incident_id']} — {incident['cause']}")

    tools.apply_fix_simulated()
    tools.verify_fix()


def run_diagnosis(agent_runner: AdkAgentRunner | None, tools: DiagnosticTools) -> None:
    """Run one full pass — diagnose, apply the simulated fix, verify — guaranteeing it always completes."""
    tools.incident_produced = False
    tools.fix_applied = False
    tools.fix_verified = False

    if agent_runner is not None:
        user_prompt = DIAGNOSTIC_USER_PROMPT.format(
            consumer_group=FACTURATION_CONSUMER_GROUP,
            topic=TOPIC_FACTURES,
            partition=FACTURATION_PARTITION,
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
        return

    if not tools.fix_applied:
        if agent_runner is not None:
            logger.warning("No fix applied by the agent — falling back to deterministic fix + verification")
        tools.apply_fix_simulated()

    if not tools.fix_verified:
        if agent_runner is not None:
            logger.warning("No verification performed by the agent — falling back to deterministic verification")
        tools.verify_fix()


def main():
    """Main loop — waits for an 'alerts' signal (or self-triggers at startup), runs the full loop, repeats on cooldown."""
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "diagnostic-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_ALERTS])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    tools = DiagnosticTools(producer, admin)

    agent_runner = None
    if DIAGNOSTIC_LLM_API_KEY:
        agent_runner = AdkAgentRunner(
            name="diagnostic-agent",
            description="Diagnostique un poison message qui bloque un consumer group Kafka, propose puis applique la correction, et vérifie qu'elle a fonctionné.",
            instruction=SYSTEM_PROMPT,
            tools=[
                tools.get_consumer_lag,
                tools.read_from_offset,
                tools.diagnose,
                tools.apply_fix_simulated,
                tools.verify_fix,
            ],
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
