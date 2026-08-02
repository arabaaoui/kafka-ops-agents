"""
Replay Agent — Consumes 'replay-tasks' via KIP-932 share groups and replays
the faulty 'facturation' messages, enriching them where possible.

Uses ShareGroupClient (KIP-932 emulator, same as PoC 1): tasks are locked
when acquired, acknowledged on completion, and automatically retried on
crash (lock expiry). Designed to run with multiple replicas cooperatively
consuming the same 'replay-tasks' topic.

The replay LLM is OPTIONAL. If REPLAY_LLM_API_KEY is empty, no ADK agent is
ever instantiated: enrichment is fully deterministic Python (simulated
success rate), same principle as the Execution agent in PoC 1.
"""

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_REPLAY_TASKS,
    TOPIC_FACTURATION,
    TOPIC_FACTURATION_CORRIGE,
    TOPIC_FACTURATION_DEAD_LETTER,
    ENRICHMENT_SUCCESS_RATE,
    SHARE_GROUP_LOCK_DURATION_MS,
    SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
    REPLAY_LLM_PROVIDER,
    REPLAY_LLM_MODEL,
    REPLAY_LLM_API_KEY,
)
from common.share_group_client import ShareGroupClient, AcknowledgeType
from common.adk_factory import AdkAgentRunner
from replay.prompts import SYSTEM_PROMPT, REPLAY_USER_PROMPT

# Logging with [REPLAY] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [REPLAY] %(levelname)s %(message)s",
)
logger = logging.getLogger("replay")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

MAX_ENRICHMENT_ATTEMPTS = 3
SOURCE_READ_BUDGET_S = 15


def find_faulty_messages(source_consumer: Consumer, topic: str, count: int) -> list[dict]:
    """Scan `topic` from the beginning and return up to `count` messages with siret=null."""
    faulty = []
    deadline = time.time() + SOURCE_READ_BUDGET_S

    while len(faulty) < count and time.time() < deadline:
        msg = source_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        try:
            value = msg.value()
            if value is None:
                continue
            payload = json.loads(value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if payload.get("siret") is None:
            faulty.append(payload)

    return faulty


def deterministic_enrich(message: dict) -> dict | None:
    """No-LLM enrichment: simulated success rate, no randomness beyond that ratio."""
    if random.random() > ENRICHMENT_SUCCESS_RATE:
        return None
    enriched = dict(message)
    enriched["siret"] = f"{abs(hash(('siret', message.get('id')))) % 10**14:014d}"
    enriched["enriched"] = True
    return enriched


def produce_corrected(producer: Producer, message: dict) -> None:
    """Publish an enriched message to 'facturation-corrige'."""
    producer.produce(
        TOPIC_FACTURATION_CORRIGE,
        key=str(message.get("id")),
        value=json.dumps(message, ensure_ascii=False).encode("utf-8"),
    )
    logger.info(f"Message {message.get('id')} enrichi avec succès et republié dans '{TOPIC_FACTURATION_CORRIGE}'")


def produce_dead_letter(producer: Producer, message: dict, incident_id: str | None) -> None:
    """Publish a message that could not be enriched after all attempts to the dead-letter topic."""
    dead = {**message, "incident_id": incident_id, "dead_lettered_at": datetime.now(timezone.utc).isoformat()}
    producer.produce(
        TOPIC_FACTURATION_DEAD_LETTER,
        key=str(message.get("id")),
        value=json.dumps(dead, ensure_ascii=False).encode("utf-8"),
    )
    logger.warning(f"Message {message.get('id')}: DEAD-LETTER après {MAX_ENRICHMENT_ATTEMPTS} tentatives d'enrichissement")


class ReplayTools:
    """Tools exposed to the replay ADK agent (only used when an LLM is configured)."""

    def __init__(self, producer: Producer):
        self._producer = producer
        self.last_enriched: dict | None = None
        self.published = False

    def enrich_message(self, message_json: str) -> dict:
        """Attempt to recover the missing siret for a faulty message. Simulated success rate."""
        message = json.loads(message_json) if isinstance(message_json, str) else message_json
        enriched = deterministic_enrich(message)
        self.last_enriched = enriched
        return enriched or {}

    def publish_corrected(self, message_json: str) -> str:
        """Publish the enriched message to 'facturation-corrige'."""
        message = json.loads(message_json) if isinstance(message_json, str) else message_json
        produce_corrected(self._producer, message)
        self.published = True
        return "OK: corrected message published"


def run_with_llm(agent_runner: AdkAgentRunner, tools: ReplayTools, message: dict, attempt: int) -> bool:
    """Delegate one enrichment attempt to the ADK agent. Returns True if the message was published."""
    tools.last_enriched = None
    tools.published = False
    message_json = json.dumps(message, ensure_ascii=False)
    user_prompt = REPLAY_USER_PROMPT.format(message_json=message_json, attempt=attempt)
    try:
        response = agent_runner.run(user_prompt)
        logger.info(f"Agent response: {response}")
    except Exception as e:
        logger.error(f"Replay LLM call failed for message {message.get('id')}: {e} — falling back to deterministic enrichment")
        enriched = deterministic_enrich(message)
        if enriched:
            produce_corrected(tools._producer, enriched)
            return True
        return False

    return tools.published


def process_faulty_message(
    message: dict,
    incident_id: str | None,
    producer: Producer,
    agent_runner: AdkAgentRunner | None,
    tools: ReplayTools | None,
) -> bool:
    """Try up to MAX_ENRICHMENT_ATTEMPTS times to enrich and publish a faulty message."""
    for attempt in range(1, MAX_ENRICHMENT_ATTEMPTS + 1):
        if agent_runner is not None:
            success = run_with_llm(agent_runner, tools, message, attempt)
        else:
            enriched = deterministic_enrich(message)
            success = enriched is not None
            if success:
                produce_corrected(producer, enriched)

        if success:
            return True

        logger.info(f"Message {message.get('id')}: échec enrichissement, tentative {attempt}/{MAX_ENRICHMENT_ATTEMPTS}")

    produce_dead_letter(producer, message, incident_id)
    return False


def process_task(
    client: ShareGroupClient,
    task_msg,
    source_consumer: Consumer,
    producer: Producer,
    agent_runner: AdkAgentRunner | None,
    tools: ReplayTools | None,
) -> None:
    """Process a single replay task: replay every faulty message it references."""
    try:
        value = task_msg.value
        if value is None:
            client.acknowledge(task_msg, AcknowledgeType.ACK)
            return

        task = json.loads(value) if isinstance(value, str) else value
        incident_id = task.get("incident_id")
        topic = task.get("topic")
        count = task.get("count", 0)

        logger.info(f"Replay task acquired: incident={incident_id} topic={topic} count={count}")

        # Extend the lock immediately: replaying `count` messages can take a while.
        client.acknowledge(task_msg, AcknowledgeType.RENEW)

        faulty_messages = find_faulty_messages(source_consumer, topic, count)
        logger.info(f"Found {len(faulty_messages)}/{count} faulty messages to replay for incident {incident_id}")

        enriched_count = 0
        dead_letter_count = 0
        for message in faulty_messages:
            if process_faulty_message(message, incident_id, producer, agent_runner, tools):
                enriched_count += 1
            else:
                dead_letter_count += 1

        producer.flush(timeout=10)
        client.acknowledge(task_msg, AcknowledgeType.ACK)
        logger.info(
            f"Replay task {incident_id}: ACK — enriched={enriched_count} dead_letter={dead_letter_count}"
        )

    except Exception as e:
        logger.error(f"Replay task failed: {e} — not acknowledging, will be retried")


def main():
    """Main loop — polls 'replay-tasks' via share group, replays faulty messages, acknowledges."""
    group_id = os.getenv("SHARE_GROUP", "replay-group")
    consumer_id = f"replayer-{os.getpid()}"

    client = ShareGroupClient(
        group_id=group_id,
        topics=[TOPIC_REPLAY_TASKS],
        consumer_id=consumer_id,
        lock_duration_ms=SHARE_GROUP_LOCK_DURATION_MS,
        max_delivery_attempts=SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
    )

    source_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"replay-source-reader-{consumer_id}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    agent_runner = None
    tools = None
    if REPLAY_LLM_API_KEY:
        tools = ReplayTools(producer)
        agent_runner = AdkAgentRunner(
            name="replay-agent",
            description="Enrichit et republie les messages fautifs identifiés par un incident de facturation.",
            instruction=SYSTEM_PROMPT,
            tools=[tools.enrich_message, tools.publish_corrected],
            provider=REPLAY_LLM_PROVIDER,
            model=REPLAY_LLM_MODEL,
            api_key=REPLAY_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={REPLAY_LLM_PROVIDER} model={REPLAY_LLM_MODEL}")
    else:
        logger.info(f"No REPLAY_LLM_API_KEY configured — running deterministic enrichment (success_rate={ENRICHMENT_SUCCESS_RATE})")

    client.start()
    # Every replay task in this PoC references 'facturation' — subscribe once.
    source_consumer.subscribe([TOPIC_FACTURATION])
    logger.info(f"Replay agent started: consumer_id={consumer_id} group={group_id}")

    try:
        while not shutdown_event:
            messages = client.poll(timeout=1.0)

            for msg in messages:
                process_task(client, msg, source_consumer, producer, agent_runner, tools)

            time.sleep(0.5)

    except Exception as e:
        logger.exception(f"Fatal error in replay loop: {e}")
    finally:
        logger.info("Closing replay agent...")
        client.stop()
        source_consumer.close()
        producer.flush(timeout=10)
        logger.info("Replay agent stopped.")


if __name__ == "__main__":
    main()
