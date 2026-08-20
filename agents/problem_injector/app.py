"""
Problem Injector — One-shot script that seeds the month-end billing spike
scenario described in the article: a burst of invoices on 'factures' that
the 'facturation' consumer group falls behind on.

Creates the 'factures' topic (if missing), produces a batch of invoice
messages, then has a throwaway consumer in the 'facturation' group commit
only part of them — deliberately leaving the lag the diagnostic agent is
meant to discover (1452 messages, matching the article's incident). There is
no faulty message in this scenario: the root cause is the topic's
configuration, not its content. Runs once and exits; not a long-running
service.
"""

import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_FACTURES, FACTURATION_CONSUMER_GROUP, TARGET_LAG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PROBLEM-INJECTOR] %(levelname)s %(message)s",
)
logger = logging.getLogger("problem_injector")

# Consumed and committed right away by the 'facturation' group, so that
# TOTAL_MESSAGES - CONSUMED_BY_GROUP == TARGET_LAG (1452, the article's figure).
CONSUMED_BY_GROUP = 100
TOTAL_MESSAGES = TARGET_LAG + CONSUMED_BY_GROUP


def ensure_topic(admin: AdminClient, topic: str) -> None:
    """Create the topic if it doesn't already exist."""
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        logger.info(f"Topic '{topic}' already exists — skipping creation")
        return

    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    futures = admin.create_topics([new_topic])
    for name, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{name}' created")
        except Exception as e:
            logger.warning(f"Could not create topic '{name}' (may already exist): {e}")


def make_message(msg_id: int) -> dict:
    return {
        "id": msg_id,
        "client_id": f"CLI-{msg_id % 500:04d}",
        "montant": round(50 + (msg_id % 100) * 3.37, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def produce_batch(producer: Producer, start_id: int, count: int) -> None:
    for i in range(count):
        msg = make_message(start_id + i)
        producer.produce(
            TOPIC_FACTURES,
            key=str(msg["id"]),
            value=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            on_delivery=lambda err, m: logger.error(f"Delivery failed: {err}") if err else None,
        )
    producer.flush(timeout=10)


def create_group_lag(count_to_consume: int) -> None:
    """
    Have the 'facturation' consumer group read and commit only the first
    `count_to_consume` messages from 'factures', then disconnect — leaving
    the rest of the topic as lag for the diagnostic agent to find.
    """
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": FACTURATION_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_FACTURES])

    consumed = 0
    deadline = time.time() + 30
    last_msg = None
    while consumed < count_to_consume and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        last_msg = msg
        consumed += 1

    if last_msg is not None:
        consumer.commit(
            offsets=[TopicPartition(TOPIC_FACTURES, last_msg.partition(), last_msg.offset() + 1)],
            asynchronous=False,
        )
    consumer.close()
    logger.info(f"Consumer group '{FACTURATION_CONSUMER_GROUP}' committed {consumed} messages")


def main() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    ensure_topic(admin, TOPIC_FACTURES)

    # Give the broker a moment to propagate topic metadata before producing.
    time.sleep(2)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    logger.info(f"Simulating a month-end billing spike: producing {TOTAL_MESSAGES} messages into '{TOPIC_FACTURES}'...")
    produce_batch(producer, start_id=1, count=TOTAL_MESSAGES)

    create_group_lag(CONSUMED_BY_GROUP)

    lag = TOTAL_MESSAGES - CONSUMED_BY_GROUP
    logger.info(
        f"{TOTAL_MESSAGES} messages dans '{TOPIC_FACTURES}' — "
        f"consumer group '{FACTURATION_CONSUMER_GROUP}' a un retard de {lag} messages"
    )


if __name__ == "__main__":
    main()
