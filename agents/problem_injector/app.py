"""
Problem Injector — One-shot script that seeds the 'facturation' problem scenario.

Creates the 'facturation' topic (if missing), then produces 500 valid invoice
messages followed by 50 invalid ones (siret=null) — simulating a legacy
producer bug. Runs once and exits; not a long-running service.
"""

import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_FACTURATION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PROBLEM-INJECTOR] %(levelname)s %(message)s",
)
logger = logging.getLogger("problem_injector")

VALID_COUNT = 500
INVALID_COUNT = 50
VALID_SIRET = "12345678901234"


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


def make_message(msg_id: int, valid: bool) -> dict:
    return {
        "id": msg_id,
        "siret": VALID_SIRET if valid else None,
        "montant": round(50 + (msg_id % 100) * 3.37, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def produce_batch(producer: Producer, start_id: int, count: int, valid: bool) -> None:
    for i in range(count):
        msg = make_message(start_id + i, valid)
        producer.produce(
            TOPIC_FACTURATION,
            key=str(msg["id"]),
            value=json.dumps(msg, ensure_ascii=False).encode("utf-8"),
            on_delivery=lambda err, m: logger.error(f"Delivery failed: {err}") if err else None,
        )
    producer.flush(timeout=10)


def main() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    ensure_topic(admin, TOPIC_FACTURATION)

    # Give the broker a moment to propagate topic metadata before producing.
    time.sleep(2)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    logger.info(f"Producing {VALID_COUNT} valid messages into '{TOPIC_FACTURATION}'...")
    produce_batch(producer, start_id=1, count=VALID_COUNT, valid=True)

    logger.info(f"Producing {INVALID_COUNT} invalid messages (siret=null) into '{TOPIC_FACTURATION}'...")
    produce_batch(producer, start_id=VALID_COUNT + 1, count=INVALID_COUNT, valid=False)

    total = VALID_COUNT + INVALID_COUNT
    logger.info(f"{total} messages dans {TOPIC_FACTURATION} (dont {INVALID_COUNT} avec siret=null)")


if __name__ == "__main__":
    main()
