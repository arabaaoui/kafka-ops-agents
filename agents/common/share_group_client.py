"""
Share Group Client — thin wrapper around the native KIP-932 ShareConsumer.

confluent-kafka-python >= 2.15.0 (Preview) ships a native `ShareConsumer`
that speaks the KIP-932 share-group protocol directly with the broker —
per-record acquisition locks, ACK/RELEASE/REJECT, delivery counting and
lock-expiry redelivery all happen broker-side. This module no longer
emulates any of that; it only adapts the native client to the small
interface the agents already use (start/stop/poll/acknowledge/get_stats),
so agent code doesn't depend on confluent_kafka types directly.

IMPORTANT — no lock renewal in the Python client:
The native Java KafkaShareConsumer exposes `AcknowledgeType.RENEW` to
extend a record's acquisition lock while a long operation (e.g. an LLM
call) is in flight. The Python ShareConsumer (2.15.0 Preview) does not —
it only exposes ACCEPT / RELEASE / REJECT, and the docs state renewal
"is not yet available". There is no client-side way to push the lock
deadline back.

Mitigation used by this PoC (see agents/execution/agent.py):
    - If processing fails, RELEASE the message immediately so it's
      redelivered right away instead of waiting out the full lock.
    - If processing is expected to run close to or past the lock
      duration (e.g. a slow LLM call), raise the broker-side
      `group.share.record.lock.duration.ms` (default 30s) instead of
      trying to renew from the client — accepting slower crash recovery
      as the trade-off.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from confluent_kafka import AcknowledgeType as NativeAckType
from confluent_kafka import ShareConsumer as NativeShareConsumer

from .config import KAFKA_BOOTSTRAP_SERVERS, SHARE_GROUP_LOCK_DURATION_MS

logger = logging.getLogger(__name__)


class AcknowledgeType(str, Enum):
    """Mirrors confluent_kafka.AcknowledgeType. RENEW is kept only as a
    marker so callers get a clear error instead of an AttributeError —
    see the module docstring."""

    ACK = "ACK"          # -> AcknowledgeType.ACCEPT
    RELEASE = "RELEASE"  # -> AcknowledgeType.RELEASE
    REJECT = "REJECT"    # -> AcknowledgeType.REJECT
    RENEW = "RENEW"       # not supported by the native Python client


_ACK_MAP = {
    AcknowledgeType.ACK: NativeAckType.ACCEPT,
    AcknowledgeType.RELEASE: NativeAckType.RELEASE,
    AcknowledgeType.REJECT: NativeAckType.REJECT,
}


@dataclass
class ShareMessage:
    """Plain-data view of a polled record, decoupled from confluent_kafka.Message."""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: str | None
    delivery_count: int
    raw: Any  # the underlying confluent_kafka.Message, needed by acknowledge()


class ShareGroupClient:
    """
    Wraps confluent_kafka.ShareConsumer (KIP-932, native, explicit ack mode).

    Usage:
        client = ShareGroupClient(group_id="execution-group", topics=["tasks"])
        client.start()

        for msg in client.poll():
            try:
                process(msg)
                client.acknowledge(msg, AcknowledgeType.ACK)
            except Exception:
                client.acknowledge(msg, AcknowledgeType.RELEASE)
        client.commit_sync()
    """

    def __init__(
        self,
        group_id: str,
        topics: list[str],
        consumer_id: str | None = None,
        lock_duration_ms: int = SHARE_GROUP_LOCK_DURATION_MS,
    ):
        self.group_id = group_id
        self.topics = topics
        self.consumer_id = consumer_id or f"consumer-{id(self)}"
        # Informative only: the lock duration is a broker/group setting
        # (group.share.record.lock.duration.ms) and is not configurable here.
        self.lock_duration_ms = lock_duration_ms

        self._consumer = NativeShareConsumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "share.acknowledgement.mode": "explicit",
        })
        self._running = False
        self._stats = {"acquired": 0, "acked": 0, "released": 0, "rejected": 0}

    # ---- Public API ----

    def start(self) -> None:
        """Start the share group consumer."""
        self._consumer.subscribe(self.topics)
        self._running = True
        logger.info(
            "ShareGroupClient started (native ShareConsumer): group=%s consumer=%s topics=%s",
            self.group_id,
            self.consumer_id,
            self.topics,
        )

    def stop(self) -> None:
        """Stop the share group consumer. Commits pending acks and leaves the group."""
        self._running = False
        self._consumer.close()
        logger.info("ShareGroupClient stopped: consumer=%s", self.consumer_id)

    def poll(self, timeout: float = 1.0) -> list[ShareMessage]:
        """
        Poll a batch of acquired messages from the share group.

        In explicit ack mode, the broker requires every message from the
        previous poll() to be acknowledged before the next poll() call —
        callers must acknowledge (ACK/RELEASE/REJECT) each returned
        message before polling again.
        """
        batch = self._consumer.poll(timeout)
        if batch is None or batch.is_empty():
            return []

        messages = []
        for msg in batch:
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else None
            messages.append(
                ShareMessage(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=key,
                    value=value,
                    delivery_count=msg.delivery_count(),
                    raw=msg,
                )
            )
            self._stats["acquired"] += 1
        return messages

    def acknowledge(self, message: ShareMessage, ack_type: AcknowledgeType = AcknowledgeType.ACK) -> None:
        """
        Acknowledge a previously polled message (local state only — sent to
        the broker on the next poll(), commit_sync() or commit_async()).

        - ACK: record accepted, won't be redelivered
        - RELEASE: record released back to the share group for immediate redelivery
        - REJECT: record rejected (broker-side dead-letter/skip semantics)
        - RENEW: not supported — raises, see module docstring
        """
        if ack_type == AcknowledgeType.RENEW:
            raise NotImplementedError(
                "AcknowledgeType.RENEW is not available in the native Python ShareConsumer "
                "(confluent-kafka 2.15.0 Preview). Keep processing under the broker's "
                "group.share.record.lock.duration.ms, or raise that broker setting instead."
            )

        self._consumer.acknowledge(message.raw, _ACK_MAP[ack_type])

        if ack_type == AcknowledgeType.ACK:
            self._stats["acked"] += 1
        elif ack_type == AcknowledgeType.RELEASE:
            self._stats["released"] += 1
        elif ack_type == AcknowledgeType.REJECT:
            self._stats["rejected"] += 1

    def commit_sync(self) -> None:
        """Flush pending acknowledgements to the broker."""
        self._consumer.commit_sync()

    def get_stats(self) -> dict[str, int]:
        """Get share group statistics for this consumer instance."""
        return dict(self._stats)
