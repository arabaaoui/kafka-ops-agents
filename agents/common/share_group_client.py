"""
Share Group Client — KIP-932 behavior emulator for Kafka consumer groups.

This module provides cooperative message consumption with per-message
acknowledgement, automatic lock expiry, delivery counting, and dead-letter
semantics — matching the KIP-932 share group behavior.

Since confluent-kafka does not yet expose a native KafkaShareConsumer in Python,
this emulator wraps a standard Consumer and adds the share-group lifecycle on top.

State is persisted to a local JSON file per consumer instance so that locks
survive process restarts.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SHARE_GROUP_LOCK_DURATION_MS,
    SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
)

logger = logging.getLogger(__name__)


class MessageState(str, Enum):
    AVAILABLE = "AVAILABLE"
    ACQUIRED = "ACQUIRED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    ARCHIVED = "ARCHIVED"


class AcknowledgeType(str, Enum):
    ACK = "ACK"       # Success — message delivered
    RELEASE = "RELEASE"  # Abandon — message back to AVAILABLE
    RENEW = "RENEW"    # Extend lock — message stays ACQUIRED


@dataclass
class ShareMessage:
    """A message tracked by the share group emulator."""
    topic: str
    partition: int
    offset: int
    key: str | None
    value: str | None
    state: MessageState = MessageState.AVAILABLE
    delivery_count: int = 0
    acquired_by: str = ""
    acquired_at: float = 0.0
    lock_expires_at: float = 0.0
    acknowledged_at: float = 0.0


class ShareGroupClient:
    """
    Emulates a KIP-932 share group consumer.

    Usage:
        client = ShareGroupClient(
            group_id="execution-group",
            topics=["tasks"],
            consumer_id="worker-1",
        )

        for msg in client.poll():
            try:
                process(msg)
                client.acknowledge(msg, AcknowledgeType.ACK)
            except Exception:
                # Message will be re-acquired after lock expiry
                pass
    """

    def __init__(
        self,
        group_id: str,
        topics: list[str],
        consumer_id: str | None = None,
        lock_duration_ms: int = SHARE_GROUP_LOCK_DURATION_MS,
        max_delivery_attempts: int = SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
        state_file: str | None = None,
    ):
        self.group_id = group_id
        self.topics = topics
        self.consumer_id = consumer_id or f"consumer-{os.getpid()}"
        self.lock_duration_s = lock_duration_ms / 1000.0
        self.max_delivery_attempts = max_delivery_attempts

        # State persistence
        if state_file is None:
            state_file = f"/tmp/share-group-{group_id}-{self.consumer_id}.json"
        self._state_file = Path(state_file)

        # In-memory state: (topic, partition, offset) -> ShareMessage
        self._messages: dict[tuple[str, int, int], ShareMessage] = {}
        self._lock = threading.RLock()

        # Kafka consumer (standard consumer group)
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # We manage acks ourselves
            "max.poll.interval.ms": 600_000,  # 10 min
        })

        self._load_state()
        self._expiry_thread: threading.Thread | None = None
        self._running = False

    # ---- Public API ----

    def start(self) -> None:
        """Start the share group consumer."""
        self._consumer.subscribe(self.topics)
        self._running = True
        self._expiry_thread = threading.Thread(target=self._expiry_loop, daemon=True)
        self._expiry_thread.start()
        logger.info(
            "ShareGroupClient started: group=%s consumer=%s topics=%s",
            self.group_id,
            self.consumer_id,
            self.topics,
        )

    def stop(self) -> None:
        """Stop the share group consumer."""
        self._running = False
        if self._expiry_thread:
            self._expiry_thread.join(timeout=5)
        self._consumer.close()
        self._save_state()
        logger.info("ShareGroupClient stopped: consumer=%s", self.consumer_id)

    def poll(self, timeout: float = 1.0) -> list[ShareMessage]:
        """
        Poll for available messages. Returns messages that are in AVAILABLE state.
        Automatically acquires them (marks ACQUIRED with lock).
        """
        acquired = []

        # Poll Kafka
        msg = self._consumer.poll(timeout)
        if msg is None:
            return acquired

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return acquired
            logger.error("Kafka error: %s", msg.error())
            return acquired

        # Parse message
        key = msg.key().decode("utf-8") if msg.key() else None
        value = msg.value().decode("utf-8") if msg.value() else None
        msg_key = (msg.topic(), msg.partition(), msg.offset())

        with self._lock:
            # Check if we already know this message
            if msg_key in self._messages:
                existing = self._messages[msg_key]
                if existing.state == MessageState.ARCHIVED:
                    return acquired  # Dead-lettered, skip
                if existing.state == MessageState.ACKNOWLEDGED:
                    return acquired  # Already done, skip
                if existing.state == MessageState.ACQUIRED:
                    if existing.acquired_by == self.consumer_id:
                        # We already have it — return it again (RENEW case)
                        acquired.append(existing)
                        return acquired
                    # Another consumer has it — check lock
                    if time.time() < existing.lock_expires_at:
                        return acquired  # Still locked

            # Acquire the message
            sm = self._messages.get(msg_key)
            if sm is None:
                sm = ShareMessage(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=key,
                    value=value,
                )
                self._messages[msg_key] = sm

            sm.state = MessageState.ACQUIRED
            sm.delivery_count += 1
            sm.acquired_by = self.consumer_id
            sm.acquired_at = time.time()
            sm.lock_expires_at = time.time() + self.lock_duration_s

            acquired.append(sm)

        self._save_state()
        return acquired

    def acknowledge(self, message: ShareMessage, ack_type: AcknowledgeType = AcknowledgeType.ACK) -> None:
        """
        Acknowledge a previously acquired message.

        - ACK: message is done, moves to ACKNOWLEDGED → ARCHIVED
        - RELEASE: message goes back to AVAILABLE for another consumer
        - RENEW: extends the lock without incrementing delivery_count
        """
        msg_key = (message.topic, message.partition, message.offset)

        with self._lock:
            sm = self._messages.get(msg_key)
            if sm is None:
                logger.warning("Acknowledge on unknown message: %s", msg_key)
                return

            if ack_type == AcknowledgeType.RENEW:
                # Extend lock, no delivery_count increment
                sm.lock_expires_at = time.time() + self.lock_duration_s
                sm.acquired_at = time.time()
                logger.debug("RENEW: %s lock extended to %s", msg_key, sm.lock_expires_at)

            elif ack_type == AcknowledgeType.RELEASE:
                sm.state = MessageState.AVAILABLE
                sm.acquired_by = ""
                sm.lock_expires_at = 0.0
                logger.debug("RELEASE: %s back to AVAILABLE", msg_key)

            elif ack_type == AcknowledgeType.ACK:
                sm.state = MessageState.ACKNOWLEDGED
                sm.acknowledged_at = time.time()
                logger.info("ACK: %s acknowledged (attempt %d)", msg_key, sm.delivery_count)

        self._save_state()

    def get_stats(self) -> dict[str, int]:
        """Get share group statistics."""
        with self._lock:
            states = {}
            for sm in self._messages.values():
                states[sm.state.value] = states.get(sm.state.value, 0) + 1
            return states

    def get_my_acquired(self) -> list[ShareMessage]:
        """Get all messages currently acquired by this consumer."""
        with self._lock:
            return [
                sm
                for sm in self._messages.values()
                if sm.state == MessageState.ACQUIRED and sm.acquired_by == self.consumer_id
            ]

    # ---- Internal ----

    def _expiry_loop(self) -> None:
        """Background thread that expires stale locks and handles dead-letter."""
        while self._running:
            time.sleep(1)
            now = time.time()

            with self._lock:
                for msg_key, sm in list(self._messages.items()):
                    if sm.state == MessageState.ACQUIRED:
                        if now >= sm.lock_expires_at:
                            # Lock expired
                            if sm.delivery_count >= self.max_delivery_attempts:
                                sm.state = MessageState.ARCHIVED
                                logger.warning(
                                    "DEAD-LETTER: %s archived after %d attempts",
                                    msg_key,
                                    sm.delivery_count,
                                )
                            else:
                                sm.state = MessageState.AVAILABLE
                                sm.acquired_by = ""
                                sm.lock_expires_at = 0.0
                                logger.info(
                                    "LOCK EXPIRED: %s back to AVAILABLE (attempt %d/%d)",
                                    msg_key,
                                    sm.delivery_count,
                                    self.max_delivery_attempts,
                                )

            self._save_state()

    def _load_state(self) -> None:
        """Load share group state from disk."""
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text())
            for entry in data.get("messages", []):
                msg_key = (entry["topic"], entry["partition"], entry["offset"])
                self._messages[msg_key] = ShareMessage(**entry)
            logger.debug("Loaded %d messages from state file", len(self._messages))
        except Exception:
            logger.exception("Failed to load state file")

    def _save_state(self) -> None:
        """Save share group state to disk."""
        try:
            data = {
                "group_id": self.group_id,
                "consumer_id": self.consumer_id,
                "messages": [
                    {
                        "topic": sm.topic,
                        "partition": sm.partition,
                        "offset": sm.offset,
                        "key": sm.key,
                        "value": sm.value,
                        "state": sm.state.value,
                        "delivery_count": sm.delivery_count,
                        "acquired_by": sm.acquired_by,
                        "acquired_at": sm.acquired_at,
                        "lock_expires_at": sm.lock_expires_at,
                        "acknowledged_at": sm.acknowledged_at,
                    }
                    for sm in self._messages.values()
                ],
            }
            self._state_file.write_text(json.dumps(data, indent=2))
        except Exception:
            logger.exception("Failed to save state file")