"""Where collected messages go. The Kafka value is the frame exactly as received."""

from __future__ import annotations

import logging
from typing import Protocol

from confluent_kafka import Producer

from . import metrics

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, topic: str, raw: bytes, headers: dict[str, str], timestamp_ms: int) -> None: ...

    def close(self) -> None: ...


class KafkaSink:
    """Writes to Kafka without blocking the event loop.

    produce() only puts the message in a local queue; a background thread inside
    the client sends it. poll(0) hands back delivery results.
    """

    def __init__(self, bootstrap: str, venue: str) -> None:
        self._venue = venue
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": f"collector-{venue}",
                "enable.idempotence": True,  # no duplicates or reordering on retries
                "acks": "all",
                "linger.ms": 50,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 200_000,
            }
        )

    def _delivered(self, err, _msg) -> None:
        if err is not None:
            metrics.DELIVERY_ERRORS.labels(self._venue).inc()
            log.error("kafka delivery failed: %s", err)

    def send(self, topic: str, raw: bytes, headers: dict[str, str], timestamp_ms: int) -> None:
        kwargs = {
            "value": raw,
            "headers": [(k, v.encode()) for k, v in headers.items()],
            "timestamp": timestamp_ms,
            "on_delivery": self._delivered,
        }
        try:
            self._producer.produce(topic, **kwargs)
        except BufferError:
            # Local queue full (Kafka is slow or down): wait for room once, then
            # let the error surface so the session restarts with a backoff.
            self._producer.poll(5)
            self._producer.produce(topic, **kwargs)
        self._producer.poll(0)

    def close(self) -> None:
        left = self._producer.flush(10)
        if left:
            log.warning("%d messages were not delivered before shutdown", left)
