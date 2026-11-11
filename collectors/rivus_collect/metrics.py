"""Prometheus metrics exposed by every collector on /metrics."""

from prometheus_client import Counter, Gauge

MESSAGES = Counter(
    "rivus_collector_messages_total", "Raw messages received", ["venue", "kind"]
)
BYTES = Counter("rivus_collector_bytes_total", "Raw bytes received", ["venue", "kind"])
LAST_MESSAGE = Gauge(
    "rivus_collector_last_message_timestamp_seconds",
    "Unix time the last message of this kind arrived",
    ["venue", "kind"],
)
CONNECTED = Gauge("rivus_collector_connected", "1 while the WebSocket is open", ["venue"])
CONNECTS = Counter("rivus_collector_connects_total", "WebSocket connections opened", ["venue"])
DISCONNECTS = Counter(
    "rivus_collector_disconnects_total", "WebSocket sessions ended", ["venue", "reason"]
)
DELIVERY_ERRORS = Counter(
    "rivus_collector_kafka_delivery_errors_total", "Messages Kafka did not accept", ["venue"]
)
