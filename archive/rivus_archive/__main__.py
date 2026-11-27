"""Archives the raw trade topics to Parquet files, as its own consumer group.

A second, independent reader of the same topics as the stream: it has its own group
(rivus-archive), its own position in every partition and its own lag, and Flink never
notices it. Positions are committed by hand, only once the messages before them are in
a file (see writer.py), so a crash never loses or doubles a message in the archive.

    python -m rivus_archive
"""

from __future__ import annotations

import logging
import os
import signal
import time
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from .writer import Archiver, Message, prune

log = logging.getLogger("rivus_archive")

TOPICS = [r"^raw\..*\.trades$"]  # a leading ^ makes it a pattern: every venue's raw trades
GROUP = os.environ.get("ARCHIVE_GROUP", "rivus-archive")

MESSAGES = Counter("rivus_archive_messages_total", "Messages read from Kafka")
FILES = Counter("rivus_archive_files_total", "Parquet files written")
LAST_COMMIT = Gauge("rivus_archive_last_commit_timestamp_seconds", "Unix time of the last commit")


def header(headers: list[tuple[str, bytes]] | None, name: str) -> str | None:
    for key, value in headers or []:
        if key == name and value is not None:
            return value.decode(errors="replace")
    return None


def main() -> None:
    root = Path(os.environ.get("ARCHIVE_DIR", "/archive"))
    keep_days = int(os.environ.get("ARCHIVE_KEEP_DAYS", "30"))
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            "group.id": GROUP,
            # With no saved position (the first start), take everything Kafka still has.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    archiver = Archiver(root)

    def on_revoke(c, partitions):
        # Partitions are being handed to another member of the group (or we are
        # leaving): write what we hold for them and commit, so the next owner
        # starts exactly where we stopped.
        archiver.flush_all()
        commit(c)

    def commit(c) -> None:
        ready = archiver.take_committable()
        if ready:
            offsets = [TopicPartition(t, p, o) for (t, p), o in ready.items()]
            c.commit(offsets=offsets, asynchronous=False)
            LAST_COMMIT.set(time.time())

    consumer.subscribe(TOPICS, on_revoke=on_revoke)
    start_http_server(int(os.environ.get("METRICS_PORT", "8000")))

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("archiving %s to %s as group %s", TOPICS, root, GROUP)
    last_prune = 0.0

    while running:
        for message in consumer.consume(num_messages=1000, timeout=1.0):
            if message.error():
                log.warning("kafka: %s", message.error())
                continue
            key = message.key()
            archiver.add(
                Message(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    timestamp_ms=message.timestamp()[1],
                    key=key.decode(errors="replace") if key is not None else None,
                    collector=header(message.headers(), "collector"),
                    value=message.value().decode(errors="replace"),
                ),
                time.time(),
            )
            MESSAGES.inc()
        before = archiver.files_written
        archiver.flush_old(time.time())
        FILES.inc(archiver.files_written - before)
        commit(consumer)
        if time.time() - last_prune > 3600:
            removed = prune(root, keep_days, datetime.now(UTC))
            if removed:
                log.info("deleted %d day folders older than %d days", removed, keep_days)
            last_prune = time.time()

    archiver.flush_all()
    commit(consumer)
    consumer.close()
    log.info("stopped after %d files", archiver.files_written)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()
