"""Keeps Redis up to date with the latest price per venue and pair.

Reads the normalised trades topics (one per venue) and the quotes topic. For each (venue, pair):
  HSET latest:<venue>:<pair>   last_price, bid, ask, ...   (the current state)
  PUBLISH rivus:ticks <json>                                (for live listeners)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time

import redis
from confluent_kafka import Consumer
from prometheus_client import Counter, Gauge, start_http_server

from .state import LatestState, redis_key

log = logging.getLogger("rivus_latest")

# A leading ^ makes the client treat the name as a pattern: one trades topic per venue.
TOPICS = [r"^rivus\.trades\..*", "rivus.quotes"]
CHANNEL = "rivus:ticks"
INDEX_KEY = "latest:keys"

MESSAGES = Counter("rivus_latest_messages_total", "Messages read from Kafka", ["kind"])
UPDATES = Counter("rivus_latest_updates_total", "Redis hash updates written")
LAST_WRITE = Gauge("rivus_latest_last_write_timestamp_seconds", "Unix time of the last write")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            "group.id": "rivus-latest",
            # Only the present matters here: with no saved position, start at the end.
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(TOPICS)
    store = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"), decode_responses=True)
    start_http_server(int(os.environ.get("METRICS_PORT", "8000")))
    state = LatestState()

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("writing latest prices to redis")

    while running:
        # Take whatever arrived in the last 200 ms as one batch: Redis then gets
        # at most five writes a second per key however busy the feed is.
        batch = []
        for message in consumer.consume(num_messages=1000, timeout=0.2):
            if message.error():
                log.warning("kafka: %s", message.error())
                continue
            kind = "quote" if message.topic() == "rivus.quotes" else "trade"
            MESSAGES.labels(kind).inc()
            try:
                batch.append((kind, json.loads(message.value())))
            except ValueError:
                continue
        updates = state.fold(batch)
        if not updates:
            continue
        try:
            pipe = store.pipeline(transaction=False)
            for (venue, pair), fields in updates.items():
                key = redis_key(venue, pair)
                pipe.hset(key, mapping=fields)
                pipe.sadd(INDEX_KEY, key)
            results = pipe.execute()
            # Publish the full state after the write, so listeners see whole rows.
            pipe = store.pipeline(transaction=False)
            for (venue, pair) in updates:
                pipe.hgetall(redis_key(venue, pair))
            for full in pipe.execute():
                store.publish(CHANNEL, json.dumps(full, separators=(",", ":")))
            UPDATES.inc(len(results) // 2)
            LAST_WRITE.set(time.time())
        except redis.RedisError as exc:
            log.warning("redis: %s", exc)
            time.sleep(1)

    consumer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()
