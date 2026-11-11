#!/bin/bash
# Creates the raw topics: raw.<venue>.<kind> for kind in trades, book, control.
# One partition each, so every venue's messages keep the order they arrived in.
set -euo pipefail

KT=/opt/kafka/bin/kafka-topics.sh
HOUR=3600000

create() { # topic retention_ms retention_bytes segment_ms
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions 1 --replication-factor 1 \
    --config retention.ms="$2" --config retention.bytes="$3" \
    --config segment.ms="$4" --config message.timestamp.type=CreateTime
}

for venue in $VENUES; do
  # Trades are small and feed the hourly batch rebuild: keep 7 days.
  create "raw.$venue.trades" $((7 * 24 * HOUR)) -1 $((24 * HOUR))
  # Book messages are large (Bitstamp sends 100 levels each time): keep 6 hours, 1 GB at most.
  create "raw.$venue.book" $((6 * HOUR)) 1073741824 $HOUR
  # Heartbeats, subscription replies, status: keep 1 day.
  create "raw.$venue.control" $((24 * HOUR)) 268435456 $((6 * HOUR))
done

"$KT" --bootstrap-server "$BOOTSTRAP" --list
