#!/bin/bash
# Creates every topic. One partition each, so messages keep the order they
# were written in.
#   raw.<venue>.<kind>   frames exactly as received (kind: trades, book, control)
#   rivus.<name>         normalised and derived streams written by Flink
set -euo pipefail

KT=/opt/kafka/bin/kafka-topics.sh
HOUR=3600000

create() { # topic retention_ms retention_bytes segment_ms
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions 1 --replication-factor 1 \
    --config retention.ms="$2" --config retention.bytes="$3" \
    --config segment.ms="$4" --config message.timestamp.type=CreateTime 2>&1 \
    | grep -v "could collide" || true
}

for venue in $VENUES; do
  # Trades are small and feed the hourly batch rebuild: keep 7 days.
  create "raw.$venue.trades" $((7 * 24 * HOUR)) -1 $((24 * HOUR))
  # Book messages are large (Bitstamp sends 100 levels each time): keep 6 hours, 1 GB at most.
  create "raw.$venue.book" $((6 * HOUR)) 1073741824 $HOUR
  # Heartbeats, subscription replies, status: keep 1 day.
  create "raw.$venue.control" $((24 * HOUR)) 268435456 $((6 * HOUR))
  # Normalised trades, one topic per venue so each stays in event order.
  create "rivus.trades.$venue" $((7 * 24 * HOUR)) -1 $((24 * HOUR))
done

for name in bars_1s bars_1m late_trades gaps venue_checks; do
  create "rivus.$name" $((7 * 24 * HOUR)) -1 $((24 * HOUR))
done
create "rivus.quotes" $((24 * HOUR)) 1073741824 $((6 * HOUR))

"$KT" --bootstrap-server "$BOOTSTRAP" --list
