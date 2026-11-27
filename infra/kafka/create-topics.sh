#!/bin/bash
# Creates every topic, or brings an existing one up to what is written here.
# Safe to run again: it only ever adds partitions and copies, never removes.
#
#   raw.<venue>.<kind>   frames exactly as received (kind: trades, book, control)
#   rivus.<name>         normalised and derived streams written by Flink
#
# Partitions. Topics written per pair are split into 3 partitions, keyed by the
# pair, so each pair stays in order inside its partition and different pairs
# can be read in parallel. With two pairs per venue, two of the three are busy:
# a partition count only helps up to the number of distinct keys. Everything
# else has 1 partition: little volume, and order across the whole topic.
#
# Copies. Every partition is stored on 3 brokers (replication factor 3), and a
# write counts as done once 2 of them have it (min.insync.replicas=2 with the
# producers' acks=all), so one broker can be lost without losing a write.
set -euo pipefail

BIN=/opt/kafka/bin
REPLICAS="${REPLICAS:-3}"
MIN_INSYNC="${MIN_INSYNC:-2}"
BROKERS="${BROKERS:-1 2 3}"
HOUR=3600000
LIST=/tmp/topics.txt
: > "$LIST"
# Every Kafka tool call starts a Java process (several seconds), and this runs
# before the collectors on every `docker compose up`. So: describe everything
# once, and call the tools only for what differs. When nothing does, that one
# describe is all it costs.
describe() { "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe; }
DESC=$(describe)
changed=0

topic() { # name partitions retention_ms retention_bytes segment_ms
  echo "$1" >> "$LIST"
  local summary now
  summary=$(echo "$DESC" | grep -E "^Topic: $1[[:space:]]" || true)
  if [ -z "$summary" ]; then
    echo "$1: creating"
    "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$1" --partitions "$2" --replication-factor "$REPLICAS" \
      --config min.insync.replicas="$MIN_INSYNC" \
      --config retention.ms="$3" --config retention.bytes="$4" \
      --config segment.ms="$5" --config message.timestamp.type=CreateTime 2>&1 \
      | grep -v "could collide" || true
    changed=1
    return
  fi
  # An existing topic: add partitions if it has fewer. Kafka cannot remove them.
  now=$(echo "$summary" | grep -o 'PartitionCount: [0-9]*' | grep -o '[0-9]*')
  if [ "$now" -lt "$2" ]; then
    echo "$1: $now -> $2 partitions"
    "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --alter --topic "$1" --partitions "$2"
    changed=1
  fi
  if ! echo "$summary" | grep -q "min.insync.replicas=$MIN_INSYNC"; then
    "$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics \
      --entity-name "$1" --add-config min.insync.replicas="$MIN_INSYNC" > /dev/null
  fi
}

for venue in $VENUES; do
  # Trades are small and feed the hourly batch rebuild: keep 7 days.
  topic "raw.$venue.trades" 3 $((7 * 24 * HOUR)) -1 $((24 * HOUR))
  # Book messages are large (Bitstamp sends 100 levels each time): keep 6 hours, 1 GB a partition at most.
  topic "raw.$venue.book" 3 $((6 * HOUR)) 1073741824 $HOUR
  # Heartbeats, subscription replies, status: keep 1 day.
  topic "raw.$venue.control" 1 $((24 * HOUR)) 268435456 $((6 * HOUR))
  # Normalised trades, one topic per venue, keyed by pair.
  topic "rivus.trades.$venue" 3 $((7 * 24 * HOUR)) -1 $((24 * HOUR))
done

for name in bars_1s bars_1m late_trades gaps venue_checks; do
  topic "rivus.$name" 1 $((7 * 24 * HOUR)) -1 $((24 * HOUR))
done
topic "rivus.quotes" 3 $((24 * HOUR)) 1073741824 $((6 * HOUR))
# Kafka's own record of every consumer group's position.
echo "__consumer_offsets" >> "$LIST"

# ------------------------------------------------- copies onto every broker
# A topic created when there was one broker has one copy. Give every partition
# with fewer than $REPLICAS copies a full set, the preferred leader rotating
# across brokers by partition number so the leaders are spread out.
[ "$changed" = 1 ] && DESC=$(describe)
read -ra ids <<< "$BROKERS"
plan=""
while read -r line; do
  t=$(echo "$line" | grep -o 'Topic: [^[:space:]]*' | head -1 | cut -d' ' -f2)
  grep -qxF "$t" "$LIST" || continue
  p=$(echo "$line" | grep -o 'Partition: [0-9]*' | cut -d' ' -f2)
  replicas=$(echo "$line" | grep -o 'Replicas: [0-9,]*' | cut -d' ' -f2)
  [ "$(echo "$replicas" | tr ',' '\n' | wc -l)" -ge "$REPLICAS" ] && continue
  reps=""
  for i in $(seq 0 $((REPLICAS - 1))); do
    reps="$reps${reps:+,}${ids[$(( (p + i) % ${#ids[@]} ))]}"
  done
  plan="$plan${plan:+,}{\"topic\":\"$t\",\"partition\":$p,\"replicas\":[$reps]}"
done < <(echo "$DESC" | grep 'Partition:')

if [ -n "$plan" ]; then
  echo "{\"version\":1,\"partitions\":[$plan]}" > /tmp/plan.json
  echo "copying $(grep -o '"topic"' /tmp/plan.json | wc -l) partitions onto more brokers"
  "$BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" \
    --reassignment-json-file /tmp/plan.json --execute > /dev/null
  while "$BIN/kafka-reassign-partitions.sh" --bootstrap-server "$BOOTSTRAP" \
          --reassignment-json-file /tmp/plan.json --verify | grep -q "in progress"; do
    sleep 5
  done
  echo "copies complete"
  # Move leadership to each partition's preferred broker (the first in its list).
  "$BIN/kafka-leader-election.sh" --bootstrap-server "$BOOTSTRAP" \
    --election-type preferred --all-topic-partitions > /dev/null 2>&1 || true
  DESC=$(describe)
fi

echo "$DESC" | grep -E '^Topic:' | sed -E 's/\tTopicId: [^\t]*//; s/\tConfigs:.*//'
