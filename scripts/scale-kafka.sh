#!/bin/bash
# Grow a running stack from one Kafka broker to three, in place, keeping every
# message and the stream jobs' state. Used once, on 2026-11-27; kept as the record
# of how it was done and for anyone starting from an older volume.
#
#   1. start the two new brokers next to the first one
#   2. stop the Flink jobs with savepoints, so they come back exactly where they were
#   3. kafka-init: add partitions, copy every partition onto all three brokers
#      (with the jobs stopped, they see the new partitions from their first second)
#   4. rebuild and restart what changed: collectors (keys, broker list), Flink
#      (second task manager), the batch, the latest-price writer
#   5. Flink starts the jobs from their savepoints
#
#   bash scripts/scale-kafka.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1 BUILDX_NO_DEFAULT_ATTESTATIONS=1

echo "== building first, so the jobs are down for as short a time as possible"
docker compose build

echo "== starting kafka-2 and kafka-3"
docker compose up -d --wait kafka-2 kafka-3

echo "== stopping the jobs with savepoints"
started=$(date -u +%FT%TZ)
docker compose exec -T flink-submit /opt/flink/submit.sh stop

echo "== partitions and copies (this copies every topic twice; a few minutes)"
docker compose up --force-recreate --exit-code-from kafka-init kafka-init

echo "== restarting what changed"
docker compose up -d
docker compose restart prometheus
docker compose exec -T flink-submit /opt/flink/submit.sh resume

echo "== waiting for both jobs (up to 4 minutes)"
for _ in $(seq 48); do
  sleep 5
  log=$(docker compose logs --since "$started" flink-submit 2>/dev/null | sed 's/^[^|]*| //')
  if [ "$(echo "$log" | grep -c -E 'rivus-[a-z]+ submitted as')" -ge 2 ]; then
    echo "$log" | grep -E 'savepoint|empty state|submitted as|FAILED'
    exit 0
  fi
done
echo "== the jobs are not both running: docker compose logs --since 10m flink-submit"
exit 1
