#!/bin/bash
# Throws away everything derived from the raw topics and rebuilds it by
# replaying them: the rivus.* topics, Flink's state, and the ClickHouse tables.
# The raw topics and the collectors are not touched, so nothing is lost and
# collection carries on throughout.
#
#   bash scripts/replay.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1 BUILDX_NO_DEFAULT_ATTESTATIONS=1

echo "stopping everything downstream of the raw topics"
docker compose rm -sf flink-submit flink-taskmanager flink-jobmanager clickhouse latest > /dev/null
docker volume rm -f rivus_clickhouse-data rivus_flink-state > /dev/null

echo "deleting derived topics and consumer positions"
docker compose exec -T kafka bash -c '
  B=/opt/kafka/bin; S="--bootstrap-server localhost:9092"
  for t in $($B/kafka-topics.sh $S --list | grep "^rivus\."); do
    $B/kafka-topics.sh $S --delete --topic "$t" > /dev/null
  done
  sleep 3
  for g in $($B/kafka-consumer-groups.sh $S --list | grep -E "^(rivus|clickhouse)-"); do
    $B/kafka-consumer-groups.sh $S --delete --group "$g" > /dev/null 2>&1 || true
  done'

echo "starting again"
docker compose up -d
