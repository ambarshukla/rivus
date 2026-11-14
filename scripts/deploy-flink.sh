#!/bin/bash
# Deploy a change to flink/ without spoiling the minute in flight.
#
# A plain rebuild restarts the job manager, the jobs die with it, and a job
# whose SQL changed then starts with empty state. This stops the jobs first,
# each with a savepoint (its state and its position in Kafka, saved at the
# moment it stops), restarts, and lets flink/submit.sh start them from those
# savepoints. If the saved state does not fit the new SQL, submit.sh notices
# and falls back to empty state: never worse than a plain rebuild.
#
#   bash scripts/deploy-flink.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1 BUILDX_NO_DEFAULT_ATTESTATIONS=1

echo "== building first, so the jobs are down for as short a time as possible"
docker compose build

echo "== stopping the jobs with savepoints"
started=$(date -u +%FT%TZ)
docker compose exec -T flink-submit /opt/flink/submit.sh stop \
  || echo "== no savepoints; carrying on as a plain restart"

echo "== restarting what changed"
docker compose up -d
# If nothing in flink/ changed, the submit container was not replaced and is
# still holding: release it. Harmless when it is a new container.
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
