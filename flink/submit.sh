#!/bin/bash
# Keeps the SQL jobs running on the session cluster. Jobs do not survive a
# restart of the job manager, so this loops forever: every 30 seconds it asks
# which jobs are running and submits any that are missing.
#
# A job is resumed from its last checkpoint (its saved state and its position
# in Kafka) when one exists and the SQL has not changed since. Otherwise it
# starts with empty state from the last position it committed to Kafka.
#
# Every start is also recorded in ClickHouse (rivus.job_starts), with the time
# the previous job is last known to have worked. The hourly reconciliation
# uses it to label differences in that period "restart".
set -uo pipefail

JM="${JOBMANAGER_HOST:-flink-jobmanager}"
REST="http://$JM:8081"
JOBS="${JOBS:-normalise process}"
SQL=/opt/flink/sql
STATE=/flink-state
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://clickhouse:8123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-rivus}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-rivus}"
PENDING="$STATE/jobs/starts.pending"
mkdir -p "$STATE/jobs"

log() { echo "$(date -u +%FT%TZ) $*"; }

running() { # job name -> success if a job of that name is alive
  curl -fsS "$REST/jobs/overview" 2>/dev/null \
    | tr '{' '\n' | grep "\"name\":\"rivus-$1\"" \
    | grep -Eq '"state":"(RUNNING|RESTARTING|CREATED|INITIALIZING)"'
}

sql_hash() { cat "$SQL/tables.sql" "$SQL/$1.sql" | md5sum | cut -d' ' -f1; }

last_checkpoint() { # job name -> path of its newest complete checkpoint, if usable
  local jid hash
  jid=$(cat "$STATE/jobs/$1.jid" 2>/dev/null) || return 1
  hash=$(cat "$STATE/jobs/$1.sqlhash" 2>/dev/null) || return 1
  [ "$hash" = "$(sql_hash "$1")" ] || return 1
  # shellcheck disable=SC2012
  ls -dt "$STATE/checkpoints/$jid"/chk-*/_metadata 2>/dev/null | head -1 | xargs -r dirname
}

down_since() { # job name -> when its previous job last saved a checkpoint, if known
  local jid newest
  jid=$(cat "$STATE/jobs/$1.jid" 2>/dev/null) || return 0
  # shellcheck disable=SC2012
  newest=$(ls -dt "$STATE/checkpoints/$jid"/chk-*/_metadata 2>/dev/null | head -1)
  [ -n "$newest" ] && date -u -d "@$(stat -c %Y "$newest")" '+%F %T.000'
}

record_start() { # job name, job id, started at, down since (may be empty), true|false resumed
  # Written to a file first: ClickHouse may not be up yet when a job starts.
  echo "{\"job\":\"$1\",\"job_id\":\"$2\",\"started_at\":\"$3\",\"down_since\":\"${4:-$3}\",\"resumed\":$5}" >> "$PENDING"
}

send_starts() { # hand the recorded starts to ClickHouse; keep them if it is not there
  [ -s "$PENDING" ] || return 0
  curl -fsS -o /dev/null -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
       --data-binary "@$PENDING" \
       "$CLICKHOUSE_URL/?query=INSERT%20INTO%20rivus.job_starts%20FORMAT%20JSONEachRow" 2>/dev/null \
    && rm -f "$PENDING"
}

submit() { # job name, optional checkpoint path
  local init=/tmp/init-$1.sql out before
  before=$(down_since "$1")
  cp "$SQL/tables.sql" "$init"
  [ -n "${2:-}" ] && echo "SET 'execution.state-recovery.path' = 'file://$2';" >> "$init"
  out=$(/opt/flink/bin/sql-client.sh -Drest.address="$JM" -Drest.port=8081 -i "$init" -f "$SQL/$1.sql" 2>&1)
  local jid
  jid=$(echo "$out" | grep -o 'Job ID: [0-9a-f]\{32\}' | tail -1 | cut -d' ' -f3)
  if [ -z "$jid" ]; then
    echo "$out" | grep -B2 -A12 'ERROR' | tail -40
    return 1
  fi
  echo "$jid" > "$STATE/jobs/$1.jid"
  sql_hash "$1" > "$STATE/jobs/$1.sqlhash"
  record_start "$1" "$jid" "$(date -u '+%F %T.%3N')" "$before" "$([ -n "${2:-}" ] && echo true || echo false)"
  log "rivus-$1 submitted as $jid"
}

while true; do
  if curl -fsS "$REST/overview" > /dev/null 2>&1; then
    for job in $JOBS; do
      running "$job" && continue
      checkpoint=$(last_checkpoint "$job" || true)
      if [ -n "$checkpoint" ]; then
        log "submitting rivus-$job, resuming from $checkpoint"
        submit "$job" "$checkpoint" && continue
        log "resume failed; starting rivus-$job with empty state"
      else
        log "submitting rivus-$job with empty state"
      fi
      submit "$job" || log "submitting rivus-$job FAILED"
    done
  else
    log "waiting for the job manager"
  fi
  send_starts
  sleep 30
done
