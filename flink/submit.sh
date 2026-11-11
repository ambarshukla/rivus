#!/bin/bash
# Keeps the SQL jobs running on the session cluster. Jobs do not survive a
# restart of the job manager, so this loops forever: every 30 seconds it asks
# which jobs are running and submits any that are missing.
#
# A job is resumed from its last checkpoint (its saved state and its position
# in Kafka) when one exists and the SQL has not changed since. Otherwise it
# starts with empty state from the last position it committed to Kafka.
set -uo pipefail

JM="${JOBMANAGER_HOST:-flink-jobmanager}"
REST="http://$JM:8081"
JOBS="${JOBS:-normalise process}"
SQL=/opt/flink/sql
STATE=/flink-state
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

submit() { # job name, optional checkpoint path
  local init=/tmp/init-$1.sql out
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
  sleep 30
done
