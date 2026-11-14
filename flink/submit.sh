#!/bin/bash
# Keeps the SQL jobs running on the session cluster. Jobs do not survive a
# restart of the job manager, so this loops forever: every 30 seconds it asks
# which jobs are running and submits any that are missing.
#
# What a job starts from, in this order:
#   1. a savepoint, if the jobs were stopped on purpose before a deploy
#      (`submit.sh stop`, run by scripts/deploy-flink.sh). The SQL may have
#      changed; if the saved state does not fit the new SQL the job fails to
#      start, and is then started the third way.
#   2. its last checkpoint, if the SQL has not changed since.
#   3. empty state, from the last position it committed to Kafka. The minute
#      in flight comes out wrong; the reconciliation labels it "restart".
#
# Every start is also recorded in ClickHouse (rivus.job_starts), with the time
# the previous job is last known to have worked. The hourly reconciliation
# uses it to label differences in that period "restart".
#
#   submit.sh           the loop
#   submit.sh stop      stop every job with a savepoint and hold the loop
#   submit.sh resume    let the loop submit again without a deploy
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
SAVEPOINTS="$STATE/savepoints"
# While this file exists and is fresh the loop submits nothing: the jobs were
# stopped on purpose and a deploy is on its way. A hold older than ten minutes
# is a deploy that never came, and is ignored.
HOLD="$STATE/jobs/hold"
HOLD_SECONDS=600
# After a start from saved state: how long the job gets to have every task
# running, and how long it must then stay that way.
SETTLE_TIMEOUT="${SETTLE_TIMEOUT:-90}"
SETTLE_STEADY="${SETTLE_STEADY:-15}"
mkdir -p "$STATE/jobs" "$SAVEPOINTS"

log() { echo "$(date -u +%FT%TZ) $*"; }

overview() { # one line per job: {"jid":...,"name":...,"state":...,"tasks":{...}}
  curl -fsS "$REST/jobs/overview" 2>/dev/null | sed 's/{"jid"/\n{"jid"/g' | grep '^{"jid"'
}

live_line() { # job name -> the overview line of its live job, if any
  overview | grep "\"name\":\"rivus-$1\"" \
    | grep -E '"state":"(RUNNING|RESTARTING|CREATED|INITIALIZING)"' | head -1
}

running() { [ -n "$(live_line "$1")" ]; }

field() { grep -o "\"$1\":\"\?[^,\"}]*" | head -1 | sed 's/.*:"\?//'; } # first value of a JSON field on stdin

sql_hash() { cat "$SQL/tables.sql" "$SQL/$1.sql" | md5sum | cut -d' ' -f1; }

last_checkpoint() { # job name -> path of its newest complete checkpoint, if usable
  local jid hash
  jid=$(cat "$STATE/jobs/$1.jid" 2>/dev/null) || return 1
  hash=$(cat "$STATE/jobs/$1.sqlhash" 2>/dev/null) || return 1
  [ "$hash" = "$(sql_hash "$1")" ] || return 1
  # shellcheck disable=SC2012
  ls -dt "$STATE/checkpoints/$jid"/chk-*/_metadata 2>/dev/null | head -1 | xargs -r dirname
}

down_since() { # job name -> when its previous job last saved its state, if known
  local jid newest
  if [ -s "$STATE/jobs/$1.savepoint" ]; then
    newest="$(cat "$STATE/jobs/$1.savepoint")/_metadata"
  else
    jid=$(cat "$STATE/jobs/$1.jid" 2>/dev/null) || return 0
    # shellcheck disable=SC2012
    newest=$(ls -dt "$STATE/checkpoints/$jid"/chk-*/_metadata 2>/dev/null | head -1)
  fi
  [ -n "$newest" ] && [ -e "$newest" ] && date -u -d "@$(stat -c %Y "$newest")" '+%F %T.000'
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

settled() { # job id -> success once every task has been running, without a failure, for a while
  # Saved state that does not fit the SQL is only noticed when a task loads it:
  # the job is accepted, then fails and restarts for ever. So watch it.
  local deadline=$(( $(date +%s) + SETTLE_TIMEOUT )) steady_since="" line
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if curl -fsS "$REST/jobs/$1/exceptions" 2>/dev/null | grep -q '"entries":\[{'; then
      curl -fsS "$REST/jobs/$1/exceptions" 2>/dev/null | grep -o '"exceptionName":"[^"]*"' | head -3
      return 1
    fi
    line=$(overview | grep "\"jid\":\"$1\"")
    if echo "$line" | grep -q '"state":"RUNNING"' \
       && [ "$(echo "$line" | field running)" = "$(echo "$line" | field total)" ]; then
      [ -z "$steady_since" ] && steady_since=$(date +%s)
      [ $(( $(date +%s) - steady_since )) -ge "$SETTLE_STEADY" ] && return 0
    else
      steady_since=""
    fi
    sleep 3
  done
  return 1
}

cancel() { # job id: cancel it and wait until it is gone
  curl -fsS -o /dev/null -X PATCH "$REST/jobs/$1?mode=cancel" 2>/dev/null
  for _ in $(seq 20); do
    overview | grep "\"jid\":\"$1\"" | grep -Eq '"state":"(CANCELED|FAILED|FINISHED)"' && return 0
    sleep 2
  done
}

submit() { # job name, optional path of saved state (a checkpoint or a savepoint)
  local init=/tmp/init-$1.sql out before jid
  before=$(down_since "$1")
  cp "$SQL/tables.sql" "$init"
  [ -n "${2:-}" ] && echo "SET 'execution.state-recovery.path' = 'file://$2';" >> "$init"
  out=$(/opt/flink/bin/sql-client.sh -Drest.address="$JM" -Drest.port=8081 -i "$init" -f "$SQL/$1.sql" 2>&1)
  jid=$(echo "$out" | grep -o 'Job ID: [0-9a-f]\{32\}' | tail -1 | cut -d' ' -f3)
  if [ -z "$jid" ]; then
    echo "$out" | grep -B2 -A12 'ERROR' | tail -40
    return 1
  fi
  if [ -n "${2:-}" ] && ! settled "$jid"; then
    log "rivus-$1 ($jid) did not come up from $2; cancelling it"
    cancel "$jid"
    return 1
  fi
  echo "$jid" > "$STATE/jobs/$1.jid"
  sql_hash "$1" > "$STATE/jobs/$1.sqlhash"
  record_start "$1" "$jid" "$(date -u '+%F %T.%3N')" "$before" "$([ -n "${2:-}" ] && echo true || echo false)"
  log "rivus-$1 submitted as $jid"
}

start() { # job name: start it from the best saved state there is
  local job=$1 savepoint checkpoint
  if [ -s "$STATE/jobs/$job.savepoint" ]; then
    savepoint=$(cat "$STATE/jobs/$job.savepoint")
    log "submitting rivus-$job from the savepoint $savepoint"
    if submit "$job" "$savepoint"; then
      rm -f "$STATE/jobs/$job.savepoint"
      # Keep this savepoint until the next one; drop the older ones.
      find "$SAVEPOINTS" -mindepth 1 -maxdepth 1 -name "savepoint-*" ! -path "$savepoint" \
           -mmin +60 -exec rm -rf {} + 2>/dev/null
      return 0
    fi
    # The savepoint was taken when the job stopped, so its Kafka position is
    # the committed one: empty state continues from exactly there.
    # The marker stays until the job is submitted: it is how the start is
    # recorded with the time the previous job stopped.
    log "the savepoint does not fit; starting rivus-$job with empty state"
    submit "$job" || log "submitting rivus-$job FAILED"
    rm -f "$STATE/jobs/$job.savepoint"
    return
  fi
  checkpoint=$(last_checkpoint "$job" || true)
  if [ -n "$checkpoint" ]; then
    log "submitting rivus-$job, resuming from $checkpoint"
    submit "$job" "$checkpoint" && return 0
    log "resume failed; starting rivus-$job with empty state"
  else
    log "submitting rivus-$job with empty state"
  fi
  submit "$job" || log "submitting rivus-$job FAILED"
}

stop_job() { # job name: stop it with a savepoint and remember where the savepoint is
  local jid request answer location
  jid=$(live_line "$1" | field jid)
  [ -n "$jid" ] || { log "rivus-$1 is not running; nothing to stop"; return 0; }
  request=$(curl -fsS -X POST -H 'Content-Type: application/json' \
            -d "{\"targetDirectory\":\"file://$SAVEPOINTS\",\"drain\":false}" \
            "$REST/jobs/$jid/stop" | field request-id)
  [ -n "$request" ] || { log "rivus-$1: the stop request was refused"; return 1; }
  for _ in $(seq 60); do
    answer=$(curl -fsS "$REST/jobs/$jid/savepoints/$request" 2>/dev/null)
    if echo "$answer" | grep -q '"id":"COMPLETED"'; then
      location=$(echo "$answer" | field location | sed 's#^file:/*#/#')
      if [ -n "$location" ] && [ -e "$location/_metadata" ]; then
        echo "$location" > "$STATE/jobs/$1.savepoint"
        log "rivus-$1 stopped; savepoint at $location"
        return 0
      fi
      log "rivus-$1: the savepoint failed: $(echo "$answer" | cut -c1-300)"
      return 1
    fi
    sleep 1
  done
  log "rivus-$1: no savepoint after 60 seconds"
  return 1
}

case "${1:-loop}" in
  stop)
    # Hold first, so the loop in the other process does not resubmit a job the
    # moment it stops. Upstream job first: what it has written is then complete
    # before the downstream job saves its position.
    touch "$HOLD"
    failed=0
    for job in $JOBS; do stop_job "$job" || failed=1; done
    if [ "$failed" = 1 ]; then
      rm -f "$HOLD"
      log "not every job could be stopped with a savepoint; the loop is released"
    fi
    exit "$failed" ;;
  resume)
    rm -f "$HOLD"
    log "hold released"
    exit 0 ;;
esac

# A new container means the deploy has arrived.
rm -f "$HOLD"

while true; do
  if [ -e "$HOLD" ] && [ $(( $(date +%s) - $(stat -c %Y "$HOLD") )) -lt "$HOLD_SECONDS" ]; then
    log "holding: the jobs were stopped for a deploy"
  elif curl -fsS "$REST/overview" > /dev/null 2>&1; then
    rm -f "$HOLD"
    for job in $JOBS; do
      running "$job" || start "$job"
    done
  else
    log "waiting for the job manager"
  fi
  send_starts
  sleep 30
done
