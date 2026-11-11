#!/bin/bash
# Runs the real job files (sql/normalise.sql, sql/process.sql) against fixture
# files, inside this container with no cluster and no Kafka, and compares what
# they write with tests/expected/. Pass "update" to rewrite the expected files.
set -uo pipefail

T=/opt/flink/tests
rm -rf /tmp/t && mkdir -p /tmp/t/in /tmp/t/out
for f in "$T"/fixtures/raw/*.jsonl; do
  name=$(basename "$f" .jsonl)
  mkdir -p "/tmp/t/in/$name" && cp "$f" "/tmp/t/in/$name/"
done
mkdir -p /tmp/t/in/trades && cp "$T"/fixtures/trades/*.jsonl /tmp/t/in/trades/

for job in normalise process; do
  echo "running $job.sql"
  if ! /opt/flink/bin/sql-client.sh -i "$T/tables_test.sql" -f "/opt/flink/sql/$job.sql" > "/tmp/t/$job.log" 2>&1 \
     || grep -q '\[ERROR\]' "/tmp/t/$job.log"; then
    grep -B3 -A15 'ERROR' "/tmp/t/$job.log" | head -60
    echo "FAIL: $job.sql did not run"
    exit 1
  fi
done

fail=0
for dir in /tmp/t/out/*/; do
  name=$(basename "$dir")
  # Collect every part file (finished or not), drop the two fields that depend
  # on the wall clock or on the order windows happen to fire in, and sort.
  find "$dir" -type f ! -name '_*' -exec cat {} + \
    | sed -E 's/,"detected_at":"[^"]*"//; s/,"detail":"[^"]*"//' | sort > "/tmp/t/$name.actual"
  if [ "${1:-}" = "update" ]; then
    cp "/tmp/t/$name.actual" "$T/expected/$name.jsonl"
  elif diff -u "$T/expected/$name.jsonl" "/tmp/t/$name.actual" > "/tmp/t/$name.diff" 2>&1; then
    echo "ok    $name ($(wc -l < "/tmp/t/$name.actual") rows)"
  else
    echo "FAIL  $name"; head -30 "/tmp/t/$name.diff"; fail=1
  fi
done
for expected in "$T"/expected/*.jsonl; do
  [ -f "/tmp/t/$(basename "$expected" .jsonl).actual" ] || { echo "FAIL  no output for $(basename "$expected")"; fail=1; }
done
[ "$fail" = 0 ] && echo "all SQL tests passed"
exit $fail
