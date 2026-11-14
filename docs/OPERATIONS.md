# Operating rivus

How to tell whether the stack is healthy, what to do when it is not, and how to change it
without losing data. Commands are run from the repository folder. In Git Bash on Windows,
run `export MSYS_NO_PATHCONV=1` first.

## A two-minute health check

| Look at | Healthy looks like |
|---|---|
| `docker compose ps` | 15 containers up; Kafka, ClickHouse, Redis and the Flink JobManager "healthy" |
| The page, <http://localhost:18080>, "Stream health" | every venue's newest trade a few seconds old; bar delay about 7 s |
| Prometheus, <http://localhost:19090/alerts> | nothing firing (see the table below for the ones that fire for known reasons) |
| Flink, <http://localhost:18081> | two jobs RUNNING: `rivus-normalise` and `rivus-process` |
| The page, "Hourly reconciliation" | the newest hour is the last completed one (it appears at about ten past) and "Differences" is 0 |

The two Flink jobs never finish. A streaming job reads an endless input, so "Completed
Jobs" stays empty and the running time is simply the time since the jobs were last
started. `SELECT * FROM rivus.job_starts FINAL ORDER BY started_at` in ClickHouse lists
every start, with whether the job resumed from a checkpoint.

The same check from a terminal:

```sh
docker compose ps
CH="docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus"
$CH < scripts/counts.sql        # rows per table and venue
$CH < scripts/check_bars.sql    # every stored bar against a rebuild from the stored trades
$CH -q "SELECT hour_start, sum(stream_bars), sum(matched), sum(differences)
        FROM rivus.recon_runs FINAL GROUP BY hour_start ORDER BY hour_start DESC LIMIT 6"
```

## Start, stop, rebuild

```sh
docker compose up -d      # start; safe to repeat
docker compose stop       # stop; data stays in the volumes
BUILDX_NO_DEFAULT_ATTESTATIONS=1 docker compose up -d --build   # after changing code
bash scripts/deploy-flink.sh   # after changing anything in flink/: keeps the jobs' state
```

- **Stopping loses trades.** The venues do not replay what was sent while nobody was
  listening. Everything else catches up: Flink resumes from its last checkpoint, ClickHouse
  from its position in Kafka, Airflow reconciles the newest hour.
- **Always rebuild with the variable.** Without it every image gets a new id and every
  container restarts, the collectors included.
- **`docker compose down -v` deletes all data.** `down` without `-v` keeps it.

## What a change costs

| You change | What restarts | What it costs |
|---|---|---|
| `collectors/` or a venue file | that venue's collector | the trades sent during the reconnect, a second or two; shown as a gap |
| `flink/sql/*.sql`, deployed with `scripts/deploy-flink.sh` | both jobs, from a savepoint | about 90 seconds of bars arrive late; none is wrong. If the saved state does not fit the new SQL the script says so and the next row applies |
| `flink/sql/*.sql`, deployed with a plain rebuild | the changed job, **with empty state** | the minute in flight comes out wrong; the reconciliation labels it "restart" |
| Nothing, but Flink restarts | both jobs, resumed from the last checkpoint | nothing |
| `api/` | the API | the page reconnects by itself |
| `batch/` | Airflow | nothing, unless an hourly run is in progress (ten past the hour, about 15 s) |
| `clickhouse/init/*.sql` | nothing: these files only run on an empty volume | apply the new statement by hand (below) |
| `monitoring/alerts.yml` | Prometheus | nothing |

Applying a ClickHouse change by hand:

```sh
# a new table: every statement is IF NOT EXISTS, so the whole file can be run again
docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus --multiquery \
  < clickhouse/init/03_job_starts.sql

# a setting of a Kafka table (they cannot be altered): drop the view and the queue, then run
# the file again. The position is kept by Kafka, so nothing is lost. ($CH as defined above.)
$CH --multiquery -q "DROP VIEW rivus.trades_mv; DROP TABLE rivus.trades_queue"
docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus --multiquery \
  < clickhouse/init/01_schema.sql
```

## The alerts

Rules are in `monitoring/alerts.yml`. They are shown in Prometheus and Grafana and sent
nowhere (decision 25).

| Alert | Means | First thing to do |
|---|---|---|
| `TargetDown` | a service does not answer Prometheus | `docker compose ps`, then that service's logs |
| `CollectorDisconnected` | a collector has had no connection for 2 minutes | `docker compose logs --since 10m collector-<venue>`; it retries by itself, so check the network and the venue's status page |
| `VenueQuiet` | connected, but no trade for 15 minutes | usually the venue; compare with its own website |
| `StreamJobMissing` | fewer than 2 Flink jobs | `docker compose logs --since 10m flink-submit`: it resubmits every 30 s and says why it cannot |
| `StreamJobRestarting` | Flink restarted a job by itself | Flink UI, the job's Exceptions tab |
| `CheckpointsStopped` | no completed checkpoint for 10 minutes | Flink UI, Checkpoints tab; a restart now would re-read more than usual |
| `NoNewBars` | no new 1-second bar stored for 2 minutes | walk the pipeline (next section) |
| `ConsumerLagging` | a consumer is more than 20,000 messages behind | which group? `rivus-*` is Flink, `clickhouse-*` is ClickHouse |
| `HealthQueryFailing` | the API cannot query ClickHouse | `docker compose logs --since 10m api clickhouse` |
| `TradesMissed` | a sequence gap in the last hour | expected after a dropped connection or a collector restart; clears after an hour |
| `ReconciliationDifferences` | a bar differed from the batch rebuild in the last 24 hours | the page lists each with its reason. "restart" after a deploy is expected; "unexplained" is worth an hour of your time |
| `ReconciliationStale` | no hour reconciled for 3 hours | Airflow, <http://localhost:18082>, DAG `reconcile_hourly` |

## Finding where data stops

Walk the pipeline in the order the data travels and find the first stage whose input is
healthy and whose output is not. The Grafana dashboard's rows are in the same order.

1. **Collector.** Is the venue's message rate zero? `docker compose logs --since 10m collector-<venue>`.
2. **Flink.** Is the lag of `rivus-normalise` or `rivus-process` growing? Then Flink is
   stuck or down: `flink-submit` logs and the Flink UI.
3. **ClickHouse.** Is the lag of a `clickhouse-*` group growing? Then ClickHouse is not
   reading: `SELECT * FROM system.kafka_consumers`.
4. **Everything flows but bars are late.** One silent venue can hold back the watermark;
   the Flink UI shows the watermark per task.

## Disk

Everything that grows has an expiry, so the stack levels off; the estimate from the first
three days is about 25 GB.

| What | Kept for |
|---|---|
| Kafka: raw trades, normalised trades, bars | 7 days |
| Kafka: raw best bid/offer | 6 hours or 1 GB |
| ClickHouse: trades | 30 days |
| ClickHouse: quotes, 1-second bars | 3 days, 7 days |
| ClickHouse: 1-minute bars, checks, gaps | 90 days |
| ClickHouse's own logs | 7 days |
| Batch working files | 72 hours |
| Prometheus | 15 days |

```sh
docker system df -v                       # size of every volume
docker stats --no-stream                  # memory, CPU and bytes written per container
$CH -q "SELECT table, formatReadableSize(sum(bytes_on_disk)) AS size, sum(rows) AS rows
        FROM system.parts WHERE active AND database = 'rivus' GROUP BY table ORDER BY sum(bytes_on_disk) DESC"
```

## Doing things by hand

```sh
# reconcile one hour again (it replaces that hour's earlier result)
docker compose exec airflow bash -c \
  "cd /opt/rivus && /home/airflow/batch-venv/bin/python -m rivus_batch all --hour 2026-11-11T13:00:00"

# rebuild everything derived from the raw topics, then compare with what was there
bash scripts/replay.sh

# all the tests
docker compose --profile test build
for t in collectors-test latest-test api-test batch-test flink-sql-test alerts-test; do
  docker compose --profile test run --rm $t || echo "FAILED: $t"
done
```

Raw trades stay in Kafka for 7 days: an hour older than that cannot be reconciled again.

## On a laptop

- **Sleep.** With the lid closed on mains power the stack keeps running. On battery, standby
  cuts the network and the venues' trades for that time are missed; the gap check reports
  them.
- **Clock.** Bars use the venues' timestamps, so a wrong laptop clock does not change them.
  It does change `recv_time`, the time a message was received.
- **After a restart.** Docker has to be running; the containers then start by themselves
  and Flink resumes from its checkpoints. The time the stack was off is missing data.
