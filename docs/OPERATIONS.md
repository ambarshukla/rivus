# Operating rivus

How to tell whether the stack is healthy, what to do when it is not, and how to change it
without losing data. Commands are run from the repository folder. In Git Bash on Windows,
run `export MSYS_NO_PATHCONV=1` first.

## A two-minute health check

| Look at | Healthy looks like |
|---|---|
| `docker compose ps` | 18 containers up; the three Kafka brokers, ClickHouse, Redis and the Flink JobManager "healthy" |
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
| `infra/kafka/create-topics.sh` (more partitions, more copies) | nothing by itself: it runs before the collectors on every `docker compose up`, and when a topic already matches it costs one describe (a few seconds) | adding partitions to a topic a running Flink job reads: stop the jobs first (`scripts/scale-kafka.sh` shows the order), or the job finds the new partitions up to 30 s late and their first trades count as late |
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
| `TradesNotStored` | no new trade stored in ClickHouse for 3 minutes | if 1-second bars are fresh, ClickHouse's reader of the trade topics is stuck: `SELECT table, last_poll_time, last_commit_time, exceptions.text FROM system.kafka_consumers`. A restart of ClickHouse resumes it; nothing is lost while Kafka keeps the topic |
| `MinuteBarsNotStored` | no new 1-minute bar stored for 5 minutes | the same check, for `bars_1m_queue` |
| `ConsumerLagging` | a consumer is more than 20,000 messages behind | which group? `rivus-*` is Flink, `clickhouse-*` is ClickHouse. For Flink it is the position saved with each checkpoint: if bars are fresh and checkpoints complete, only the saved position is stuck, and a job restart clears it |
| `HealthQueryFailing` | the API cannot query ClickHouse | `docker compose logs --since 10m api clickhouse` |
| `TradesMissed` | a sequence gap in the last hour | expected after a dropped connection or a collector restart; clears after an hour |
| `ReconciliationDifferences` | a bar differed from the batch rebuild in the last 24 hours | the page lists each with its reason. "restart" after a deploy is expected; "unexplained" is worth an hour of your time |
| `ReconciliationStale` | no hour reconciled for 3 hours | Airflow, <http://localhost:18082>, DAG `reconcile_hourly` |

**Re-checking past hours.** An hour checked while its stream bars were held up is recorded as
"unexplained, bar missing". Once the bars are stored, check it again (raw trades are kept for
7 days), then redo the day's summary:

```sh
B="cd /opt/rivus && /home/airflow/batch-venv/bin/python -m rivus_batch"
docker compose exec -T airflow bash -c "$B all --hour 2026-11-23T17:00:00"
docker compose exec -T airflow bash -c "$B rollup --day 2026-11-23"
```

## Kafka brokers

Three brokers, `kafka`, `kafka-2` and `kafka-3`; every partition has a copy on each, and a
write is accepted once two have it. `kafka` is also the only controller (decision 32).

```sh
K="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
$K --describe --topic raw.coinbase.trades      # leader and in-sync copies of each partition
$K --describe --under-replicated-partitions   # partitions missing a copy: empty when healthy
docker compose exec -T kafka /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status   # the controller
```

| Broker lost | What happens | What to do |
|---|---|---|
| `kafka-2` or `kafka-3` | the partitions it led get a new leader within seconds; 2 copies stay in sync, so writes carry on. Measured: no restart, no delivery error, no missed trade | start it again: it catches up by itself (7 s after a 2-minute stop). Then move leadership back: `kafka-leader-election.sh --election-type preferred --all-topic-partitions` |
| two of the three | writes are refused (fewer than 2 copies in sync); collectors queue up to 200,000 messages, then reconnect in a loop | start one of them |
| `kafka` | the controller is gone: nothing can change leaders. Partitions led by the other two carry on; those it led stop | start it again; the cure is a cluster with three controllers |

**Memory.** Copying partitions onto a broker reads every topic through Linux's file cache.
On Docker Desktop that cache counts against the Windows host until it is freed: after the
first copy of 4.8 GB the Docker VM held 14.9 GB and Windows had 1.1 GB free.
`docker run --rm --privileged alpine:3.20 sh -c 'sync; echo 1 > /proc/sys/vm/drop_caches'`
gave 3 GB back. Each broker itself uses about 0.7 GB.

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
