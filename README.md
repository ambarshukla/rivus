# rivus

Live trades and best bid/offer from three crypto exchanges, turned into price bars in real
time, checked for problems as they flow, and rebuilt in batch every hour to prove the stream
got it right. *rivus* is Latin for "stream".

> **Independent personal project**, built on personal time and equipment. Not affiliated with
> or endorsed by any exchange named here. It reads the exchanges' public market-data feeds;
> fetched data is never committed and nothing derived from it is published. Sources and the
> status of each venue's terms: [`docs/PROVENANCE.md`](docs/PROVENANCE.md).

```
Coinbase ─┐
Kraken   ─┼─▶ collectors ─▶ Kafka ─▶ Flink SQL ─▶ ClickHouse ─▶ C# API ─▶ web page
Bitstamp ─┘   (Python)      raw      bars, gaps,   (history)     + SignalR   (live)
                            topics   late events,  Redis
                                     cross-venue   (latest price)
                                     checks
                 Airflow (hourly): export raw trades ─▶ dbt on DuckDB ─▶ rebuilt bars
                                   ─▶ compare with the stream's bars ─▶ differences table
                 Prometheus + Grafana: throughput, lag, gaps, reconciliation breaks
```

## Status

| Phase | What | State |
|---|---|---|
| 1 | Collect: Python collectors writing raw messages to Kafka | working |
| 2 | Process: Flink SQL bars, gaps, late events, cross-venue checks; ClickHouse and Redis | working |
| 3 | Show: ASP.NET Core API, SignalR, web page, Grafana | next |
| 4 | Reconcile: Airflow + dbt on DuckDB, hourly | planned |

## Run it

Needs only Docker (about 4 GB of memory for the stack). Nothing is installed on the host.

```sh
docker compose up -d --build        # first start
docker compose logs -f flink-submit # watch the two Flink jobs being submitted
```

After changing code, rebuild with `BUILDX_NO_DEFAULT_ATTESTATIONS=1 docker compose up -d --build`.
Without that variable Docker gives every image a new id on every build and restarts all
containers, and a restarted collector misses trades.

Look at it:

```sh
# Flink web UI:  http://localhost:18081
# row counts per table and venue
docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus < scripts/counts.sql
# rebuild every bar from the stored trades and compare with what the stream produced
docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus < scripts/check_bars.sql
# latest price for one venue and pair
docker compose exec redis redis-cli hgetall latest:kraken:BTC-USD
# raw messages as received
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw.kraken.trades --from-beginning --max-messages 3
```

Tests (all run in Docker):

```sh
docker compose --profile test build
docker compose --profile test run --rm collectors-test   # Python lint and tests
docker compose --profile test run --rm latest-test
docker compose --profile test run --rm flink-sql-test    # the SQL jobs against fixtures
```

Rebuild everything downstream from the raw topics (collection keeps running):

```sh
bash scripts/replay.sh
```

## Where things are

| Path | What |
|---|---|
| `compose.yaml` | every service |
| `collectors/` | the Python collector; `venues/*.yaml` is one file per exchange |
| `infra/kafka/` | topic creation |
| `flink/sql/` | `tables.sql` (every topic as a table), `normalise.sql`, `process.sql` |
| `flink/submit.sh` | keeps the jobs running and resumes them from checkpoints |
| `clickhouse/init/` | tables fed from Kafka |
| `latest/` | the Redis latest-price writer |
| `scripts/` | checks and the replay script |
| `docs/` | [build log](docs/BUILD_LOG.md), [decisions](docs/DECISIONS.md), [glossary](docs/GLOSSARY.md), [provenance](docs/PROVENANCE.md) |
