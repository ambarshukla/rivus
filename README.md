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
| 2 | Process: Flink SQL bars, gaps, late events, cross-venue checks | next |
| 3 | Show: ASP.NET Core API, SignalR, web page, Grafana | planned |
| 4 | Reconcile: Airflow + dbt on DuckDB, hourly | planned |

## Run it

Needs only Docker. Nothing is installed on the host.

```sh
docker compose up -d --build        # Kafka and one collector per venue
docker compose logs -f collector-kraken
docker compose --profile test run --rm collectors-test   # lint and tests
```

Peek at what is arriving:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw.kraken.trades --from-beginning --max-messages 3
```

## Where things are

| Path | What |
|---|---|
| `compose.yaml` | every service |
| `collectors/` | the Python collector; `venues/*.yaml` is one file per exchange |
| `infra/kafka/` | topic creation |
| `docs/` | [build log](docs/BUILD_LOG.md), [decisions](docs/DECISIONS.md), [glossary](docs/GLOSSARY.md), [provenance](docs/PROVENANCE.md) |
