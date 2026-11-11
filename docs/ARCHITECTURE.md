# Architecture

How the pieces fit, what each guarantees, and what happens when one fails. Terms are in the
[glossary](GLOSSARY.md); the reasons for each choice are in [DECISIONS](DECISIONS.md).

## The two paths

```
                    ┌──────────────────────── stream (seconds) ────────────────────────┐
venues ─▶ collectors ─▶ raw.<venue>.* ─▶ normalise ─▶ rivus.trades.<venue> ─▶ process ─▶ rivus.bars_*, gaps, ...
          (Python)      (Kafka)          (Flink)      rivus.quotes            (Flink)        │
                             │                             │                                 ▼
                             │                             └─▶ latest ─▶ Redis ─▶ API ◀── ClickHouse
                             │                                 (Python)          (C#)        ▲
                             │                                                    │          │
                             │                                                 browser       │
                             └──────────── batch (hourly) ──▶ dbt on DuckDB ─▶ compare ──────┘
                                           (Airflow)          rebuilt bars     recon_runs, recon_diffs
```

The stream path produces bars within about four seconds. The batch path waits until an hour
is over, rebuilds that hour's bars from the same raw messages with separately written
parsing, and records every difference. The two meet only at the raw topics and in the
comparison.

## Components

| Component | Does | State it keeps |
|---|---|---|
| `collector-<venue>` | one WebSocket per venue; copies every frame to Kafka unchanged | none |
| Kafka | holds raw and derived topics | topics (retention below) |
| Flink `rivus-normalise` | maps venue formats to one trade and one quote schema | Kafka positions |
| Flink `rivus-process` | removes duplicates; bars, late trades, gaps, venue check | open windows, trade ids seen (1 h), last id per pair |
| `flink-submit` | resubmits a missing job, from its last checkpoint if the SQL is unchanged | job ids and SQL hashes |
| ClickHouse | history; pulls from Kafka itself | tables (TTL below) |
| `latest` | newest trade and quote per venue and pair into Redis | none (Redis is rebuilt in a second) |
| `api` | REST, SignalR, the page; measures stream health for Prometheus | none |
| Airflow | runs the hourly and daily reconciliation | run history; one folder per hour |
| Prometheus, Grafana | metrics and the dashboard | 15 days of metrics |

## Topics

| Topic | Written by | Retention | Content |
|---|---|---|---|
| `raw.<venue>.trades` | collector | 7 days | trade messages as received |
| `raw.<venue>.book` | collector | 6 hours, 1 GB | best bid/offer messages as received |
| `raw.<venue>.control` | collector | 1 day | heartbeats, subscription replies, status |
| `rivus.trades.<venue>` | normalise | 7 days | trades in one schema (may contain duplicates) |
| `rivus.quotes` | normalise | 1 day | best bid/offer in one schema |
| `rivus.bars_1s`, `rivus.bars_1m` | process | 7 days | bars |
| `rivus.late_trades`, `rivus.gaps`, `rivus.venue_checks` | process | 7 days | the stream's checks |

Every topic has one partition. Raw messages carry four headers: `venue`, `conn` (connection
id), `seq` (position within the connection) and `recv_ns`; the record timestamp is the time
of receipt.

## ClickHouse tables

| Table | Key | Kept |
|---|---|---|
| `trades` | venue, pair, trade_id | 30 days |
| `quotes` | venue, pair, event_time | 3 days |
| `bars_1s` | venue, pair, window_start | 7 days |
| `bars_1m` | venue, pair, window_start | 90 days |
| `late_trades`, `gaps` | venue, pair, trade_id | 90 days |
| `venue_checks` | pair, window_start | 90 days |
| `recon_runs`, `recon_diffs`, `recon_daily` | hour or day, venue, pair (, bar, field) | not expired |

All but `quotes` are `ReplacingMergeTree`: a row delivered twice replaces itself. Queries that
must not see a duplicate before the background merge use `FINAL`.

## Time

- **Event time** is the venue's timestamp on the trade. Bars are built on it.
- Each venue's topic has its own **watermark**, 2 seconds behind that venue's newest trade.
  The job uses the lowest. A venue silent for 2 seconds stops counting.
- A trade older than the watermark is written to `late_trades`, with flags saying whether its
  1-second and 1-minute bars had already been emitted without it.
- **Gaps** are found in arrival order, not event-time order, and only for venues whose trade
  ids are consecutive per pair (Coinbase, Kraken).

## Delivery guarantees

| Hop | Guarantee | Made safe by |
|---|---|---|
| venue → collector | best effort; a disconnect loses trades | Kraken's replay on reconnect; gap detection |
| collector → Kafka | no duplicates or reordering (idempotent producer) | |
| Kafka → Flink → Kafka | at least once | duplicate removal by trade id in `process` |
| Kafka → ClickHouse | at least once | `ReplacingMergeTree` keys |
| stream vs truth | not guaranteed | the hourly reconciliation |

## What happens when something fails

| Failure | Effect | Recovery |
|---|---|---|
| Venue drops the connection, or goes silent | trades missed while disconnected | collector reconnects with backoff; Kraken replays its last 50 trades; a gap row records what was missed |
| Collector restarts | same as a dropped connection | same |
| Flink cluster restarts | results pause | jobs resume from their last checkpoint; verified to leave bars, duplicates and late counts unchanged |
| Flink SQL is changed | the job starts with empty state from its last committed position | the minute in flight is wrong; the reconciliation reports it |
| ClickHouse or the API is down | nothing is lost; Kafka holds the data | ClickHouse catches up from its saved positions |
| Redis restarts | latest prices empty for about a second | refilled by the next trade and quote |
| Kafka is down | collectors buffer briefly in memory, then lose data | restart; gaps record the loss |
| Everything downstream of raw is wrong | | `scripts/replay.sh` rebuilds it all from the raw topics |

## Ports on the host (all bound to 127.0.0.1)

| Port | Service |
|---|---|
| 18080 | the web page and API |
| 13000 | Grafana |
| 19090 | Prometheus |
| 18081 | Flink web UI |
| 18082 | Airflow |
| 18123 | ClickHouse HTTP |
| 16379 | Redis |
| 19092 | Kafka |
