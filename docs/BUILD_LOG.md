# Build log

What was built, what was predicted before running it, and what actually happened.

## Phase 1: collect (2026-11-11)

**Built.** Kafka 4.1 in KRaft mode, nine raw topics, and one Python asyncio collector per
venue, each driven by a YAML file.

**Probed first.** A throwaway script subscribed to each venue for 20 seconds and printed the
first messages of every type. Findings that shaped the design:

- All three feeds work without an account. Coinbase's subscription reply shows the public
  depth channel is limited to 50 levels.
- Trade ids look consecutive per pair on Coinbase and Kraken. Bitstamp's ids increase but are
  shared across all its pairs, so a jump there does not prove a missing trade.
- Kraken sends prices and quantities as JSON numbers; Coinbase sends strings; Bitstamp sends
  both (`price` and `price_str`). The processing step must read the string forms where they
  exist.
- Timestamps: Coinbase and Kraken ISO-8601 with microseconds; Bitstamp a microsecond epoch
  as a string.
- Kraken announces planned maintenance in its `status` message.

**Predicted.** Roughly 10 trades a second across the venues, Kraken's best-bid/offer channel
the busiest, Bitstamp's book the largest by bytes.

**Observed** (first 53 seconds, messages / bytes):

| Topic | Messages | Bytes |
|---|---|---|
| raw.coinbase.trades | 370 | 106 k |
| raw.coinbase.book | 363 | 148 k |
| raw.coinbase.control | 107 | 14 k |
| raw.kraken.trades | 67 | 37 k |
| raw.kraken.book | 1,113 | 346 k |
| raw.kraken.control | 59 | 3 k |
| raw.bitstamp.trades | 15 | 4 k |
| raw.bitstamp.book | 739 | 3,808 k |
| raw.bitstamp.control | 4 | 0.3 k |

About 8.5 trades a second, most of them Coinbase. Bitstamp's book is 75 kB a second before
compression, about 6.4 GB a day, which is why that topic is capped at 1 GB.

**Reconnect, tested live.** The Kraken collector's container was cut off from the network.
It reported "no message for 15s", retried with growing waits (0.9, 1.6, 2.4, 5.8, 12.7
seconds) while name resolution failed, and reconnected by itself 5 seconds after the network
came back. The first run showed a 10-second pause between detecting the dead connection and
retrying: the WebSocket library was waiting for a closing handshake. `close_timeout` is now
2 seconds.

**Tests.** 30, run in Docker: the shipped venue files against captured live messages, bad
configs, and the collector against a local WebSocket server (frames forwarded unchanged,
headers, stale detection, reconnect on request, reconnect after a drop, a venue that is down).

## Phase 2: process (2026-11-11)

**Built.** Flink 2.2 session cluster running two SQL jobs. `normalise` maps the three venues'
raw messages to one trade schema and one quote schema. `process` removes duplicates and
produces 1-second and 1-minute bars, late trades, sequence gaps and a per-minute cross-venue
price check. Results go to Kafka; ClickHouse 25.8 ingests them with Kafka engine tables, and
a small Python service keeps the latest price per venue and pair in Redis.

**Versions checked first.** Flink 2.3.0 is the newest release but has no Kafka connector yet;
the newest connector (5.0.0) is built for 2.1 and 2.2, so this runs Flink 2.2.1.

**Measured before choosing the watermark.** Over 5,000 trades, the gap between a trade's own
timestamp and its arrival varied by at most 0.25 seconds within a venue. The arrival times
were about 0.4 seconds *earlier* than the trade times: this machine's clock runs 0.4 seconds
behind the venues'. Event-time processing does not care; anything comparing with the local
clock must allow for it.

**What went wrong, and what it changed.** Four things did not work the first time.

1. *One merged trades topic.* Expected: replaying 20 minutes of backlog gives the same bars
   as live processing. Got: 10,914 of 11,649 trades flagged late and missing from bars,
   because the merged topic was far out of time order. Fixed with a topic per venue
   (decision 8). After the fix the same replay flagged 0.
2. *Checkpointing was silently off.* The interval was set in the cluster's configuration, but
   a job takes it from the client that submits it. No checkpoints meant no saved Kafka
   positions, so every restart re-read everything. The settings now live in `tables.sql`.
3. *Restarts without state.* A restarted job re-sent trades that the next job then saw as
   late duplicates. Fixed by resuming from the last checkpoint and by removing duplicates in
   the job that builds the bars (decisions 9 and 10).
4. *Every rebuild restarted every container*, collectors included, and each collector restart
   is a real gap in the data: about 30 gaps were recorded during this session, all at restart
   times. Builds on this Docker version attach a provenance record that changes the image id
   even when nothing changed. `BUILDX_NO_DEFAULT_ATTESTATIONS=1` stops that.

**Verified by running it.**

- `scripts/check_bars.sql` rebuilds every bar from the stored trades inside ClickHouse and
  compares. After a full replay from the raw topics: 265 one-minute bars and 5,800 one-second
  bars, all equal in count, volume, high, low, open and close; 0 late trades; 0 duplicate
  trades (23,579 rows, 23,579 distinct).
- Restarting the Flink cluster mid-stream: both jobs resumed from their checkpoints, and the
  same check still showed no differences and no duplicates.
- Changing the SQL (which forces a start with empty state): the one minute in flight at the
  restart came out wrong for each venue and pair, 6 bars in all; every 1-second bar was still
  right. This is the known cost of an empty-state start and is what the hourly batch
  reconciliation exists to catch.
- Delay from the end of a 1-second window to the bar being stored: 3.4 seconds on average,
  4.9 at worst.
- Memory in use by the whole stack: about 3.5 GB (Flink 1.8, Kafka 0.8, ClickHouse 0.7).

**A measuring mistake worth remembering.** Flink's REST API reported the watermark trailing
the clock by up to 14 seconds. The API only refreshes its metrics every 10 seconds; the real
lag was about 4.

**Tests.** `flink/run-sql-tests.sh` runs the real job files against fixture files, with no
cluster and no Kafka, and compares with expected output: the captured venue messages through
`normalise`, and ten hand-written trades through `process` (a duplicate, a gap of two, a
Bitstamp id jump that must not count as a gap, two minutes, three venues). The expected bars
and spreads were worked out by hand before the first run and matched. Six unit tests cover
the Redis writer's merging of trades and quotes.

## Phase 3: show (2026-11-11)

**Built.** An ASP.NET Core 10 service: REST endpoints for latest prices, bars, gaps, late
trades, the cross-venue check and reconciliation results; a SignalR hub that pushes price
changes; and a plain HTML and JavaScript page. Prometheus scrapes eight targets (three
collectors, the latest-price writer, both Flink processes, the API, a Kafka lag exporter) and
Grafana starts with one provisioned dashboard.

**Expected.** The page shows three venues updating several times a second and a chart in which
the three lines sit almost on top of each other.

**Observed.** As expected: the venues' 1-minute closes differ by 1 to 4 basis points. SignalR
connected over WebSocket and relayed about 15 updates a second. All eight Prometheus targets
were up on the first scrape.

**What went wrong.** The statistics query failed with "unknown identifier `metric`": an
`ORDER BY` after a chain of `UNION ALL` belongs to the last `SELECT` only, where the column
had no alias. The unit tests had not caught it because they replace ClickHouse with a stub;
it showed up within seconds against the real database. The order was not needed and was
removed.

**Verified by running it.** Every endpoint answered against live data; a malformed pair got a
400 with a reason. Screenshots of the page and the dashboard were taken with a headless
browser inside the Docker network and checked by eye. The reconciliation tables exist and are
empty until phase 4.

**Tests.** 34, run by `dotnet test` in Docker: request checking, the ClickHouse row mapping
(exact decimals, values sent as parameters and never inside the SQL), the gauge reset rule,
and the application started in memory with fake stores (every endpoint, bad requests, the
page and its scripts, the SignalR handshake, a store failure returning 503).

## Phase 4: reconcile (2026-11-11)

**Built.** Airflow 3.4 with two DAGs. `reconcile_hourly` runs at ten past each hour for the
hour just ended: export the raw trade messages from Kafka, export the stream's bars, late
trades and gaps from ClickHouse, run dbt (1.12, on DuckDB) to rebuild the 1-minute bars and
compare, and load the comparison into ClickHouse. `reconcile_daily` adds the hours up. The
web page and the Grafana dashboard show the results.

**Predicted.** Bars agree except (a) the minute that was in flight when the stream job was
restarted with empty state at about 13:36, and (b) possibly some open or close prices where
trades share a timestamp.

**Observed.**

| Hour | Stream bars | Batch bars | Agree | Differ |
|---|---|---|---|---|
| 12:00 (collection began 12:43) | 94 | 94 | 94 | 0 |
| 13:00 | 349 | 349 | 343 | 6 |

- (a) was right: the 6 differing bars are the 13:35 minute for each of the six venue and pair
  combinations. The stream's bar has fewer trades than the batch's (for example 304 against
  370), which is what losing the first part of the minute looks like. Three were labelled
  "gap", because Kraken also dropped the connection in that minute, and three "unexplained".
  Nothing records job restarts, so "unexplained" is the honest label.
- (b) did not happen: no open or close differed anywhere else.
- Not predicted: the average price (vwap) differed in almost every bar, by less than one part
  in a hundred million. Flink's decimal multiplication keeps 6 decimal places here; DuckDB
  keeps 16. These are classified "rounding" and do not count against a bar (decision 22).

**What went wrong.** The first dbt run failed with a decimal overflow: DuckDB multiplies two
18-digit decimals in 64 bits. Widening one operand to 20 digits moves it to 128-bit
arithmetic. The first classification order put "gap" before "rounding", which mislabelled 19
rounding differences in minutes that also had a gap.

**Verified by running it.** The steps were run by hand for both hours, then Airflow ran the
DAG by itself twice (once on start-up for the hour it had missed, once on schedule at 14:10),
both successful, and the scheduled run's results replaced the manual ones. The daily roll-up
was run by hand for the day.

**Tests.** 7 in the batch image, with dbt running for real against the captured messages:
parsing and the maker/taker flip, duplicates, trades outside the hour, a stream that agrees,
each classification, bars missing on either side; plus dbt's own 19 data tests on every run
and a check that Airflow can load the DAG file.

## Phase 5: document and harden (2026-11-11)

**Added.** `docs/ARCHITECTURE.md`; a GitHub Actions workflow that builds and runs each
component's test image exactly as on a developer's machine (collectors, latest-price writer,
API, Flink SQL, batch) and validates the compose file.

**Test count.** Collectors 30, latest-price writer 6, API 34, Flink SQL 8 output tables
compared, batch 7 plus dbt's 19 data tests.

**Not yet verified.** The workflow has not run on GitHub: nothing has been pushed. Every
command it runs was run locally.
