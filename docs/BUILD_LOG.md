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

## After a night of running: restarts (2026-11-12)

**What happened.** The stack ran unattended for twelve hours, then the machine installed
updates and restarted at 01:29 UTC. Docker was not set to start by itself, so nothing ran
until 22:07 UTC. Every container then came back without help and both Flink jobs resumed from
their last checkpoints, 20 hours old.

**Checked.**

- Thirteen hourly reconciliations before the outage: every bar agreed except the six already
  known from the 13:35 restart. 4,328 rounding differences, nothing else.
- The interrupted hour (01:00): the batch ran 13 seconds after start-up, before the stream had
  resumed, and reported the 01:29 bar missing for all six venue and pair combinations. Run
  again a minute later: 177 of 177 bars agree. The resumed job emitted the 01:29 bar from
  state saved before the outage plus nothing else, and it equals the batch's.
- The daily roll-up ran on schedule at 00:30 UTC.
- Late trades overnight: 116, in two bursts of a few seconds (messages received up to 7.8 s
  after their trade time). All missed their 1-second bar; none missed a 1-minute bar, so the
  reconciliation was unaffected.
- Disk after twelve hours: ClickHouse 54 MB.

**Predicted before the change.** With job starts recorded, the six bars of 13:35 on the 11th
would move from "gap" and "unexplained" to "restart", and nothing else would change.
**Result.** As predicted: 25 differing fields in 6 bars, all "restart"; 343 of 349 agree as
before.

**Added.** `job_starts` in ClickHouse, written by `flink/submit.sh`; the "restart"
classification (decision 23); the batch waits for the stream to pass the hour (decision 24).
The four job starts before this change were entered by hand from the submit log and the
checkpoint files' times. The first start recorded by the script itself was the restart that
deployed it: both jobs resumed, 55 and 84 seconds after their last checkpoints.

**Tests.** Batch 10 (was 7): a restart explains differences in its minutes and not later
ones, and beats a gap in the same minute; the wait returns when the stream has passed the
hour and gives up when it does not. dbt's data tests unchanged at 19.

## Alert rules (2026-11-12)

**Added.** `monitoring/alerts.yml` with twelve rules (decision 25); `monitoring/alerts_test.yml`
and the `alerts-test` service that runs it with `promtool`; a CI job for it; an "Alerts
firing" panel on the dashboard.

**Predicted.** Loaded into the running stack an hour after the outage ended: "trades missed"
firing for Coinbase and Kraken (the ids jumped across the twenty hours), "reconciliation
stale" for all three venues, everything else quiet. **Result.** As predicted. "Reconciliation
stale" was pending, not yet firing: it waits ten minutes.

**What went wrong.** A count of 1,372,447 missed trades was printed as `1.372447e+06`; the two
rules that can show large numbers now format them. The tests passed first time, which proves
little, so two thresholds were changed on a copy of the rules: both changes were caught.

## ClickHouse's own logs (2026-11-12)

**Found** while measuring disk after the first night: the ClickHouse volume held 1.5 GB for
58 MB of market data. The rest was the server's logs about itself (decision 26).

**Changed.** `clickhouse/config/system-logs.xml`, mounted into the server's `config.d`.

**Verified by running it.** After the restart `query_log` and `part_log` carry a seven-day
expiry (read back from `system.tables`), the text log's newest row is from before the restart,
and the stream's bars continued within a minute. The old log tables were dropped by hand on
this machine; a fresh volume never creates them.

## An average price that was wrong, not rounded (2026-11-14)

**Found** by the reconciliation, as the only "unexplained" difference in 41 hours and 14,664
bars: Bitstamp ETH-USD, 2026-11-13 00:02 UTC, one trade of 0.000064 at 2017.27; the stream's
average price was 2017.265625.

**Cause.** Flink kept 6 decimal places in price times quantity (decision 27). The error is
at most half a millionth per trade, divided by the bar's volume: invisible in a busy bar,
0.004 in this one.

**Changed.** `flink/sql/process.sql` narrows both operands so the product is exact. The test
fixtures gain that trade.

**Verified by running it.** With the new fixture and the old SQL the test gives 2017.265625;
with the new SQL 2017.27. Bars that divide unevenly now also carry 8 correct decimals
(101.33333333 where the old SQL gave 101.333333).

## Less disk: slower hand-overs to ClickHouse, old batch files deleted (2026-11-14)

**Found** while checking what grows on disk after three days. Two things: ClickHouse had
written 471 GB to store about 350 MB (decision 28), and the hourly batch had kept every
hour's working folder, 2.7 GB so far (decision 29). Everything else is bounded by an expiry:
Kafka topics, the ClickHouse tables, Prometheus.

**Changed.** `kafka_flush_interval_ms` from 1 to 10 seconds in `clickhouse/init/01_schema.sql`.
A `prune` step in the batch (`rivus_batch/steps.py`), run as the last task of
`reconcile_hourly`, with two tests.

**Predicted.** About a tenth of the parts and of the merge writes; bars stored about 5 seconds
later on average.

**Verified by running it.** The setting cannot be altered on a Kafka table, so on this machine
the seven `_mv` and `_queue` objects were dropped and the schema file run again; ClickHouse
continued from the position Kafka holds for it. Ten minutes before against ten minutes after:

| | before | after |
|---|---|---|
| New parts | 1,572 | 210 |
| Written by merges | 1.53 GB | 122 MB |
| Replaced parts waiting for deletion | 1.3 GB | 110 MB |
| Delay from the end of a 1-second window to the bar being stored | 3.4 s average, 5.7 s worst | 7.6 s average, 13.7 s worst |

Trades per minute are continuous across the change and no gap was reported. The prune, run
by hand for the hour of 21:00, removed the 9 folders older than 72 hours and nothing else.

## The reconciliation by day, and what the reasons mean (2026-11-14)

**Built.** `GET /api/reconciliation/daily?days=14` returns the batch's daily roll-up (one row
per completed day, venue and pair; `days` is held between 1 and 90). The page adds them up
into one row per day under "By day", and lists what each difference reason means under the
differences table. No change to the stream or the batch.

**Verified by running it.** 37 API tests pass, four of them new. Against the running stack
the endpoint returned 18 rows for three days, and the page, loaded in a headless browser,
showed 12, 4 and 24 hours checked for 11 to 13 November with 4,010 of 4,016, 1,207 of 1,207
and 8,615 of 8,616 bars agreeing: the same numbers as the table in ClickHouse.

## A runbook (2026-11-14)

**Written.** `docs/OPERATIONS.md`: a health check, what each kind of change restarts and
costs, every alert with its first action, where data stops, what is kept for how long, and
what running on a laptop does. Each statement in it comes from something measured and
recorded above; the commands were run against the stack on the day.

## Deploying changed SQL without spoiling a minute (2026-11-14)

**Built.** `flink/submit.sh` gains `stop` (stop every job with a savepoint, hold the loop) and
`resume`, starts from a savepoint when one is waiting, and watches a job started from saved
state until it has settled. `scripts/deploy-flink.sh` runs the whole deploy (decision 30).

**Predicted.** A change that leaves the state's shape alone restores and no bar is wrong. A
change to a decimal type inside an aggregate does not restore; unknown whether Flink refuses
it at submission or fails later.

**Verified by running it, on the live stack.**

1. *State that fits.* A comment added to `tables.sql`, which changes both jobs' SQL. Stopped
   at 23:20:21 UTC, both back by 23:21:55, both recorded as resumed. The eight minutes around
   it, including 23:20 and 23:21 which were in flight, each agree with a rebuild from the
   stored trades on all six bars (trades, volume, high, low, average price). No gap, no late
   trade.
2. *State that does not fit.* `DECIMAL(18, 8)` changed to `DECIMAL(20, 8)` in the 1-minute
   average price, inside the running container only. Flink accepted the job; ten seconds
   later a task failed with `StateMigrationException`. The script cancelled it and started
   with empty state 23 seconds after the first attempt. Putting the SQL back did the same in
   the other direction.

3. *The script itself.* `scripts/deploy-flink.sh` was then run three times: with nothing
   changed, and to deploy the two fixes below. The jobs were down 97 to 100 seconds each
   time and came back from their savepoints.

Afterwards every 1-minute bar from 23:17 to 23:38 was compared with a rebuild from the stored
trades: of 22 minutes, 20 agree on all six bars, across four savepoint restarts and one stop
of five and a half minutes. The two that differ are 23:25 and 23:26, the minutes of the second
test, started with empty state on purpose.

**Two faults found by running it.**

- With nothing changed in `flink/`, the submit container is not replaced, so nothing
  released the hold and the jobs stayed down for five and a half minutes (no bar was wrong:
  they resumed from their savepoints). The deploy script now releases the hold itself.
- A start that fell back to empty state was recorded with the wrong "down since" time,
  because the savepoint it is read from had already been forgotten. Fixed in `submit.sh`;
  the two rows of the test were corrected by hand in `rivus.job_starts` (to 23:25:39 and
  23:26:31, the times in the log), so that the reconciliation labels those minutes "restart".

**Not covered.** A job that restores but whose new SQL makes old state mean something else
(for example a changed window size): Flink cannot detect that, and neither does the script.

## A blind spot in the alerts, and 23 hours checked again (2026-11-27)

**Found.** From 22 Nov 20:03 to 24 Nov 09:26 trades and 1-minute bars reached Kafka but were
not stored in ClickHouse; every row of those 37 hours was stored at 09:26:08, when a restart
started ClickHouse's readers again. Nothing was lost (Kafka keeps the topics for 7 days).
1-second bars carried on the whole time, so `NoNewBars` never fired. The cause is not known:
ClickHouse's error log has nothing at that time. A second, smaller case of a stuck position:
on 26 Nov the Flink normalise job's saved Kafka offsets stopped moving for an hour while
checkpoints completed and data was fresh; `ConsumerLagging` fired on a lag that was not real.

**Built.** `TradesNotStored` and `MinuteBarsNotStored` (decision 31), a new gauge
`rivus_last_bar_1m_age_seconds`, and a note in `ConsumerLagging` on what the Flink lag means.
`promtool` tests replay the 22 Nov pattern: fresh 1-second bars, stale trades and 1-minute
bars.

**Predicted.** The tests: `TradesNotStored` pending at minute 9 and firing at minute 12;
`MinuteBarsNotStored` firing at 14 with the bar 9.5 minutes old; `NoNewBars` silent. Live:
the trade gauge a few seconds, the 1-minute gauge under 70 seconds.

**Happened.** All alert tests pass. Live: `rivus_last_trade_age_seconds` 6 to 8 s,
`rivus_last_bar_1m_age_seconds` 29 s, both rules loaded, nothing firing.

**The stale hours checked again.** Hours 22 Nov 20:00 to 23 Nov 19:00 had been checked while
their stream bars were missing (360 "unexplained" rows an hour). Re-running the batch for
each of the 24 hours from the raw trades still in Kafka: 7,998 + 817 stream bars, every one
matched, 4 rounding rows. The daily summaries for 22 and 23 Nov now show 0 differences.

