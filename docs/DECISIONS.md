# Decisions

Each entry: what was chosen, what lost, and why.

## 1. Kafka holds the frame exactly as received; metadata goes in headers

**Chosen.** The Kafka value is the WebSocket frame, byte for byte. The collector adds four
headers (`venue`, `conn`, `seq`, `recv_ns`) and sets the record timestamp to the time the
frame arrived.

**Lost.** Wrapping each message in an envelope (`{"venue":…, "received":…, "payload":…}`), or
normalising in the collector.

**Why.** If parsing is wrong, it can be fixed and the raw topic replayed; a collector that
normalises loses whatever it did not understand. An envelope would mean the stored bytes are
no longer what the venue sent. Headers carry the metadata without touching the value.

## 2. One topic per venue and kind, one partition each

**Chosen.** `raw.<venue>.trades`, `raw.<venue>.book`, `raw.<venue>.control`, each with a
single partition.

**Lost.** One topic for everything; or several partitions keyed by pair.

**Why.** The kinds need different retention: trades are small and feed the hourly rebuild
(7 days), Bitstamp's book messages are about 5 KB each (6 hours, 1 GB cap), control messages
are for debugging (1 day). One partition keeps each venue's messages in arrival order, which
gap detection relies on, and the volume (a few hundred messages a second in total) needs no
parallelism.

## 3. Best bid/offer only, not a full order book

**Chosen.** The "book" topic carries whatever each venue offers that contains the best bid and
offer without the collector keeping state: Coinbase `ticker`, Kraken `ticker` triggered on
best-bid/offer changes, Bitstamp `order_book` (top 100 levels; the first is the best).

**Lost.** Subscribing to incremental depth updates and maintaining a book per venue.

**Why.** Everything downstream needs only the best bid and offer. Maintaining a book means
snapshots, sequence checks and resynchronisation for three different protocols, for no use.
Cost accepted: Coinbase's `ticker` is sent only after a trade, so its best bid/offer is as of
the last trade, not continuous.

## 4. The collector sorts messages by one field, set in the venue's config

**Chosen.** Each venue file names one field (`type`, `channel` or `event`) and which of its
values mean trades or book. Everything else, including anything unparseable, is "control".

**Lost.** A Python class per venue.

**Why.** A venue is then a YAML file: adding or dropping one changes no code. The collector
reads the message to route it but never alters it.

## 5. confluent-kafka rather than aiokafka

**Chosen.** `confluent-kafka` (the librdkafka client). `produce()` only queues the message; a
background thread sends it, so it does not block the asyncio loop.

**Lost.** `aiokafka`, the native asyncio client.

**Why.** librdkafka tracks new broker versions closely (the broker here is Kafka 4.1, which
dropped old protocol versions) and gives idempotent delivery with one setting.

## 6. Ask Kraken to replay recent trades on every connect

**Chosen.** `snapshot: true` on Kraken's trade channel: each connection starts with the last
50 trades. Coinbase's `last_match` plays a similar, smaller role.

**Lost.** `snapshot: false` and no duplicates.

**Why.** After a short disconnect the replay fills the hole. The price is duplicates, which
the processing step removes by trade id; it must do that anyway.

## 7. Flink writes to Kafka; ClickHouse and Redis read from Kafka

**Chosen.** Every Flink result goes to a Kafka topic. ClickHouse pulls those topics itself
(Kafka table engine plus a materialized view per topic). A small Python service copies the
latest trade and quote per venue and pair into Redis.

**Lost.** Flink writing straight into ClickHouse and Redis with connectors.

**Why.** Flink has no maintained SQL sink for Redis. ClickHouse's own Flink connector is
young, insert-only and published for Flink 2.0, while this project runs 2.2. Kafka in the
middle needs no connector beyond the standard Kafka one, lets each store be rebuilt by
re-reading a topic, and gives the web service a live feed of bars for free.

## 8. One normalised trades topic per venue

**Chosen.** `rivus.trades.coinbase`, `rivus.trades.kraken`, `rivus.trades.bitstamp`. The
processing job reads all three as one table; Flink keeps a watermark per topic and uses the
lowest.

**Lost.** One `rivus.trades` topic holding all venues. This was built first.

**Why.** It broke on replay. When the normalise job caught up on 20 minutes of backlog it
read the three raw topics at different speeds and interleaved them in the single output
topic, far out of time order. The watermark followed whichever venue was furthest ahead and
most of the other venues' trades were then "late": 10,914 of 11,649 trades were flagged and
left out of bars. With a topic per venue each stays in its own time order, and the same
replay flags none.

## 9. Duplicates are removed in the processing job, and normalising keeps no state

**Chosen.** `normalise.sql` is a stateless mapping. `process.sql` starts by keeping the first
copy of each (venue, pair, trade id).

**Lost.** Removing duplicates in the normalise job, so the topic is already clean.

**Why.** Flink's Kafka sink here is at-least-once: after a restart the job re-sends whatever
it wrote since its last checkpoint. If the clean-up sat upstream of that sink, those re-sent
trades would reach the bars as fresh duplicates. Putting the clean-up at the start of the job
that builds the bars covers every source of duplicates at once: venue replays, collector
restarts and upstream re-sends.

## 10. Jobs resume from their last checkpoint

**Chosen.** `flink/submit.sh` records each job's id and a hash of its SQL. When a job is
missing it is resubmitted from its newest checkpoint if the SQL is unchanged, otherwise with
empty state from its last committed Kafka position.

**Lost.** Always starting with empty state; or one Flink cluster per job (application mode).

**Why.** Without the saved state a restarted job forgets which trades it has seen and where
its open windows stood. Tested by restarting the cluster under load: with resume, every bar
still matched the stored trades and no trade was duplicated or flagged late.

## 11. At-least-once delivery, made safe by the tables

**Chosen.** Sinks may deliver a row twice. ClickHouse tables are `ReplacingMergeTree`, keyed
so that a repeated row replaces itself; bars use the trade count as the version, so a fuller
bar beats a partial one.

**Lost.** Exactly-once delivery with Kafka transactions.

**Why.** Transactions only release data to readers at each checkpoint, so everything
downstream would lag by the checkpoint interval (30 seconds). Idempotent tables give the same
end result with no added delay.

## 12. Watermark 2 seconds behind; a quiet venue stops counting after 2 seconds

**Chosen.** A trade is late if it is more than 2 seconds older than the newest trade from its
venue. A venue with no trades for 2 seconds no longer holds the watermark back.

**Lost.** A longer allowance (fewer late trades, slower bars) or none.

**Why.** Measured: within a venue, trades arrive at most 0.25 seconds out of step. Two seconds
is generous. Bitstamp often goes several seconds without a trade; without the idle rule every
bar would wait for it. Measured result: a 1-second bar is in ClickHouse 3.4 seconds after its
window ends on average, 4.9 at worst.

## 13. Gaps are detected in arrival order, and not for Bitstamp

**Chosen.** For Coinbase and Kraken, a trade id more than one above the previous id seen for
that pair is a gap. Trades are compared in the order they arrive.

**Lost.** Comparing in event-time order; including Bitstamp.

**Why.** Coinbase and Kraken number trades consecutively per pair (checked on 5,000 trades:
every step was 1 except across collector restarts). Bitstamp's ids are shared across all its
pairs, so they jump constantly. Arrival order is used because ids are issued in order; sorting
by event time would drop late trades first and report a gap that is not there.

## 14. Venues are compared on the minute's volume-weighted average price

**Chosen.** `venue_checks` compares each venue's 1-minute VWAP.

**Lost.** Comparing closing prices.

**Why.** A quiet venue's last trade of the minute may be 50 seconds older than a busy one's,
so closes differ just because the market moved. The average over the minute is steadier.

## 15. The API talks to ClickHouse over plain HTTP

**Chosen.** `ClickHouseStore` posts SQL to ClickHouse's HTTP interface with `HttpClient` and
reads one JSON object per line. Caller-supplied values go as `param_name=value` and appear in
the SQL as `{name:Type}`.

**Lost.** A ClickHouse driver package for .NET.

**Why.** The whole exchange is visible in forty lines, there is no dependency to keep up with,
and server-side parameters rule out SQL injection. The cost is writing the row mapping by
hand, which here is one naming rule (snake_case to PascalCase).

## 16. Live prices reach the browser through Redis publish/subscribe

**Chosen.** The latest-price writer publishes each change on a Redis channel. The API
subscribes and forwards every message to all SignalR clients.

**Lost.** The API consuming Kafka directly.

**Why.** Redis already holds the latest state, so the page loads that state with one read and
then follows the changes from the same place. The API needs no Kafka client, and several API
instances would all receive the same messages without any coordination.

## 17. Stream health is measured by the API and exposed as Prometheus gauges

**Chosen.** Every 15 seconds the API runs one ClickHouse query (trades per minute, bar delay,
gaps, late trades, venue spread, reconciliation differences) and publishes the answers as
`rivus_*` gauges. The same query feeds the "stream health" table on the page.

**Lost.** Pointing Grafana straight at ClickHouse with a plugin.

**Why.** Grafana then needs one data source and no plugin download, and the figures are
ordinary Prometheus series that alert rules could use later.

## 18. The chart is drawn by hand in SVG

**Chosen.** About sixty lines of JavaScript build the line chart, its labels and hover.

**Lost.** A charting library.

**Why.** The page is plain HTML and JavaScript with a single vendored file (the SignalR
client). One line chart does not justify a second.

## 19. The batch starts again from the raw messages

**Chosen.** Each hour the batch reads the raw trade topics straight from Kafka and parses the
venues' messages itself, in dbt models written separately from the Flink SQL.

**Lost.** Rebuilding bars from the stream's normalised trades (`rivus.trades`).

**Why.** A check that shares the stream's parsing would agree with the stream's mistakes. Two
independent readings of the same raw bytes that arrive at the same bars are evidence; one
reading compared with itself is not.

## 20. One DuckDB file per hour

**Chosen.** Each run writes `/data/batch/<hour>/recon.duckdb`, next to the raw and stream
files it was built from.

**Lost.** One growing DuckDB database with incremental models.

**Why.** An hour is then a self-contained folder that can be rebuilt, inspected or deleted
without touching any other, and two hours can run at once (DuckDB allows one writer per
file). The results that matter are copied to ClickHouse.

## 21. Airflow standalone, with the batch code in its own Python environment

**Chosen.** One Airflow 3 container (`airflow standalone`, SQLite). The DAG's tasks run
`python -m rivus_batch <step>` from a separate virtual environment in the same image.

**Lost.** Airflow's separate services on Postgres; importing dbt into Airflow's own Python.

**Why.** On one laptop the separate services cost memory for no gain. Airflow and dbt pin
many of the same libraries to different versions, so they are kept apart; it also means every
step is an ordinary command that runs the same with or without Airflow.

## 22. Differences are classified, and rounding does not count against a bar

**Chosen.** A differing field gets the first reason that fits: rounding (relative difference
under one millionth), late event, gap, ordering, unexplained. A bar whose only differences are
rounding counts as agreeing.

**Lost.** Comparing with a tolerance and reporting nothing below it; or counting every
difference equally.

**Why.** Nearly every bar's average price differs in its last digits: Flink's decimal
arithmetic keeps six decimal places through the multiplication, DuckDB keeps sixteen. That is
worth recording and not worth an alarm. Rounding is tested first, because a tiny difference
is rounding whatever else happened in that minute.

## 23. Job starts are recorded, and explain differences as "restart"

**Chosen.** The script that keeps the Flink jobs submitted writes one row per start to
ClickHouse (`job_starts`): when the job started, whether it resumed from a checkpoint, and
when the job before it last saved a checkpoint. The reconciliation labels a differing bar
"restart" if its minute lies between that last checkpoint (less one minute) and the start.
The order of reasons is now rounding, restart, late event, gap, ordering, unexplained.

**Lost.** A Kafka topic for job starts, read by ClickHouse like every other table: it would
match the rest of the design, but needs a Kafka client in the Flink image for about one
message a week. Asking Flink's REST API for job history: a session cluster forgets its jobs
when the job manager restarts, which is exactly when the answer is needed. Putting "restart"
after "gap": a job that starts with empty state has forgotten the last trade id it saw and
its open windows, so gaps and late trades in that minute are symptoms of the restart.

**Why.** The first restart with changed SQL produced six differing bars, half labelled "gap"
and half "unexplained", although the cause was known. "Unexplained" should mean that nobody
knows. The last checkpoint is used as the start of the outage because nothing records the
moment a job dies; with a checkpoint every 30 seconds it is at most that far off. The starts
are written to a file first and handed to ClickHouse when it answers, so a job is never held
up by the record of it.

## 24. The batch waits for the stream to pass the hour

**Chosen.** Before exporting the stream's bars, the batch waits (up to five minutes) until the
stream has produced a bar later than the hour being checked. If none comes it compares anyway.

**Lost.** Failing the task and letting Airflow retry: a venue outage would then look like a
broken batch. Not waiting: correct on a normal day, wrong after an outage.

**Why.** A minute's bar is only emitted once a later trade has been seen. After the machine
had been off overnight, Airflow started faster than Flink and checked the interrupted hour
before the stream had emitted its last minute: six bars were reported missing that appeared
seconds later.

## 25. Alert rules in Prometheus, shown and not sent

**Chosen.** Twelve rules in `monitoring/alerts.yml`, evaluated by Prometheus, visible on its
`/alerts` page and as a panel on the Grafana dashboard. Each is tested with `promtool` against
made-up metric values, in CI.

**Lost.** Alertmanager with e-mail or chat delivery: it needs a destination and a secret, and
on one laptop the person to be told is the person at the keyboard. Grafana-managed alerts:
they live in Grafana's database or more provisioning files, and cannot be unit-tested the way
rule files can.

**Why.** The gauges existed but somebody had to look at them. After the stack was found to
have been off for twenty hours, "what is wrong right now" should be one glance. Rules as a
file are reviewed and tested like code. Thresholds come from what was measured: a healthy
reconciliation is between 1 h 10 min and 2 h 10 min old, so three hours is stale; consumer
lag sits in the hundreds, so twenty thousand is behind.

An alert cannot report that the whole machine is off: Prometheus is off too.

## 26. ClickHouse's own logs are cut back

**Chosen.** A settings file (`clickhouse/config/system-logs.xml`) sets the server's log level
to "information", keeps `query_log` and `part_log` for seven days, and switches off the other
self-logging tables (text, trace, metric, asynchronous metric, processors profile, query
views, OpenTelemetry spans).

**Lost.** Leaving the defaults; or keeping every table with a short expiry.

**Why.** After thirteen hours the market data took 58 MB and ClickHouse's logs about itself
took 580 MB in tables and 138 MB in files, none of it ever expiring. The Kafka tables poll
every second and each poll is logged at trace level. The two tables kept are the ones that
answer real questions (which query was slow; what was written and merged); the metric tables
duplicate what Prometheus already collects.

## 27. The stream's average price is computed exactly

**Chosen.** In the bar queries, price and quantity are narrowed to `DECIMAL(18, 8)` before
they are multiplied, so the product keeps all 16 decimal places.

**Lost.** Leaving the arithmetic alone and widening the "rounding" tolerance for bars with
little volume.

**Why.** Flink caps a decimal at 38 digits. The product of two `DECIMAL(24, 8)` would need 49,
so Flink cuts its scale to 6 decimal places. Decision 22 recorded this as harmless rounding,
and for ordinary bars it was. For a bar holding one trade of 0.000064 it was not: the value
0.12910528 became 0.129105, and dividing by the quantity gave 2017.265625 for a trade at
2017.27. The reconciliation called it "unexplained", correctly, since the stream's number was
wrong and not merely rounded. A wider tolerance would have hidden a wrong price; ten digits
before the decimal point are more than any price or quantity here needs. Decision 22 still
stands: the classification stays, with far less to classify.

## 28. ClickHouse takes rows from Kafka every ten seconds, not every second

**Chosen.** `kafka_flush_interval_ms = 10000` on all seven queue tables.

**Lost.** One second (the first setting); or keeping one second and shortening how long
replaced parts stay on disk (`old_parts_lifetime`), which frees space sooner but writes just
as much.

**Why.** Every hand-over from a queue to its table creates a part, a small set of files, and
ClickHouse then merges parts into larger ones in the background, rewriting the data each time.
At one second that was about 9,000 new parts an hour. Measured over an hour: 34 MB of new data
caused 7.5 GB of merge writes, and the server had written 471 GB in its first two days. The
price of ten seconds is that a bar or trade reaches ClickHouse up to ten seconds later, so the
delay measured in decision 12 no longer holds (see the build log). The live prices on the page
come from Redis and are not affected; the hourly comparison already waits for the stream to
pass the hour (decision 24).

## 29. The batch keeps three days of working files

**Chosen.** After each hourly run, the working folders of hours more than 72 hours old are
deleted (`BATCH_KEEP_HOURS`).

**Lost.** Keeping them all; or deleting each hour's files as soon as its results are loaded.

**Why.** An hour's folder holds the raw export, the DuckDB file and dbt's output: about 40 MB,
a gigabyte a day, with nothing ever removing them. They are only needed to look into a
difference by hand, which happens within a day or two. The results are in ClickHouse, and an
hour can be rebuilt from Kafka for as long as the raw trades are kept (7 days).

## 30. A deploy stops the jobs with a savepoint, and falls back if it does not fit

**Chosen.** `scripts/deploy-flink.sh` builds the image, stops both jobs with a savepoint,
restarts, and `flink/submit.sh` starts each job from its savepoint even though its SQL has
changed. The job is then watched: if a task fails or the job is not fully running within 90
seconds, it is cancelled and started with empty state, as before.

**Lost.** Starting changed SQL with empty state every time (decision 10, the minute in flight
comes out wrong); or restoring with "ignore state that cannot be restored", which never fails
and can therefore silently drop state.

**Why.** Most changes to a job leave its saved state usable: a comment, a filter, a new
output column. Flink SQL promises nothing here, and a state that does not fit is discovered
late: the job is accepted, a task then fails with `StateMigrationException` while loading,
and the job restarts for ever. So the script does not predict whether state fits, it tries
and looks. A savepoint is taken as the job stops, so its position in Kafka is also the
committed one, and the fall-back continues from exactly there: the worst case equals the old
behaviour. Tried both ways on the day (build log).

## 31. Every stored table gets its own freshness alert

**Chosen.** Besides `NoNewBars` (1-second bars), `TradesNotStored` and `MinuteBarsNotStored`
check how old the newest stored trade and 1-minute bar are. The API measures both in
ClickHouse, like the other health gauges.

**Lost.** One end-to-end check on the 1-second bars, as before; or alerts on ClickHouse's own
Kafka reader metrics.

**Why.** Each table is filled by its own reader in ClickHouse, and readers can stop one at a
time: from 22 to 24 Nov 2026 trades and 1-minute bars stopped being stored for 37 hours while
1-second bars carried on, and nothing fired. Measuring the age of the newest row checks what
users actually read, whatever the cause; reader metrics would only cover one cause.

## 32. Three brokers, three copies, one controller kept

**Chosen.** Two more brokers join the running cluster as brokers only. Every partition is
stored on all three (replication factor 3) with `min.insync.replicas=2`, and the producers
already use `acks=all`: a write is acknowledged once two brokers have it, so losing any one
broker loses nothing and stops nothing. The first node stays the only controller.

**Lost.** Three combined broker-and-controller nodes, the usual shape of a new cluster; or
staying on one broker.

**Why.** The controller quorum of a running cluster cannot be grown without a migration of
its own (moving to a dynamic quorum), and starting a new cluster would throw away the 7 days
of raw trades the batch rebuilds from. Adding brokers and copying partitions onto them
(`kafka-reassign-partitions`) happens while everything runs. The cost is honest: the first
node is still a single point of failure for the cluster's metadata. While it is down, the
partitions it leads have no leader and nothing can move leadership; partitions led by the
other two carry on. A new cluster should start with three controllers.

## 33. Partitions keyed by pair; only the stateless job runs in parallel

**Chosen.** The raw trade and book topics, the normalised trade topics and the quotes topic
have 3 partitions, and every message carries its pair as the Kafka key, so a pair always
lands in the same partition and stays in order there. Collectors hash keys the way Java
clients do (`murmur2_random`), so a pair is in the same partition whoever writes it.
The normalise job runs 3 parallel copies, one per partition, spread over two task managers.
The process job stays at 1.

**Lost.** Process job at parallelism 3; more partitions; keys by venue.

**Why.** Normalising is one message in, rows out, no state: it splits freely, and a pair's
messages all pass through the same copy, in order. The process job is order-sensitive: the
opening and closing price of a bar are the first and last trade to arrive, and "a jump in
trade ids is a gap" assumes ids arrive in order. Its duplicate filter is keyed by trade id,
so with several copies two trades of one pair would pass through different copies and could
reach the bar out of order. Keying everything by pair instead would cap it at six keys, and
the volume (a few hundred messages a second) does not need it.

With two pairs per venue, two of each topic's three partitions are busy (BTC-USD hashes to
partition 0, ETH-USD to 2) and partition 1 is empty. A partition count only spreads load up
to the number of distinct keys. Three partitions keep one leader per broker and room for a
third pair.

## 34. A spare collector per venue, trades only; the first copy wins

**Chosen.** A second collector per venue (`collector-<venue>-b`) with its own connection,
writing trades only to the same raw topics. Every trade then reaches Kafka twice; the
process job keeps the first copy to arrive, ClickHouse keeps one row per trade id, and the
batch keeps the first copy received. Each message says which collector sent it (header
`collector`: `a` or `b`).

**Lost.** One collector per venue, as before; a spare that forwards everything; picking one
"live" collector and switching to the spare only when it fails.

**Why.** A dropped connection or a restarted collector loses the trades sent while it is
away, for good: the venues do not replay them. With two connections, a trade is lost only
if both miss it. Writing both and removing duplicates by a natural key (the venue's trade
id) needs no failover logic and no decision about which side is healthy; the duplicate
filter already existed because Kraken replays its last trades on every connect. Book
messages are not doubled: each is replaced by the next a moment later, and Bitstamp's are
large.

The gap check still compares trade ids in arrival order. With both copies complete that
order is still the venue's order: each trade's first arrival is the earlier of two in-order
streams, which is itself in order. Only when one collector misses trades *and* the other's
copy of them arrives later than the next trade from the first does a gap appear that the
other copy then fills; the reconciliation would show those trades as present.

## 35. A second consumer group archives the raw trades, committing after each file

**Chosen.** `archive`, a small Python service in its own consumer group (`rivus-archive`),
reads every raw trade topic and writes Parquet files of up to 20,000 messages or 60 seconds,
one folder per topic, day and hour, kept 30 days. Each file is named after its partition and
first offset. Positions are committed by hand, only after the file holding everything before
them has been written and renamed into place.

**Lost.** Kafka Connect with an S3 or file sink; committing automatically every few seconds;
relying on Kafka's 7 days of retention alone.

**Why.** It shows what a consumer group is for: a new reader of the same topics with its own
positions and lag, which costs the producers and the stream nothing and can be rewound on its
own. Committing after the write means a crash re-reads from the start of the unwritten file
and rebuilds it under the same name, so the archive holds each message once (at least once
delivery, made exactly once by naming files after offsets). Automatic commits could move the
position past messages still in memory and lose them on a crash. Kafka Connect does the same
job in production, but is a JVM service of its own, too heavy for this laptop. The archive
also outlives Kafka's retention: the hourly batch can only rebuild the last 7 days.
