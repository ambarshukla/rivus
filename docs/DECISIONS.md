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
