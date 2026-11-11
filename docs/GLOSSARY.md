# Glossary

**Airflow.** A scheduler for batch work: it runs tasks at set times, in a set order, retries
failures and keeps a history of every run.

**Backoff.** Waiting longer after each failed attempt (here 1 second doubling to 60) so a
struggling server is not hammered. *Jitter* is a random factor on the wait so that several
clients do not retry in step.

**Bar.** A summary of the trades in one time window: first price (open), highest, lowest,
last (close), total quantity (volume). Also called a candle or OHLCV.

**Basis point (bps).** One hundredth of a percent. 10 bps = 0.1%.

**Best bid / best offer (BBO).** The highest price anyone will currently pay and the lowest
price anyone will currently sell at. Also called top of book.

**Broker.** A Kafka server.

**Checkpoint.** A snapshot Flink takes of a running job: its state and its position in each
Kafka topic. A job restarted from a checkpoint carries on as if it had never stopped.

**Collector.** A program that holds a connection to a venue and copies what arrives into
Kafka.

**Control message.** Anything from a venue that is not a trade or a book update: heartbeats,
replies to subscriptions, status notices.

**DAG.** Directed acyclic graph: Airflow's word for a set of tasks and the order they must
run in.

**dbt.** A tool that builds tables from SQL `SELECT` statements (models) in dependency order
and runs tests on the results.

**DuckDB.** A database that runs inside the program using it, with no server, storing
everything in one file. Good at analysing files of data.

**Event time.** When something happened according to its source, as opposed to *processing
time*, when our system handled it. Bars are built on event time.

**Gauge.** A Prometheus measurement that can go up or down (a delay, a count in the last
hour), as opposed to a *counter*, which only ever rises.

**Heartbeat.** A message sent on a schedule to show the connection is alive. Silence longer
than the schedule means the connection is dead even if it looks open.

**Hub (SignalR).** The server-side endpoint browsers connect to; the server sends named
messages to everyone connected to it.

**Idempotent producer.** A Kafka client setting that lets the broker discard a message it has
already stored, so a retry after a network error cannot create a duplicate or reorder.

**KRaft.** Kafka's built-in way of running its own cluster metadata, replacing ZooKeeper.

**Late event.** A message that arrives after the watermark has passed its event time. Its
window may already have been emitted without it.

**Maker / taker.** The maker's order was resting in the book; the taker's order arrived and
matched it. A trade's "side" here is always the taker's.

**Materialized view (ClickHouse).** A trigger: every batch inserted into one table is
transformed and inserted into another.

**Order book.** The list of all outstanding buy and sell orders for a pair, by price.

**Pair.** Two currencies traded against each other, such as BTC-USD.

**Partition.** An ordered log inside a Kafka topic. Order is guaranteed within a partition,
not across partitions.

**Publish/subscribe.** A sender posts messages to a named channel without knowing who is
listening; every current listener gets a copy. Nothing is stored.

**Reconciliation.** Computing the same result two independent ways and explaining every
difference.

**ReplacingMergeTree.** A ClickHouse table type in which rows with the same key are
eventually collapsed into one. `FINAL` in a query forces the collapse at read time.

**Retention.** How long (or how many bytes) Kafka keeps messages before deleting them.

**Scrape.** Prometheus collecting numbers by requesting a service's `/metrics` page on a
schedule.

**SignalR.** ASP.NET Core's library for pushing messages from server to browser over a
WebSocket, with fallbacks and automatic reconnection.

**Stale.** A connection that is open but has delivered nothing for longer than expected.

**State.** What a streaming job must remember between messages: open windows, trade ids
already seen, the previous id per pair.

**Topic.** A named stream of messages in Kafka.

**Trade.** One exchange of a quantity at a price between a buyer and a seller. Venues give
each a trade id; consecutive ids make missing trades detectable.

**Tumbling window.** Fixed, back-to-back time slices (every second, every minute). Each
event belongs to exactly one.

**Venue.** An exchange: here Coinbase, Kraken or Bitstamp.

**VWAP.** Volume-weighted average price: total value traded divided by total quantity.

**Watermark.** Flink's running claim that "nothing older than this time is still coming".
A window is emitted when the watermark passes its end.

**WebSocket.** A long-lived two-way connection over which a server can push messages.
