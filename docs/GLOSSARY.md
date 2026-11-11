# Glossary

**Backoff.** Waiting longer after each failed attempt (here 1 second doubling to 60) so a
struggling server is not hammered. *Jitter* is a random factor on the wait so that several
clients do not retry in step.

**Best bid / best offer (BBO).** The highest price anyone will currently pay and the lowest
price anyone will currently sell at. Also called top of book.

**Broker.** A Kafka server.

**Collector.** A program that holds a connection to a venue and copies what arrives into
Kafka.

**Control message.** Anything from a venue that is not a trade or a book update: heartbeats,
replies to subscriptions, status notices.

**Heartbeat.** A message sent on a schedule to show the connection is alive. Silence longer
than the schedule means the connection is dead even if it looks open.

**Idempotent producer.** A Kafka client setting that lets the broker discard a message it has
already stored, so a retry after a network error cannot create a duplicate or reorder.

**KRaft.** Kafka's built-in way of running its own cluster metadata, replacing ZooKeeper.

**Order book.** The list of all outstanding buy and sell orders for a pair, by price.

**Pair.** Two currencies traded against each other, such as BTC-USD.

**Partition.** An ordered log inside a Kafka topic. Order is guaranteed within a partition,
not across partitions.

**Retention.** How long (or how many bytes) Kafka keeps messages before deleting them.

**Stale.** A connection that is open but has delivered nothing for longer than expected.

**Topic.** A named stream of messages in Kafka.

**Trade.** One exchange of a quantity at a price between a buyer and a seller. Venues give
each a trade id; consecutive ids make missing trades detectable.

**Venue.** An exchange: here Coinbase, Kraken or Bitstamp.

**WebSocket.** A long-lived two-way connection over which a server can push messages.
