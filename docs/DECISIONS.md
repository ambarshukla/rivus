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
