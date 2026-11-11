# Provenance

Every external data source, what is taken from it, and the status of its terms.

**No venue's terms have been confirmed as permitting this use.** The project therefore runs
on one machine only, commits no fetched data, and publishes nothing derived from venue data.

| Venue | Endpoint | Channels used | Terms | Status |
|---|---|---|---|---|
| Coinbase Exchange | `wss://ws-feed.exchange.coinbase.com` | `matches`, `ticker`, `heartbeat` | <https://www.coinbase.com/legal/market_data> | not yet confirmed |
| Kraken | `wss://ws.kraken.com/v2` | `trade`, `ticker` | <https://www.kraken.com/legal> | not yet confirmed |
| Bitstamp | `wss://ws.bitstamp.net` | `live_trades_*`, `order_book_*` | <https://www.bitstamp.net/legal/terms-of-use/> | not yet confirmed |

Pairs: BTC-USD and ETH-USD on each venue. All channels are public and documented and need no
account or key.

## How the feeds are used

- One WebSocket connection per venue.
- Reconnects back off exponentially (1 second doubling to 60) with jitter.
- The client identifies itself with a generic `User-Agent` and sends no personal details.
- Only the subscriptions listed in `collectors/venues/*.yaml` are sent.

## Data kept in the repository

`collectors/tests/fixtures/<venue>/{trades,book,control}.jsonl`: a handful of messages per
venue (at most five per file) captured from the live feeds on 2026-11-11, used to test that
each venue's config sorts messages correctly. `flink/tests/fixtures/raw/` and
`batch/tests/fixtures/` hold copies of the same files for the SQL and dbt tests. Nothing else fetched from a venue is committed;
`data/` is ignored by git and Kafka's storage is a Docker volume.
