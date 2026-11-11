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
