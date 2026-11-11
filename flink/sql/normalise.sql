-- Job 1: turn each venue's raw messages into one schema and write the result
-- to the rivus.trades.<venue> and rivus.quotes topics. The job keeps no state:
-- one message in, one or more rows out.

SET 'pipeline.name' = 'rivus-normalise';

CREATE TEMPORARY VIEW trades_coinbase (venue, pair, trade_id, price, qty, side, event_time, recv_time) AS
SELECT
  'coinbase'                            AS venue,
  product_id                            AS pair,
  trade_id,
  CAST(price AS DECIMAL(24, 8))         AS price,
  CAST(size AS DECIMAL(24, 8))          AS qty,
  -- Coinbase reports the side of the resting (maker) order; the taker is the opposite.
  CASE side WHEN 'buy' THEN 'sell' ELSE 'buy' END AS side,
  TO_TIMESTAMP(REPLACE(REPLACE(`time`, 'T', ' '), 'Z', '')) AS event_time,
  CAST(recv_time AS TIMESTAMP(3))       AS recv_time
FROM raw_coinbase_trades
WHERE `type` IN ('match', 'last_match');

CREATE TEMPORARY VIEW trades_kraken (venue, pair, trade_id, price, qty, side, event_time, recv_time) AS
SELECT
  'kraken',
  REPLACE(t.symbol, '/', '-'),
  t.trade_id,
  t.price,
  t.qty,
  t.side,
  TO_TIMESTAMP(REPLACE(REPLACE(t.`timestamp`, 'T', ' '), 'Z', '')),
  CAST(recv_time AS TIMESTAMP(3))
FROM raw_kraken_trades
CROSS JOIN UNNEST(data) AS t (symbol, side, price, qty, trade_id, `timestamp`)
WHERE channel = 'trade';

CREATE TEMPORARY VIEW trades_bitstamp (venue, pair, trade_id, price, qty, side, event_time, recv_time) AS
SELECT
  'bitstamp',
  -- channel is 'live_trades_btcusd': characters 13-15 are the base currency.
  UPPER(SUBSTRING(channel FROM 13 FOR 3)) || '-' || UPPER(SUBSTRING(channel FROM 16)),
  data.id,
  CAST(data.price_str AS DECIMAL(24, 8)),
  CAST(data.amount_str AS DECIMAL(24, 8)),
  CASE data.`type` WHEN 0 THEN 'buy' ELSE 'sell' END,
  CAST(TO_TIMESTAMP_LTZ(CAST(data.microtimestamp AS BIGINT) / 1000, 3) AS TIMESTAMP(3)),
  CAST(recv_time AS TIMESTAMP(3))
FROM raw_bitstamp_trades
WHERE event = 'trade';

CREATE TEMPORARY VIEW quotes_all AS
SELECT
  'coinbase'                             AS venue,
  product_id                             AS pair,
  CAST(best_bid AS DECIMAL(24, 8))       AS bid,
  CAST(best_bid_size AS DECIMAL(24, 8))  AS bid_qty,
  CAST(best_ask AS DECIMAL(24, 8))       AS ask,
  CAST(best_ask_size AS DECIMAL(24, 8))  AS ask_qty,
  TO_TIMESTAMP(REPLACE(REPLACE(`time`, 'T', ' '), 'Z', '')) AS event_time,
  CAST(recv_time AS TIMESTAMP(3))        AS recv_time
FROM raw_coinbase_book
WHERE `type` = 'ticker'

UNION ALL
SELECT
  'kraken',
  REPLACE(q.symbol, '/', '-'),
  q.bid, q.bid_qty, q.ask, q.ask_qty,
  TO_TIMESTAMP(REPLACE(REPLACE(q.`timestamp`, 'T', ' '), 'Z', '')),
  CAST(recv_time AS TIMESTAMP(3))
FROM raw_kraken_book
CROSS JOIN UNNEST(data) AS q (symbol, bid, bid_qty, ask, ask_qty, `timestamp`)
WHERE channel = 'ticker'

UNION ALL
SELECT
  'bitstamp',
  -- channel is 'order_book_btcusd': characters 12-14 are the base currency.
  UPPER(SUBSTRING(channel FROM 12 FOR 3)) || '-' || UPPER(SUBSTRING(channel FROM 15)),
  CAST(data.bids[1][1] AS DECIMAL(24, 8)),   -- arrays start at 1; [1] is the best level
  CAST(data.bids[1][2] AS DECIMAL(24, 8)),
  CAST(data.asks[1][1] AS DECIMAL(24, 8)),
  CAST(data.asks[1][2] AS DECIMAL(24, 8)),
  CAST(TO_TIMESTAMP_LTZ(CAST(data.microtimestamp AS BIGINT) / 1000, 3) AS TIMESTAMP(3)),
  CAST(recv_time AS TIMESTAMP(3))
FROM raw_bitstamp_book
WHERE event = 'data' AND CARDINALITY(data.bids) > 0 AND CARDINALITY(data.asks) > 0;

EXECUTE STATEMENT SET
BEGIN

-- Each venue is written to its own topic (rivus.trades.<venue>), which keeps
-- that venue's trades in the order they happened. Duplicates are not removed
-- here; the process job does that. See docs/DECISIONS.md, 8 and 9.
INSERT INTO trades_out_coinbase SELECT * FROM trades_coinbase WHERE trade_id IS NOT NULL;
INSERT INTO trades_out_kraken   SELECT * FROM trades_kraken   WHERE trade_id IS NOT NULL;
INSERT INTO trades_out_bitstamp SELECT * FROM trades_bitstamp WHERE trade_id IS NOT NULL;

INSERT INTO quotes
SELECT venue, pair, bid, bid_qty, ask, ask_qty, event_time, recv_time
FROM quotes_all
WHERE bid IS NOT NULL AND ask IS NOT NULL;

END;
