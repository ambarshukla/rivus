-- Job 2: read normalised trades, drop duplicates, and produce bars, late
-- trades, sequence gaps and the cross-venue price check.

SET 'pipeline.name' = 'rivus-process';

-- Keep the first copy of each trade. Duplicates are normal: Kraken replays its
-- last 50 trades on every connect, and the normalise job can write a trade
-- twice after a restart. Ordering by processing time and keeping row 1 means
-- "first to arrive wins" and never changes its mind, so the result is still a
-- plain stream of inserts with its event time intact.
CREATE TEMPORARY VIEW trades_once AS
SELECT venue, pair, trade_id, price, qty, side, event_time, recv_time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY venue, pair, trade_id ORDER BY proc_time ASC) AS row_num
  FROM (SELECT *, PROCTIME() AS proc_time FROM trades)
)
WHERE row_num = 1;

-- One-minute bars as a view, because both the bars topic and the cross-venue
-- check read them. window_time keeps event time flowing to the next window.
CREATE TEMPORARY VIEW v_bars_1m AS
SELECT
  venue, pair, window_start AS bar_start, window_end AS bar_end, window_time AS bar_time,
  FIRST_VALUE(price)                                      AS `open`,
  MAX(price)                                              AS high,
  MIN(price)                                              AS low,
  LAST_VALUE(price)                                       AS `close`,
  SUM(qty)                                                AS volume,
  CAST(SUM(price * qty) / SUM(qty) AS DECIMAL(24, 8))     AS vwap,
  COUNT(*)                                                AS trades,
  MIN(trade_id)                                           AS first_trade_id,
  MAX(trade_id)                                           AS last_trade_id
FROM TABLE(TUMBLE(TABLE trades_once, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY venue, pair, window_start, window_end, window_time;

EXECUTE STATEMENT SET
BEGIN

-- A bar is emitted once, when the watermark passes the end of its window.
INSERT INTO bars_1s
SELECT
  venue, pair, window_start, window_end,
  FIRST_VALUE(price), MAX(price), MIN(price), LAST_VALUE(price),
  SUM(qty),
  CAST(SUM(price * qty) / SUM(qty) AS DECIMAL(24, 8)),
  COUNT(*), MIN(trade_id), MAX(trade_id)
FROM TABLE(TUMBLE(TABLE trades_once, DESCRIPTOR(event_time), INTERVAL '1' SECOND))
GROUP BY venue, pair, window_start, window_end;

INSERT INTO bars_1m
SELECT venue, pair, bar_start, bar_end, `open`, high, low, `close`,
       volume, vwap, trades, first_trade_id, last_trade_id
FROM v_bars_1m;

-- Trades older than the watermark. A late trade still makes it into a bar if
-- that bar's window has not closed yet; the two flags say whether it did not.
INSERT INTO late_trades
SELECT venue, pair, trade_id, price, qty, event_time, recv_time, watermark_time,
       FLOOR(event_time TO SECOND) + INTERVAL '0.999' SECOND <= watermark_time,
       FLOOR(event_time TO MINUTE) + INTERVAL '59.999' SECOND <= watermark_time
FROM (SELECT *, CURRENT_WATERMARK(event_time) AS watermark_time FROM trades_once)
WHERE event_time <= watermark_time;

-- Coinbase and Kraken number trades 1, 2, 3... per pair, so a jump means
-- trades were missed. Bitstamp shares one counter across all its pairs, so
-- a jump there means nothing and it is left out. Trades are compared in
-- arrival order (processing time): ids are issued in order, so a jump is a gap.
INSERT INTO gaps
SELECT venue, pair, prev_trade_id, trade_id, trade_id - prev_trade_id - 1,
       event_time, CAST(proc_time AS TIMESTAMP(3))
FROM (
  SELECT venue, pair, trade_id, event_time, proc_time,
         LAG(trade_id) OVER (PARTITION BY venue, pair ORDER BY proc_time) AS prev_trade_id
  FROM (SELECT *, PROCTIME() AS proc_time FROM trades_once WHERE venue IN ('coinbase', 'kraken'))
)
WHERE trade_id - prev_trade_id > 1;

-- Each minute, per pair: how far apart are the venues' average traded prices?
INSERT INTO venue_checks
SELECT
  pair, window_start, window_end,
  COUNT(*),
  MIN(vwap), MAX(vwap),
  CAST((MAX(vwap) - MIN(vwap)) / MIN(vwap) * 10000 AS DECIMAL(12, 4)),
  LISTAGG(venue || '=' || CAST(vwap AS STRING), ' ')
FROM TABLE(TUMBLE(TABLE v_bars_1m, DESCRIPTOR(bar_time), INTERVAL '1' MINUTE))
GROUP BY pair, window_start, window_end;

END;
