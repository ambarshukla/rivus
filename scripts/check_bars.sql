-- Run with:
--   docker compose exec -T clickhouse clickhouse-client --user rivus --password rivus < scripts/check_bars.sql
--
-- Rebuilds every 1-minute and 1-second bar from the stored trades and compares
-- it with the bar the stream produced. Only windows that ended more than 30
-- seconds ago are checked. Trades the stream flagged as too late for their
-- bar are left out of the rebuild, since the stream left them out too.
-- Every "bad" count should be 0.
WITH
late_1m AS (SELECT venue, pair, trade_id FROM rivus.late_trades FINAL WHERE missed_1m),
late_1s AS (SELECT venue, pair, trade_id FROM rivus.late_trades FINAL WHERE missed_1s),
rebuilt_1m AS (
  SELECT venue, pair, toStartOfMinute(event_time) AS ws,
         argMin(price, (event_time, trade_id)) AS open, max(price) AS high, min(price) AS low,
         argMax(price, (event_time, trade_id)) AS close, sum(qty) AS volume, count() AS trades
  FROM rivus.trades FINAL
  WHERE (venue, pair, trade_id) NOT IN (SELECT * FROM late_1m)
  GROUP BY venue, pair, ws
),
rebuilt_1s AS (
  SELECT venue, pair, toStartOfSecond(event_time) AS ws,
         max(price) AS high, min(price) AS low, sum(qty) AS volume, count() AS trades
  FROM rivus.trades FINAL
  WHERE (venue, pair, trade_id) NOT IN (SELECT * FROM late_1s)
  GROUP BY venue, pair, ws
)
SELECT '1m' AS bar, b.venue AS venue, count() AS bars,
       countIf(r.trades = 0) AS bad_no_trades,
       countIf(r.trades != 0 AND b.trades != r.trades) AS bad_count,
       countIf(r.trades != 0 AND b.volume != r.volume) AS bad_volume,
       countIf(r.trades != 0 AND (b.high != r.high OR b.low != r.low)) AS bad_high_low,
       countIf(r.trades != 0 AND (b.open != r.open OR b.close != r.close)) AS bad_open_close
FROM rivus.bars_1m AS b FINAL
LEFT JOIN rebuilt_1m AS r ON b.venue = r.venue AND b.pair = r.pair AND toDateTime(b.window_start) = r.ws
WHERE b.window_end < now() - INTERVAL 30 SECOND
GROUP BY venue
UNION ALL
SELECT '1s', b.venue, count(),
       countIf(r.trades = 0),
       countIf(r.trades != 0 AND b.trades != r.trades),
       countIf(r.trades != 0 AND b.volume != r.volume),
       countIf(r.trades != 0 AND (b.high != r.high OR b.low != r.low)),
       0
FROM rivus.bars_1s AS b FINAL
LEFT JOIN rebuilt_1s AS r ON b.venue = r.venue AND b.pair = r.pair AND b.window_start = r.ws
WHERE b.window_end < now() - INTERVAL 30 SECOND
GROUP BY b.venue
ORDER BY 1, 2
FORMAT PrettyCompactMonoBlock;
