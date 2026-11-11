-- The same table names as sql/tables.sql, but backed by files instead of
-- Kafka, so the job files (normalise.sql, process.sql) run unchanged against
-- fixtures. Keep the columns here in step with sql/tables.sql.

SET 'execution.target' = 'local';
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'table.dml-sync' = 'true';
SET 'table.local-time-zone' = 'UTC';

-- ----------------------------------------------------------------- inputs
CREATE TABLE raw_coinbase_trades (
  `type` STRING, trade_id BIGINT, side STRING, size STRING, price STRING,
  product_id STRING, `time` STRING,
  recv_time AS CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP_LTZ(3))
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/coinbase_trades', 'format' = 'json');

CREATE TABLE raw_coinbase_book (
  `type` STRING, product_id STRING, best_bid STRING, best_bid_size STRING,
  best_ask STRING, best_ask_size STRING, `time` STRING,
  recv_time AS CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP_LTZ(3))
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/coinbase_book', 'format' = 'json');

CREATE TABLE raw_kraken_trades (
  channel STRING, `type` STRING,
  data ARRAY<ROW<symbol STRING, side STRING, price DECIMAL(24, 8), qty DECIMAL(24, 8),
                 trade_id BIGINT, `timestamp` STRING>>,
  recv_time AS CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP_LTZ(3))
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/kraken_trades', 'format' = 'json');

CREATE TABLE raw_kraken_book (
  channel STRING,
  data ARRAY<ROW<symbol STRING, bid DECIMAL(24, 8), bid_qty DECIMAL(24, 8),
                 ask DECIMAL(24, 8), ask_qty DECIMAL(24, 8), `timestamp` STRING>>,
  recv_time AS CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP_LTZ(3))
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/kraken_book', 'format' = 'json');

CREATE TABLE raw_bitstamp_trades (
  event STRING, channel STRING,
  data ROW<id BIGINT, amount_str STRING, price_str STRING, `type` INT, microtimestamp STRING>,
  recv_time AS CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP_LTZ(3))
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/bitstamp_trades', 'format' = 'json');

CREATE TABLE raw_bitstamp_book (
  event STRING, channel STRING,
  data ROW<microtimestamp STRING, bids ARRAY<ARRAY<STRING>>, asks ARRAY<ARRAY<STRING>>>,
  recv_time AS CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP_LTZ(3))
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/bitstamp_book', 'format' = 'json');

CREATE TABLE trades (
  venue STRING, pair STRING, trade_id BIGINT, price DECIMAL(24, 8), qty DECIMAL(24, 8),
  side STRING, event_time TIMESTAMP(3), recv_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/in/trades', 'format' = 'json');

-- ---------------------------------------------------------------- outputs
CREATE TABLE trades_out_coinbase WITH (
  'connector' = 'filesystem', 'path' = '/tmp/t/out/trades_coinbase', 'format' = 'json',
  'json.encode.decimal-as-plain-number' = 'true'
) LIKE trades (EXCLUDING OPTIONS EXCLUDING WATERMARKS);

CREATE TABLE trades_out_kraken WITH (
  'connector' = 'filesystem', 'path' = '/tmp/t/out/trades_kraken', 'format' = 'json',
  'json.encode.decimal-as-plain-number' = 'true'
) LIKE trades (EXCLUDING OPTIONS EXCLUDING WATERMARKS);

CREATE TABLE trades_out_bitstamp WITH (
  'connector' = 'filesystem', 'path' = '/tmp/t/out/trades_bitstamp', 'format' = 'json',
  'json.encode.decimal-as-plain-number' = 'true'
) LIKE trades (EXCLUDING OPTIONS EXCLUDING WATERMARKS);

CREATE TABLE quotes (
  venue STRING, pair STRING, bid DECIMAL(24, 8), bid_qty DECIMAL(24, 8),
  ask DECIMAL(24, 8), ask_qty DECIMAL(24, 8), event_time TIMESTAMP(3), recv_time TIMESTAMP(3)
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/out/quotes', 'format' = 'json',
        'json.encode.decimal-as-plain-number' = 'true');

CREATE TABLE bars_1s (
  venue STRING, pair STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  `open` DECIMAL(24, 8), high DECIMAL(24, 8), low DECIMAL(24, 8), `close` DECIMAL(24, 8),
  volume DECIMAL(38, 8), vwap DECIMAL(24, 8), trades BIGINT,
  first_trade_id BIGINT, last_trade_id BIGINT
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/out/bars_1s', 'format' = 'json',
        'json.encode.decimal-as-plain-number' = 'true');

CREATE TABLE bars_1m WITH ('path' = '/tmp/t/out/bars_1m') LIKE bars_1s;

CREATE TABLE late_trades (
  venue STRING, pair STRING, trade_id BIGINT, price DECIMAL(24, 8), qty DECIMAL(24, 8),
  event_time TIMESTAMP(3), recv_time TIMESTAMP(3), watermark_time TIMESTAMP(3),
  missed_1s BOOLEAN, missed_1m BOOLEAN
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/out/late_trades', 'format' = 'json',
        'json.encode.decimal-as-plain-number' = 'true');

CREATE TABLE gaps (
  venue STRING, pair STRING, prev_trade_id BIGINT, trade_id BIGINT, missing BIGINT,
  event_time TIMESTAMP(3), detected_at TIMESTAMP(3)
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/out/gaps', 'format' = 'json');

CREATE TABLE venue_checks (
  pair STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3), venues BIGINT,
  min_vwap DECIMAL(24, 8), max_vwap DECIMAL(24, 8), spread_bps DECIMAL(12, 4), detail STRING
) WITH ('connector' = 'filesystem', 'path' = '/tmp/t/out/venue_checks', 'format' = 'json',
        'json.encode.decimal-as-plain-number' = 'true');
