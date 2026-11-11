-- Table definitions shared by every job. A table here is only a description of
-- a Kafka topic: nothing is read or written until a job uses it.

SET 'table.local-time-zone' = 'UTC';
-- Every 30 seconds each job saves its state and its position in Kafka (a
-- checkpoint). These are job settings: they must be set here, by the client
-- that submits the job, not only in the cluster's configuration.
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
-- Forget per-key state (seen trade ids, previous trade id) after an hour without updates.
SET 'table.exec.state.ttl' = '1 h';

-- ============================================================ raw sources
-- The JSON format picks the named fields out of each raw message and ignores
-- the rest. recv_time is the Kafka record timestamp set by the collector.

CREATE TABLE raw_coinbase_trades (
  `type`     STRING,
  trade_id   BIGINT,
  side       STRING,
  size       STRING,
  price      STRING,
  product_id STRING,
  `time`     STRING,
  recv_time  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka', 'topic' = 'raw.coinbase.trades',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-normalise',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TABLE raw_coinbase_book (
  `type`        STRING,
  product_id    STRING,
  best_bid      STRING,
  best_bid_size STRING,
  best_ask      STRING,
  best_ask_size STRING,
  `time`        STRING,
  recv_time     TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka', 'topic' = 'raw.coinbase.book',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-normalise',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

-- Kraken puts several trades in one message, in the "data" array.
CREATE TABLE raw_kraken_trades (
  channel   STRING,
  `type`    STRING,
  data      ARRAY<ROW<symbol STRING, side STRING, price DECIMAL(24, 8), qty DECIMAL(24, 8),
                      trade_id BIGINT, `timestamp` STRING>>,
  recv_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka', 'topic' = 'raw.kraken.trades',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-normalise',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TABLE raw_kraken_book (
  channel   STRING,
  data      ARRAY<ROW<symbol STRING, bid DECIMAL(24, 8), bid_qty DECIMAL(24, 8),
                      ask DECIMAL(24, 8), ask_qty DECIMAL(24, 8), `timestamp` STRING>>,
  recv_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka', 'topic' = 'raw.kraken.book',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-normalise',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

CREATE TABLE raw_bitstamp_trades (
  event     STRING,
  channel   STRING,
  data      ROW<id BIGINT, amount_str STRING, price_str STRING, `type` INT, microtimestamp STRING>,
  recv_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka', 'topic' = 'raw.bitstamp.trades',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-normalise',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

-- Each message is the top 100 levels per side as [price, quantity] pairs, best first.
CREATE TABLE raw_bitstamp_book (
  event     STRING,
  channel   STRING,
  data      ROW<microtimestamp STRING, bids ARRAY<ARRAY<STRING>>, asks ARRAY<ARRAY<STRING>>>,
  recv_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka', 'topic' = 'raw.bitstamp.book',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-normalise',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  'format' = 'json', 'json.ignore-parse-errors' = 'true'
);

-- ===================================================== normalised topics
-- Written by the normalise job, read by the process job, ClickHouse and the
-- latest-price writer. All times are UTC.

CREATE TABLE trades (
  venue      STRING,
  pair       STRING,
  trade_id   BIGINT,
  price      DECIMAL(24, 8),
  qty        DECIMAL(24, 8),
  side       STRING,          -- the side of the taker: 'buy' or 'sell'
  event_time TIMESTAMP(3),    -- when the venue says the trade happened
  recv_time  TIMESTAMP(3),    -- when the collector received it
  -- The watermark says "no trade older than this is still expected". Each
  -- venue's topic gets its own, trailing that venue's newest trade by 2
  -- seconds; the job uses the lowest of them. Anything older is late.
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka', 'topic-pattern' = 'rivus\.trades\..*',
  'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'rivus-process',
  'scan.startup.mode' = 'group-offsets', 'properties.auto.offset.reset' = 'earliest',
  -- A venue with no trades for 2 seconds stops holding the watermark back.
  'scan.watermark.idle-timeout' = '2 s',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
);

CREATE TABLE trades_out_coinbase WITH (
  'connector' = 'kafka', 'topic' = 'rivus.trades.coinbase',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
) LIKE trades (EXCLUDING OPTIONS EXCLUDING WATERMARKS);

CREATE TABLE trades_out_kraken WITH (
  'connector' = 'kafka', 'topic' = 'rivus.trades.kraken',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
) LIKE trades (EXCLUDING OPTIONS EXCLUDING WATERMARKS);

CREATE TABLE trades_out_bitstamp WITH (
  'connector' = 'kafka', 'topic' = 'rivus.trades.bitstamp',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
) LIKE trades (EXCLUDING OPTIONS EXCLUDING WATERMARKS);

CREATE TABLE quotes (
  venue      STRING,
  pair       STRING,
  bid        DECIMAL(24, 8),
  bid_qty    DECIMAL(24, 8),
  ask        DECIMAL(24, 8),
  ask_qty    DECIMAL(24, 8),
  event_time TIMESTAMP(3),
  recv_time  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'rivus.quotes',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
);

-- ========================================================= derived topics

CREATE TABLE bars_1s (
  venue          STRING,
  pair           STRING,
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  `open`         DECIMAL(24, 8),
  high           DECIMAL(24, 8),
  low            DECIMAL(24, 8),
  `close`        DECIMAL(24, 8),
  volume         DECIMAL(38, 8),
  vwap           DECIMAL(24, 8),
  trades         BIGINT,
  first_trade_id BIGINT,
  last_trade_id  BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'rivus.bars_1s',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
);

CREATE TABLE bars_1m WITH ('topic' = 'rivus.bars_1m') LIKE bars_1s;

CREATE TABLE late_trades (
  venue          STRING,
  pair           STRING,
  trade_id       BIGINT,
  price          DECIMAL(24, 8),
  qty            DECIMAL(24, 8),
  event_time     TIMESTAMP(3),
  recv_time      TIMESTAMP(3),
  watermark_time TIMESTAMP(3),  -- the watermark when the trade arrived
  missed_1s      BOOLEAN,       -- its 1-second bar had already been emitted
  missed_1m      BOOLEAN        -- its 1-minute bar had already been emitted
) WITH (
  'connector' = 'kafka', 'topic' = 'rivus.late_trades',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
);

CREATE TABLE gaps (
  venue         STRING,
  pair          STRING,
  prev_trade_id BIGINT,
  trade_id      BIGINT,
  missing       BIGINT,        -- how many trade ids were skipped
  event_time    TIMESTAMP(3),  -- time of the trade after the gap
  detected_at   TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'rivus.gaps',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE venue_checks (
  pair         STRING,
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  venues       BIGINT,          -- how many venues traded in the minute
  min_vwap     DECIMAL(24, 8),
  max_vwap     DECIMAL(24, 8),
  spread_bps   DECIMAL(12, 4),  -- (max - min) / min, in hundredths of a percent
  detail       STRING           -- 'venue=vwap' for each venue
) WITH (
  'connector' = 'kafka', 'topic' = 'rivus.venue_checks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'json.encode.decimal-as-plain-number' = 'true'
);
