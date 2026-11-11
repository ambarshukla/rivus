-- ClickHouse pulls from Kafka by itself. For each topic there are three objects:
--   <name>_queue  a Kafka engine table: a consumer, holds no data
--   <name>        the real table, stored on disk
--   <name>_mv     a materialized view that copies every batch from queue to table
--
-- The stream can deliver a row twice (after a restart it re-reads a little),
-- so the tables are ReplacingMergeTree: rows with the same ORDER BY key
-- collapse into one. Collapsing happens in the background, so queries that
-- must not see duplicates add FINAL.

CREATE DATABASE IF NOT EXISTS rivus;

-- ---------------------------------------------------------------- trades
CREATE TABLE IF NOT EXISTS rivus.trades_queue (
  venue String, pair String, trade_id UInt64,
  price Decimal(24, 8), qty Decimal(24, 8), side String,
  event_time DateTime64(3, 'UTC'), recv_time DateTime64(3, 'UTC')
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.trades.coinbase,rivus.trades.kraken,rivus.trades.bitstamp',
  kafka_group_name = 'clickhouse-trades', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.trades (
  venue LowCardinality(String), pair LowCardinality(String), trade_id UInt64,
  price Decimal(24, 8), qty Decimal(24, 8), side LowCardinality(String),
  event_time DateTime64(3, 'UTC'), recv_time DateTime64(3, 'UTC'),
  stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_time)
ORDER BY (venue, pair, trade_id)
TTL toDateTime(event_time) + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.trades_mv TO rivus.trades AS
SELECT * FROM rivus.trades_queue;

-- ---------------------------------------------------------------- quotes
CREATE TABLE IF NOT EXISTS rivus.quotes_queue (
  venue String, pair String,
  bid Decimal(24, 8), bid_qty Decimal(24, 8), ask Decimal(24, 8), ask_qty Decimal(24, 8),
  event_time DateTime64(3, 'UTC'), recv_time DateTime64(3, 'UTC')
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.quotes',
  kafka_group_name = 'clickhouse-quotes', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.quotes (
  venue LowCardinality(String), pair LowCardinality(String),
  bid Decimal(24, 8), bid_qty Decimal(24, 8), ask Decimal(24, 8), ask_qty Decimal(24, 8),
  event_time DateTime64(3, 'UTC'), recv_time DateTime64(3, 'UTC')
) ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (venue, pair, event_time)
TTL toDateTime(event_time) + INTERVAL 3 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.quotes_mv TO rivus.quotes AS
SELECT * FROM rivus.quotes_queue;

-- ------------------------------------------------------------------ bars
-- ReplacingMergeTree(trades): of two copies of a bar, keep the one built from
-- more trades. A bar re-emitted after a restart can be partial; the complete
-- one must win.
CREATE TABLE IF NOT EXISTS rivus.bars_1s_queue (
  venue String, pair String,
  window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
  open Decimal(24, 8), high Decimal(24, 8), low Decimal(24, 8), close Decimal(24, 8),
  volume Decimal(38, 8), vwap Decimal(24, 8),
  trades UInt64, first_trade_id UInt64, last_trade_id UInt64
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.bars_1s',
  kafka_group_name = 'clickhouse-bars-1s', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.bars_1s (
  venue LowCardinality(String), pair LowCardinality(String),
  window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
  open Decimal(24, 8), high Decimal(24, 8), low Decimal(24, 8), close Decimal(24, 8),
  volume Decimal(38, 8), vwap Decimal(24, 8),
  trades UInt64, first_trade_id UInt64, last_trade_id UInt64,
  stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(trades)
PARTITION BY toDate(window_start)
ORDER BY (venue, pair, window_start)
TTL toDateTime(window_start) + INTERVAL 7 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.bars_1s_mv TO rivus.bars_1s AS
SELECT * FROM rivus.bars_1s_queue;

CREATE TABLE IF NOT EXISTS rivus.bars_1m_queue AS rivus.bars_1s_queue
ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.bars_1m',
  kafka_group_name = 'clickhouse-bars-1m', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.bars_1m (
  venue LowCardinality(String), pair LowCardinality(String),
  window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
  open Decimal(24, 8), high Decimal(24, 8), low Decimal(24, 8), close Decimal(24, 8),
  volume Decimal(38, 8), vwap Decimal(24, 8),
  trades UInt64, first_trade_id UInt64, last_trade_id UInt64,
  stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(trades)
PARTITION BY toYYYYMM(window_start)
ORDER BY (venue, pair, window_start)
TTL toDateTime(window_start) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.bars_1m_mv TO rivus.bars_1m AS
SELECT * FROM rivus.bars_1m_queue;

-- ----------------------------------------------------------- late trades
CREATE TABLE IF NOT EXISTS rivus.late_trades_queue (
  venue String, pair String, trade_id UInt64,
  price Decimal(24, 8), qty Decimal(24, 8),
  event_time DateTime64(3, 'UTC'), recv_time DateTime64(3, 'UTC'),
  watermark_time DateTime64(3, 'UTC'), missed_1s Bool, missed_1m Bool
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.late_trades',
  kafka_group_name = 'clickhouse-late-trades', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.late_trades (
  venue LowCardinality(String), pair LowCardinality(String), trade_id UInt64,
  price Decimal(24, 8), qty Decimal(24, 8),
  event_time DateTime64(3, 'UTC'), recv_time DateTime64(3, 'UTC'),
  watermark_time DateTime64(3, 'UTC'), missed_1s Bool, missed_1m Bool,
  stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (venue, pair, trade_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.late_trades_mv TO rivus.late_trades AS
SELECT * FROM rivus.late_trades_queue;

-- ------------------------------------------------------------------ gaps
CREATE TABLE IF NOT EXISTS rivus.gaps_queue (
  venue String, pair String, prev_trade_id UInt64, trade_id UInt64, missing UInt64,
  event_time DateTime64(3, 'UTC'), detected_at DateTime64(3, 'UTC')
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.gaps',
  kafka_group_name = 'clickhouse-gaps', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.gaps (
  venue LowCardinality(String), pair LowCardinality(String),
  prev_trade_id UInt64, trade_id UInt64, missing UInt64,
  event_time DateTime64(3, 'UTC'), detected_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (venue, pair, trade_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.gaps_mv TO rivus.gaps AS
SELECT * FROM rivus.gaps_queue;

-- ---------------------------------------------------------- venue checks
CREATE TABLE IF NOT EXISTS rivus.venue_checks_queue (
  pair String, window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
  venues UInt64, min_vwap Decimal(24, 8), max_vwap Decimal(24, 8),
  spread_bps Decimal(12, 4), detail String
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'kafka:9092', kafka_topic_list = 'rivus.venue_checks',
  kafka_group_name = 'clickhouse-venue-checks', kafka_format = 'JSONEachRow',
  kafka_flush_interval_ms = 1000;

CREATE TABLE IF NOT EXISTS rivus.venue_checks (
  pair LowCardinality(String),
  window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
  venues UInt64, min_vwap Decimal(24, 8), max_vwap Decimal(24, 8),
  spread_bps Decimal(12, 4), detail String
) ENGINE = ReplacingMergeTree(venues)
PARTITION BY toYYYYMM(window_start)
ORDER BY (pair, window_start)
TTL toDateTime(window_start) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS rivus.venue_checks_mv TO rivus.venue_checks AS
SELECT * FROM rivus.venue_checks_queue;
