-- Results of the hourly batch reconciliation. Written by the Airflow job, not
-- by the stream. Running an hour again replaces its earlier rows.

-- One row per hour, venue and pair: how many 1-minute bars each side had and
-- how many agreed.
CREATE TABLE IF NOT EXISTS rivus.recon_runs (
  hour_start  DateTime('UTC'),
  venue       LowCardinality(String),
  pair        LowCardinality(String),
  stream_bars UInt64,
  batch_bars  UInt64,
  matched     UInt64,
  differences UInt64,
  checked_at  DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(hour_start)
ORDER BY (hour_start, venue, pair);

-- One row per field that differed in one bar, with the reason found for it.
CREATE TABLE IF NOT EXISTS rivus.recon_diffs (
  hour_start     DateTime('UTC'),
  venue          LowCardinality(String),
  pair           LowCardinality(String),
  window_start   DateTime64(3, 'UTC'),
  field          LowCardinality(String),
  stream_value   String,
  batch_value    String,
  classification LowCardinality(String),
  checked_at     DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(hour_start)
ORDER BY (hour_start, venue, pair, window_start, field);
