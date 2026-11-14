-- Every start of a stream job, written by flink/submit.sh.
--
-- A job that starts again was not working for a while before that. A bar whose
-- minute falls in that period can be incomplete or missing, and the hourly
-- reconciliation labels its differences "restart".
--
--   down_since  when the job before this one last saved a checkpoint: the last
--               moment it is known to have worked. Equal to started_at when
--               there was no earlier job.
--   resumed     true if the job continued from that checkpoint, false if it
--               started with empty state (always the case after its SQL changed).
CREATE TABLE IF NOT EXISTS rivus.job_starts (
  job        LowCardinality(String),
  job_id     String,
  started_at DateTime64(3, 'UTC'),
  down_since DateTime64(3, 'UTC'),
  resumed    Bool,
  stored_at  DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (job, started_at)
TTL toDateTime(started_at) + INTERVAL 90 DAY;
