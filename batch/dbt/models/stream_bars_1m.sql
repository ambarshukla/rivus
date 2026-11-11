-- The bars the stream produced for the hour, as exported from ClickHouse.
select *
from read_csv(
    '{{ var("stream_dir") }}/bars.csv',
    header = true,
    columns = {
        venue: 'VARCHAR', pair: 'VARCHAR', window_start: 'TIMESTAMP',
        open: 'DECIMAL(24,8)', high: 'DECIMAL(24,8)', low: 'DECIMAL(24,8)', close: 'DECIMAL(24,8)',
        volume: 'DECIMAL(38,8)', vwap: 'DECIMAL(24,8)', trades: 'BIGINT'
    }
)
