-- Every field of every 1-minute bar where stream and batch disagree, with the
-- most likely reason. One row per (bar, field).
--
-- Reasons, first match wins:
--   rounding     the two numbers differ by less than a millionth of their size
--   late event   the stream flagged a trade in this minute as too late for its bar
--   gap          the stream detected missing trade ids in this minute
--   ordering     only open/close differ: same trades, taken in a different order
--   unexplained  none of the above
with late as (
    select distinct venue, pair, date_trunc('minute', event_time) as window_start
    from read_csv('{{ var("stream_dir") }}/late.csv', header = true,
                  columns = {venue: 'VARCHAR', pair: 'VARCHAR', trade_id: 'BIGINT', event_time: 'TIMESTAMP'})
),

gaps as (
    select distinct venue, pair, date_trunc('minute', event_time) as window_start
    from read_csv('{{ var("stream_dir") }}/gaps.csv', header = true,
                  columns = {venue: 'VARCHAR', pair: 'VARCHAR', prev_trade_id: 'BIGINT',
                             trade_id: 'BIGINT', missing: 'BIGINT', event_time: 'TIMESTAMP'})
),

bars as (
    select
        coalesce(s.venue, b.venue)               as venue,
        coalesce(s.pair, b.pair)                 as pair,
        coalesce(s.window_start, b.window_start) as window_start,
        s.venue is not null                      as in_stream,
        b.venue is not null                      as in_batch,
        s.trades = b.trades and s.volume = b.volume as same_trades,
        s.open as s_open, s.high as s_high, s.low as s_low, s.close as s_close,
        s.volume as s_volume, s.vwap as s_vwap, cast(s.trades as decimal(38, 8)) as s_trades,
        b.open as b_open, b.high as b_high, b.low as b_low, b.close as b_close,
        b.volume as b_volume, b.vwap as b_vwap, cast(b.trades as decimal(38, 8)) as b_trades
    from {{ ref('stream_bars_1m') }} s
    full outer join {{ ref('batch_bars_1m') }} b using (venue, pair, window_start)
),

-- One row per bar and field, so every field is compared the same way.
fields as (
    select venue, pair, window_start, in_stream, in_batch, same_trades, field, stream_value, batch_value
    from bars, lateral (
        values ('trades', s_trades, b_trades), ('volume', s_volume, b_volume),
               ('open', s_open, b_open), ('high', s_high, b_high), ('low', s_low, b_low),
               ('close', s_close, b_close), ('vwap', s_vwap, b_vwap)
    ) as f(field, stream_value, batch_value)
),

differing as (
    -- A bar present on one side only is reported once, not once per field.
    select venue, pair, window_start, 'bar' as field, same_trades,
           case when in_stream then 'present' else 'missing' end as stream_value,
           case when in_batch then 'present' else 'missing' end as batch_value,
           cast(null as double) as relative_difference
    from bars
    where not (in_stream and in_batch)

    union all
    select venue, pair, window_start, field, same_trades,
           cast(stream_value as varchar), cast(batch_value as varchar),
           abs(stream_value - batch_value) / greatest(abs(batch_value), 1e-12)
    from fields
    where in_stream and in_batch and stream_value <> batch_value
)

select
    {{ hour_start() }} as hour_start,
    d.venue, d.pair, d.window_start, d.field, d.stream_value, d.batch_value,
    case
        when d.relative_difference < 1e-6 then 'rounding'
        when late.venue is not null then 'late event'
        when gaps.venue is not null then 'gap'
        when d.field in ('open', 'close') and d.same_trades then 'ordering'
        else 'unexplained'
    end as classification
from differing d
left join late using (venue, pair, window_start)
left join gaps using (venue, pair, window_start)
