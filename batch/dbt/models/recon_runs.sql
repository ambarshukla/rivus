-- The hour's verdict per venue and pair: how many bars each side has, how
-- many agree, how many do not. A bar whose only differences are rounding
-- counts as agreeing.
with bars as (
    select coalesce(s.venue, b.venue) as venue, coalesce(s.pair, b.pair) as pair,
           coalesce(s.window_start, b.window_start) as window_start,
           s.venue is not null as in_stream, b.venue is not null as in_batch
    from {{ ref('stream_bars_1m') }} s
    full outer join {{ ref('batch_bars_1m') }} b using (venue, pair, window_start)
),

real_differences as (
    select distinct venue, pair, window_start
    from {{ ref('recon_diffs') }}
    where classification <> 'rounding'
)

select
    {{ hour_start() }}                                             as hour_start,
    bars.venue,
    bars.pair,
    count(*) filter (where in_stream)                              as stream_bars,
    count(*) filter (where in_batch)                               as batch_bars,
    count(*) filter (where in_stream and in_batch and d.venue is null) as matched,
    count(*) filter (where d.venue is not null)                    as differences
from bars
left join real_differences d using (venue, pair, window_start)
group by bars.venue, bars.pair
