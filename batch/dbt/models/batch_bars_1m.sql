-- The batch's own 1-minute bars, built from the raw trades with no knowledge
-- of what the stream did. With all the hour's data in hand there are no late
-- trades and no watermarks: every trade is in its bar.
select
    venue,
    pair,
    date_trunc('minute', event_time)                          as window_start,
    first(price order by event_time, trade_id)                as open,
    max(price)                                                as high,
    min(price)                                                as low,
    last(price order by event_time, trade_id)                 as close,
    cast(sum(qty) as decimal(38, 8))                          as volume,
    -- Widen before multiplying: an 18-digit decimal times another overflows.
    cast(sum(cast(price as decimal(20, 8)) * qty) / sum(qty) as decimal(24, 8)) as vwap,
    count(*)                                                  as trades
from {{ ref('trades') }}
group by venue, pair, date_trunc('minute', event_time)
