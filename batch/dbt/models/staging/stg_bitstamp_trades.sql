-- Bitstamp: one trade per message; the pair is only in the channel name
-- ('live_trades_btcusd') and the time is microseconds since 1970.
select
    'bitstamp'                                                as venue,
    upper(substr(msg ->> 'channel', 13, 3)) || '-' || upper(substr(msg ->> 'channel', 16)) as pair,
    cast(msg -> 'data' ->> 'id' as bigint)                    as trade_id,
    cast(msg -> 'data' ->> 'price_str' as decimal(18, 8))     as price,
    cast(msg -> 'data' ->> 'amount_str' as decimal(18, 8))    as qty,
    case cast(msg -> 'data' ->> 'type' as integer) when 0 then 'buy' else 'sell' end as side,
    make_timestamp(cast(msg -> 'data' ->> 'microtimestamp' as bigint)) as event_time,
    recv_ms
from {{ raw_messages('bitstamp') }}
where msg ->> 'event' = 'trade'
