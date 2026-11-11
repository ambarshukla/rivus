-- Coinbase: one trade per message.
select
    'coinbase'                                  as venue,
    msg ->> 'product_id'                        as pair,
    cast(msg ->> 'trade_id' as bigint)          as trade_id,
    cast(msg ->> 'price' as decimal(18, 8))     as price,
    cast(msg ->> 'size' as decimal(18, 8))      as qty,
    -- Coinbase reports the resting (maker) order's side; the taker is the opposite.
    case msg ->> 'side' when 'buy' then 'sell' else 'buy' end as side,
    {{ iso_utc("msg ->> 'time'") }}             as event_time,
    recv_ms
from {{ raw_messages('coinbase') }}
where msg ->> 'type' in ('match', 'last_match')
