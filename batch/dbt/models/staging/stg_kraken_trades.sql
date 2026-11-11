-- Kraken: several trades per message, in the "data" array. unnest() turns the
-- array into one row per trade.
with messages as (
    select
        recv_ms,
        unnest(from_json(msg -> 'data', '[{
            "symbol": "VARCHAR", "side": "VARCHAR", "price": "DECIMAL(18,8)",
            "qty": "DECIMAL(18,8)", "trade_id": "BIGINT", "timestamp": "VARCHAR"
        }]')) as trade
    from {{ raw_messages('kraken') }}
    where msg ->> 'channel' = 'trade'
)

select
    'kraken'                              as venue,
    replace(trade.symbol, '/', '-')       as pair,
    trade.trade_id                        as trade_id,
    trade.price                           as price,
    trade.qty                             as qty,
    trade.side                            as side,
    {{ iso_utc('trade."timestamp"') }}    as event_time,
    recv_ms
from messages
