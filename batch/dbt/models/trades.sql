-- Every trade that happened in the hour, once. Duplicates (Kraken replays its
-- recent trades on every connect) are removed by keeping the first copy received.
with all_trades as (
    select * from {{ ref('stg_coinbase_trades') }}
    union all
    select * from {{ ref('stg_kraken_trades') }}
    union all
    select * from {{ ref('stg_bitstamp_trades') }}
)

select venue, pair, trade_id, price, qty, side, event_time, recv_ms
from all_trades
where trade_id is not null
  and event_time >= {{ hour_start() }}
  and event_time <  {{ hour_start() }} + interval 1 hour
qualify row_number() over (partition by venue, pair, trade_id order by recv_ms) = 1
