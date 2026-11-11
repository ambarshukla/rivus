-- A trade must appear once. Any row returned here is a failure.
select venue, pair, trade_id, count(*) as copies
from {{ ref('trades') }}
group by venue, pair, trade_id
having count(*) > 1
