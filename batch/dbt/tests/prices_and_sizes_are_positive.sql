-- A trade with a zero or negative price or size means a message was misread.
select *
from {{ ref('trades') }}
where price <= 0 or qty <= 0
