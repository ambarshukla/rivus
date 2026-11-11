-- In any bar: low <= open, close <= high; something was traded; prices are positive.
select *
from {{ ref('batch_bars_1m') }}
where not (
    low <= open and open <= high
    and low <= close and close <= high
    and low <= vwap and vwap <= high
    and low > 0 and volume > 0 and trades > 0
)
