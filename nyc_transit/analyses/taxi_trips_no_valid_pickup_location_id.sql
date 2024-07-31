with invalid as (
select t.*
from {{ ref('mart__fact_all_taxi_trips') }} t
left join {{ ref('mart__dim_locations') }} l
on t.pulocationid = l.LocationID
where l.LocationID is null
),

select count(*) from invalid