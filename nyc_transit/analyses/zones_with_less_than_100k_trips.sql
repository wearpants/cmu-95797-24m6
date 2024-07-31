select Zone, count(t.*) trips  
from {{ ref('mart__fact_all_taxi_trips') }} t 
join {{ ref('mart__dim_locations') }} l on t.pulocationid = l.locationid 
group by all 
having trips < 100000
