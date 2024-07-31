select 
    borough, 
    zone, 
    count(*) as trips, 
    avg(duration_min) avg_duration_mins
from {{ ref('mart__fact_all_taxi_trips') }} t
join {{ ref('mart__dim_locations') }} dl on t.DOlocationID = dl.LocationID
group by all;
--best as it is not brittle to future changes and is not verbose either
--not supported by every db

select 
    borough, 
    zone, 
    count(*) as trips, 
    avg(duration_min) avg_duration_mins
from {{ ref('mart__fact_all_taxi_trips') }} t
join {{ ref('mart__dim_locations') }} dl on t.DOlocationID = dl.LocationID
group by 1,2; 
--brittle, if you were to add a field between borough and zone in your select, this would break
--it's not clear what the 1 and 2 are despite being easier

select 
    borough, 
    zone, 
    count(*) as trips, 
    avg(duration_min) avg_duration_mins
from {{ ref('mart__fact_all_taxi_trips') }} t
join {{ ref('mart__dim_locations') }} dl on t.DOlocationID = dl.LocationID
group by borough, zone;
--verbose, even though classically normal to rewrite fields in group by
--have to change with every change to select clause