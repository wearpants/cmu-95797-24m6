select 
    fare_amount,
    zone,
    avg(fare_amount) over () as avg_fare -- get overall average fare
    avg(fare_amount) over (partition by zone) zone_avg_fare, 
--this window function finds the average fare amount for the zone this trip is in
    borough,
    avg(fare_amount) over (partition by borough) borough_avg_fare
--this window function finds the average fare amount for the borough this trip is in
from {{ ref('stg__yellow_tripdata') }} y
join {{ ref('mart__dim_locations') }} l on y.PUlocationID = l.locationid;
