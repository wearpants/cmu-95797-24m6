with zone_next_pickup_secs as
    (
        select
            zone, 
            datediff('second', pickup_datetime, lead(pickup_datetime) 
                over (partition by zone order by pickup_datetime)) as time_to_next_pickup
--datediff finds the time between two dates/times
--we are using it to find the time difference between the pickup date time and the next pickup date time
--we partition by the zone as we want to find the next pick up time in that zone
        from {{ ref('mart__fact_all_taxi_trips') }} t
        join {{ ref('mart__dim_locations') }} l on t.pulocationid = l.locationid
    )

select 
    zone, 
    avg(time_to_next_pickup) as avg_time_between_pickups
from zone_next_pickup_secs
group by all;