    select 
        l.borough, 
        count(t.*) as trips
    from {{ ref('mart__fact_all_taxi_trips') }} t 
    join {{ ref('mart__dim_locations') }} l on t.pulocationid = l.locationid 
    --bringing in the borough field from the mart__dim_locations table by joining on the pickup location id
    group by all 