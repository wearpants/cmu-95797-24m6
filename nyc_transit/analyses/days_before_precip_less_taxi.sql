/*  Write a query to determine if days immediately preceding precipitation or snow,
    had more bike trips on average than the days with precipitation or snow.

    Write a similar query to determine the reverse for taxis too, and see if this had a bigger effect 
    on inter-borough trips than inter-zone. Whilst calculating this, write a dbt macro to use that classifies a trip as inter borough, 
    inter zone or local - where inter borough is a trip between boroughs, 
    inter zone is a trip between zones in the same borough and local is in the same zone. Use this dbt macro in your solution.

    With a query as complicated as the second one it can be helpful to plan your query before you start writing it out.
    It can be helpful to break out the constituent parts and then figuring out how to join them together afterwards.

    We need:
    1. The same CTE of weather with boolean of precip by day, then the lead() to find whether the next day had any
    2. However this time it can't be the spine of the query as the taxi data is at a finer grain than just day, we want to know if a taxi trip was
        inter-borough, inter-zone or local too.
    3. Therefore we also need a table of daily taxi trips which are classified in this way
    4. We need to write a macro which checks if borough is different, then checks if zone is different and otherwise assumes local. Let's call it trip_type.
    5. We need to use this macro the create the part 3
    6. Once we have these parts we can join the precip by day CTE to our taxi trip CTE, and then perform a similar calculation to the first query,
        except grouped by trip_type

    Part 1 we can copy from the previous exercise.
    Part 5 needs to be made for use in part 3.
    Part 3 we need to make new from mart__fact_all_taxi_trips joined to mart__dim_locations.
    Then we can put them together afterwards.

*/

--Part 1

with prcp_any_by_day as
(
    select
        date,
        (prcp + snow) > 0 as prcp_any
--creating a boolean field per day to see if there was rain or snow
    from {{ ref('stg__central_park_weather') }}
)
,
--Part 3
daily_taxi_trips_wtriptype as (
    select
        date_trunc('day', pickup_datetime)::date as date,
        {{trip_type('pul', 'dol')}} as trip_type, --Part 5 use
        count(t.*) as trips
    from {{ ref('mart__fact_all_taxi_trips') }} t
    join {{ ref('mart__dim_locations') }} pul on t.pulocationid = pul.locationid
    join {{ ref('mart__dim_locations') }} dol on t.dolocationID = dol.locationid
    --joins above are inner joins meaning only records with both valid pu and do location ids will say in the dataset
    group by all
)
,
final as (
    select 
        t.date,
        t.trip_type,
        t.trips as trips_today,
        lead(t.trips) over (partition by t.trip_type order by t.date) - t.trips as trips_delta
    from daily_taxi_trips_wtriptype t
    join prcp_any_by_day ptd on t.date          = ptd.date
    join prcp_any_by_day ptm on (t.date + 1)    = ptm.date
    where ptm.prcp_any and not ptd.prcp_any
)

select
    trip_type,
    avg(trips_delta/trips_today) increase_in_trips
from final
group by all;