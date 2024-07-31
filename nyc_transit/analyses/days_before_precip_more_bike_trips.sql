with prcp_any_by_day as
(
    select
        date,
        (prcp + snow) > 0 as prcp_any
--creating a boolean field per day to see if there was rain or snow
    from {{ ref('stg__central_park_weather') }}
)
,
final as (
    select
        p.date,
        prcp_any,
        lead(prcp_any) over (order by p.date) as prcp_any_next,
--did it rain or snow the day after?
        ttd.trips as trips_today,
        ttd.trips - ttm.trips as trips_delta
    from prcp_any_by_day p
    join {{ ref('mart__fact_all_trips_daily') }} ttd on p.date = ttd.date and ttd.type = 'bike'
--join on the date to get the trips for the current day, setting a filter in the join as only want that part of the table
    join {{ ref('mart__fact_all_trips_daily') }} ttm on (p.date + 1) = ttm.date and ttm.type = 'bike'
--join on the day after to find the bike trips for the next day    
)

select avg(trips_delta/trips_today) reduction_in_trips
from final
where prcp_any_next and not prcp_any;