--write a query to calculate the 7 day moving average precipitation for every day in the weather data
--https://duckdb.org/docs/sql/window_functions#box-and-whisker-queries
select 
    date,
    avg(prcp) over( order by date     
                    range   between interval 3 days preceding               
                                and interval 3 days following)     as prcp_7_day_moving_average 
from {{ ref('stg__central_park_weather') }} 
order by date;