--write a query to calculate the 7 day moving min, max, avg, sum for precipitation and snow for every day
--in the weather data, defining the window only once.

SELECT date,
    min(prcp) over seven as prcp_7_day_moving_min,
    max(prcp) over seven as prcp_7_day_moving_max,
    avg(prcp) over seven as prcp_7_day_moving_avg,
    sum(prcp) over seven as prcp_7_day_moving_sum,
    min(snow) over seven as snow_7_day_moving_min,
    max(snow) over seven as snow_7_day_moving_max,
    avg(snow) over seven as snow_7_day_moving_avg,
    sum(snow) over seven as snow_7_day_moving_sum
FROM {{ ref('stg__central_park_weather') }} 
WINDOW seven AS (
    ORDER BY date ASC
    RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
              AND INTERVAL 3 DAYS FOLLOWING)
ORDER BY date;