{{config(
    materialized='table'
)}}

SELECT 
    city,
    date(weather_time_local) as date,
    ROUND(AVG(temperature)::numeric, 2) as avg_temperature,
    ROUND(AVG(wind_speed)::numeric, 2) as avg_wind_speed
from {{ ref('stg_weather_data') }}
GROUP BY 
    city, 
    date(weather_time_local)
ORDER BY 
    city, 
    date(weather_time_local)