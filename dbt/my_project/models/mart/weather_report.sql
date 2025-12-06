{{
    config(
    materialized='table',
    unique_key='id'
    )
}}

SELECT
    id,  -- ADDED id column
    city,
    temperature,
    weather_description,
    wind_speed,
    weather_time_local
FROM {{ ref('stg_weather_data') }}