{{
config(
    materialized='table',
    unique_key='id',
)}}

with source as (
    SELECT *
    FROM {{ source('dev', 'weather_data')}}
),
de_dup as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY time ORDER BY inserted_at DESC
        ) as rn
    FROM source
)


SELECT
    id,
    city,
    temperature,
    weather_description,
    wind_speed,
    time as weather_time_local,
    (inserted_at + (utc_offset || 'hours')::interval) as inserted_at_local
FROM de_dup
WHERE rn = 1