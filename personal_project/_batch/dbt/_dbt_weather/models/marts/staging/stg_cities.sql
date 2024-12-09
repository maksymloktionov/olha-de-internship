{{
    config (
        materialized = 'incremental',
        unique_key = ['city_id',' date']
    )
}}


SELECT distinct
    city_id,
    city,
    date,
    tavg as avg_temperature,
    tmin as min_temperature,
    tmax as max_temperature,
    prcp as total_precipitation,
    snow as snow_depth,
    wdir as wind_direction,
    wspd as avg_wind_speed,
    wpgt as wind_peak_gust,
    pres as sea_level_air_pressure,
    tsun as total_sunshine_duration

FROM {{source('landing','stg_cities')}}

{% if is_incremental() -%}
    WHERE date  >= (SELECT max(date) - INTERVAL '7 days' FROM {{ this }})
{% endif %}