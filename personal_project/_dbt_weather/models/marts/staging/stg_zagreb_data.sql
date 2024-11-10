{{
    config (
        materialized = 'incremental',
        unique_key = "date"
    )
}}

SELECT 
    5 as city_id,
    city_name,
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

FROM {{source('landing','stg_zagreb_data')}}


{% if is_incremental() %}
    where date::date >= (select coalesce (max(date::date),'1970-01-01'::date) from {{this}})

{% endif %}