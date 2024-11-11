with dim_city as (
    select city_id 
    from {{ ref('dim_city') }}
),
dim_country as (
    select country_id
    from {{ ref('dim_country') }}
),
dim_time as ( 
    select time_id 
    from {{ ref('dim_time') }}
),
aggregated_data as (
    select 
        city_id,
        country_id,
        time_id,
        avg_temp,
        min_temp,
        max_temp,
        total_precipitation,
        avg_wind_speed,
        avg_wind_direction
    from {{ ref('int_cities_join') }}
)

select distinct
    dc.city_id,
    country.country_id,
    dt.time_id,
    ad.avg_temp,
    ad.min_temp,
    ad.max_temp,
    ad.total_precipitation,
    ad.avg_wind_speed,
    ad.avg_wind_direction
from aggregated_data ad
join dim_city dc on ad.city_id = dc.city_id
join dim_country country on ad.country_id = country.country_id
join dim_time dt on ad.time_id = dt.time_id
