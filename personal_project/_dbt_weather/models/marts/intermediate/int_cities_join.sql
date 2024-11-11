with kyiv_data as (
    select * from {{ ref('stg_kyiv_data') }}
),
munich_data as (
    select * from {{ ref('stg_munich_data') }}
),
oslo_data as (
    select * from {{ ref('stg_oslo_data') }}
),
paris_data as (
    select * from {{ ref('stg_paris_data') }}
),
zagreb_data as (
    select * from {{ ref('stg_zagreb_data') }}
),
all_city_data as (
    select * from kyiv_data
    union all
    select * from munich_data
    union all
    select * from oslo_data
    union all
    select * from paris_data
    union all
    select * from zagreb_data
),
country_details as (
    select distinct 
        city_id,  
        country, 
        city as city_name, 
        continent, 
        region,
        latitude, 
        longitude,
        co2_emissions,
        agricultural_land
    from {{ ref('stg_countries_details') }}
),
city_joins as (
    select
        ac.city_id,
        ac.city_name,
        ac.date,
        extract(year from ac.date) as year,
        avg(ac.avg_temperature) as avg_temp,
        min(ac.min_temperature) as min_temp,
        max(ac.max_temperature) as max_temp,
        sum(ac.total_precipitation) as total_precipitation,
        avg(ac.wind_direction) as avg_wind_direction,
        avg(ac.avg_wind_speed) as avg_wind_speed,
            case    
                when ac.city_id = 2 then 'Germany'
                else cd.country
                end as country,
        cd.continent,
        cd.region,
        cd.latitude,
        cd.longitude,
        cd.co2_emissions,
        cd.agricultural_land
    from all_city_data ac
    join country_details cd on ac.city_id = cd.city_id  
    group by
        ac.city_id, 
        ac.city_name, 
        ac.date,
        cd.country, 
        cd.continent, 
        cd.region,
        cd.latitude, 
        cd.longitude, 
        cd.co2_emissions,
        cd.agricultural_land
)

select * 
from city_joins
