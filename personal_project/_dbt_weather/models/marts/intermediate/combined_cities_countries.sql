
select 
    c.city_id,
    c.city,
    c.date,
    c.avg_temperature,
    c.min_temperature,
    c.max_temperature,
    c.total_precipitation,
    c.snow_depth,
    c.wind_direction,
    c.avg_wind_speed,
    c.wind_peak_gust,
    c.sea_level_air_pressure,
    c.total_sunshine_duration,
    cd.country_id,
    cd.country,
    cd.region,
    cd.continent,
    cd.latitude,
    cd.longitude,
    cd.agricultural_land,
    cd.forest_area,
    cd.land_area,
    cd.rural_land,
    cd.urban_land,
    cd.co2_emissions,
    cd.methane_emissions
from {{ ref('stg_cities') }} c
left join {{ ref('stg_countries_details') }} cd
    on c.city = cd.city
