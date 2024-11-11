select
    city_id,
    city_name,
    country,
    continent,
    latitude,
    longitude,
    agricultural_land
from (
    select
        city_id,
        city_name,
        country,
        continent,
        latitude,
        longitude,
        agricultural_land,
        row_number() over (partition by city_name order by city_id desc) as row_number
    from {{ref('int_cities_join')}}
    where city_id is not null
    and city_name is not null
    and country is not null
) as filtered_cities
where row_number = 1

