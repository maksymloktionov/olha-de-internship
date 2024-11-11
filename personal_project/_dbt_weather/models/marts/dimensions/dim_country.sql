
select 
    row_number() over(order by country) as country_id,
    country,
    city_name,
    region,
    continent

from {{ref('int_cities_join')}}
group by 
    country,
    city_name,
    region,
    continent
