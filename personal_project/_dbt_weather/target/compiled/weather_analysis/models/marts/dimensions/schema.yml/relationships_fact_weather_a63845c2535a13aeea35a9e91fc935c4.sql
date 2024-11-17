
    
    

with child as (
    select city_id as from_field
    from DATA_ANALYTICS.transformations.fact_weather
    where city_id is not null
),

parent as (
    select city_id as to_field
    from DATA_ANALYTICS.transformations.dim_cities_countries
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


