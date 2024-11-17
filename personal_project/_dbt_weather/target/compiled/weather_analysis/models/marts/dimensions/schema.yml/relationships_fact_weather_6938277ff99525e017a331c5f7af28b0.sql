
    
    

with child as (
    select country_id as from_field
    from DATA_ANALYTICS.transformations.fact_weather
    where country_id is not null
),

parent as (
    select country_id as to_field
    from DATA_ANALYTICS.transformations.dim_cities_countries
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


