
    
    

with child as (
    select date_key as from_field
    from DATA_ANALYTICS.transformations.fact_weather
    where date_key is not null
),

parent as (
    select date_key as to_field
    from DATA_ANALYTICS.transformations.dim_date
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


