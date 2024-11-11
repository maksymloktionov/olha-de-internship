with time_per_period as (
    select 
        date,
        extract(year from date) as year,
        extract (quarter from date) as quarter,
        extract(month from date) as month
    from {{ref('int_cities_join')}}
)
select 
    row_number() over (order by date) as time_id,
    date,
    year,
    quarter,
    month
from time_per_period
