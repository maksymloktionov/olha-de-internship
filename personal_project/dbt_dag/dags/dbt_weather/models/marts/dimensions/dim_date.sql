with date_data as (
     {{ dbt_date.get_date_dimension("2020-01-01", "2030-12-31") }}

)

select 
    *,
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key
from date_data
