{{
    config (
        materialized = 'incremental',
        unique_key = "country_id",
        
    )
}}

select * from  {{source('landing','stg_countries_details')}}

{% if is_incremental() %}

    where date::date >= dateadd(day, -3 , current_date())

{% endif %}