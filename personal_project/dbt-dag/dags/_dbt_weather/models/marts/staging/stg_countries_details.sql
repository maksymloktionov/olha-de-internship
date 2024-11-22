{{
    config (
        materialized = 'incremental',
        unique_key = "country_id"
    )
}}

select * from  {{source('landing','stg_countries_details')}}

{% if is_incremental() %}

    where date::date >= DATEADD(DAY, -3, CURRENT_DATE())
{% endif %}