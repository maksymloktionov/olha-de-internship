{{
    config (
        materialized = 'incremental',
        unique_key = "country_id"
    )
}}

select * from  {{source('landing','stg_countries_details')}}