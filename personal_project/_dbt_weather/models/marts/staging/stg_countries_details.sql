{{ config(
    materialized = 'incremental',
    unique_key = "country_id"
) }}

SELECT 
    country_id,
    country,
    city,
    region,
    continent,
    latitude,
    longitude,
    agricultural_land,
    forest_area,
    land_area,
    rural_land,
    urban_land,
    co2_emissions,
    methane_emissions,
    loaded_at 
FROM {{ source('landing', 'stg_countries_details') }}

{% if is_incremental() %}
    
where loaded_at >= (select coalesce(max(loaded_at),'1900-01-01') from {{ this}})

{% endif %}
