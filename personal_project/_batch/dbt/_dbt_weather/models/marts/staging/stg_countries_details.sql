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

{% if is_incremental() -%}
    WHERE loaded_at  >= (SELECT max(loaded_at) - INTERVAL '7 days' FROM {{ this }})
{% endif %}
