{{
    config(
        materialized = 'incremental'
    )
}}

WITH source_data AS (
    SELECT         
        CASE
            WHEN capital_city = 'Kyiv' THEN 1
            WHEN capital_city = 'Munich' THEN 2
            WHEN capital_city = 'Oslo' THEN 3
            WHEN capital_city = 'Paris' THEN 4
            WHEN capital_city = 'Zagreb' THEN 5
            ELSE 0  
        END AS city_id,
        capital_city as city,
        country,
        region,
        continent,
        latitude,
        longitude,
        agricultural_land,
        co2_emissions,
        CURRENT_TIMESTAMP AS loaded_at
    FROM {{ source('landing', 'stg_countries_details') }}
)

SELECT * FROM source_data

UNION ALL

SELECT 
    2 AS city_id,
    'Germany' AS country,
    'Munich' AS city, 
    'Western Europe' AS region,
    'Europe' AS continent,
    48.1351 AS latitude,
    11.5820 AS longitude,
    NULL AS agricultural_land,
    603350 AS co2_emissions,
    CURRENT_TIMESTAMP AS loaded_at
WHERE NOT EXISTS (
    SELECT 1 FROM source_data WHERE city = 'Munich'
)

{% if is_incremental() %}
AND loaded_at >= (SELECT COALESCE(MAX(loaded_at), '1970-01-01'::timestamp) FROM {{ this }})
{% endif %}
