with base_data as( 
    select
        city_id,
        city,
        country_id,
        country,
        continent,
        latitude,
        longitude,
        nullif(agricultural_land, 0) as agricultural_land,
        nullif(forest_area,0) as forest_area,
        nullif(land_area,0) as land_area,
        nullif(rural_land,0) as rural_land,
        nullif(urban_land,0) as urban_land,
        co2_emissions,
        methane_emissions
        from {{ref('combined_cities_countries')}}
    ),

cleaned_data as(
    select distinct
        city_id,
        city,
        country_id,
        country,
        continent,
        latitude,
        longitude,
        coalesce(agricultural_land,0) as agricultural_land,
        coalesce(forest_area,0) as forest_area,
        coalesce(land_area,0) as land_area,
        coalesce(rural_land,0) as rural_land,
        coalesce(urban_land,0) as urban_land,
        co2_emissions,
        methane_emissions
        from base_data
        where
            city_id is not null
            and city!= ''
            and country_id is not null
            and country!= ''
)

select * from cleaned_data