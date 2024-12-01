with daily_weather as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['c.city_id', 'd.date_key']) }} as fact_weather_key,
        c.city_id,
        cd.country_id,
        d.date_key,
        c.date, 
        c.avg_temperature,
        c.min_temperature,
        c.max_temperature,
        c.total_precipitation,
        c.snow_depth,
        c.wind_direction,
        c.avg_wind_speed,
        c.wind_peak_gust,
        c.sea_level_air_pressure,
        c.total_sunshine_duration,
        
        avg(c.avg_temperature) over (partition by  year(c.date)) as yearly_avg_temp,
        min(c.min_temperature) over (partition by  year(c.date)) as yearly_min_temp,
        max(c.max_temperature) over (partition by  year(c.date)) as yearly_max_temp,
        sum(c.total_precipitation) over (partition by year(c.date)) as yearly_precipitation,
        sum(c.snow_depth) over (partition by year(c.date)) as yearly_snow_depth,
        avg(c.avg_wind_speed) over (partition by year(c.date)) as yearly_avg_wind_speed,
        case 
            when c.wind_peak_gust >= 80 then 'Extreme'
            when c.wind_peak_gust >= 50 and c.wind_peak_gust < 80 then 'Strong'
            else 'Moderate'     
            end as wind_gust_category,
        case 
            when c.sea_level_air_pressure < 1000 then 'Low'
            when c.sea_level_air_pressure between 1000 and 1020 then 'Normal'
            else 'High'
            end as sea_air_pressure,

        sum(c.total_sunshine_duration) over (partition by month(c.date)) as monthly_sunshine_hours,
        case 
            when c.total_sunshine_duration < 3 then 'Low'
            when c.total_sunshine_duration between 3 and 8 then 'Moderate'
            else 'High'
        end as sunshine_duration

    from {{ ref('combined_cities_countries') }} c
    left join {{ ref('dim_date') }} d on c.date = d.date_day
    left join {{ ref('dim_cities_countries') }} cd on c.city_id = cd.city_id

)

select * from daily_weather
