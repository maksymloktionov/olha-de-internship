select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    fact_weather_key as unique_field,
    count(*) as n_records

from DATA_ANALYTICS.transformations.fact_weather
where fact_weather_key is not null
group by fact_weather_key
having count(*) > 1



      
    ) dbt_internal_test