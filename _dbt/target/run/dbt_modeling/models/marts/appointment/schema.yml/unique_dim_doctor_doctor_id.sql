select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    doctor_id as unique_field,
    count(*) as n_records

from "my_db"."public"."dim_doctor"
where doctor_id is not null
group by doctor_id
having count(*) > 1



      
    ) dbt_internal_test