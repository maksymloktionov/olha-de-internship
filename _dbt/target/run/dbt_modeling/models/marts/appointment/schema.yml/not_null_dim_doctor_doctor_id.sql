select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select doctor_id
from "my_db"."public"."dim_doctor"
where doctor_id is null



      
    ) dbt_internal_test