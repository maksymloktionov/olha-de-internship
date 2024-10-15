select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select procedure_id
from "my_db"."public"."fct_appointment"
where procedure_id is null



      
    ) dbt_internal_test