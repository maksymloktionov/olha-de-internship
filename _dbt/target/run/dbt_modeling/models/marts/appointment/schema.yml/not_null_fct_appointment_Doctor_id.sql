select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select Doctor_id
from "my_db"."public"."fct_appointment"
where Doctor_id is null



      
    ) dbt_internal_test