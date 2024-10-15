select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select invoice_id
from "my_db"."public"."dim_billing"
where invoice_id is null



      
    ) dbt_internal_test