
  create view "my_db"."public"."dim_doctor__dbt_tmp"
    
    
  as (
    with doctor as (
    select * from "my_db"."public"."stg_doctor"
)

select * from doctor 
order by doctor_id
  );