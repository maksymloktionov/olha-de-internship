
  create view "my_db"."public"."dim_patient__dbt_tmp"
    
    
  as (
    with patient as (
    select * from "my_db"."public"."stg_patient"
)
select * from patient
order by patient_id
  );