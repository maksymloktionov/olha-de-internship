
  create view "my_db"."public"."dim_patient__dbt_tmp"
    
    
  as (
    with patient as (
    select * from "my_db"."public"."stg_patient"
),


duplicate_patients as (
    select patient_id,email,
        count(*) as patient_count 
    from patient
    group by 
        patient_id,
        email 
    having count(*) > 1  
)

select *
from duplicate_patients
order by patient_id
  );