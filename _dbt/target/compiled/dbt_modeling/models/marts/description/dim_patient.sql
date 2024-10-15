with patient as (
    select * from "my_db"."public"."stg_patient"
)
select * from patient
order by patient_id