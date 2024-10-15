with patient as (
    select * from {{ref ('stg_patient')}}
)
select * from patient
order by patient_id
