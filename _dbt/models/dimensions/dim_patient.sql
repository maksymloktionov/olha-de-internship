with patient as (
    select  *
    from {{ref('stg_patient')}}
),


duplicate_patients as (
    select 
        patient_id,
        count(*) as patient_count 
    from patient
    group by patient_id
    having count(*) > 1
)

select *
from patient
where patient_id not in (select patient_id from duplicate_patients)
