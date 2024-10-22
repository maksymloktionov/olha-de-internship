select *
from {{ref('stg_patient')}}

where patient_id not in (
    select patient_id
    from {{ref('stg_patient')}}
    group by patient_id
    having count(*) > 1
)
