select 
    Procedure_id,
    Procedure_name,
    Appointment_id
       
from ( 
    select 
        Procedure_id,
        Procedure_name,
        Appointment_id,
        row_number() over (partition by Procedure_id order by Procedure_name ) as procedure_row_number
    from {{ref('stg_medical_procedure')}}

) as ranked_procedure

where procedure_row_number = 1
