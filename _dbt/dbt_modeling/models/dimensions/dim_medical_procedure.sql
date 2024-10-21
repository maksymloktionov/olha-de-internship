with medical_procedure as (
    select 
         Procedure_id,
         Procedure_name,
         Appointment_id
       
        from {{ref ('stg_medical_procedure')}}
),

pre_final as ( 
    select 
        Procedure_id,
        Procedure_name,
        Appointment_id,
        row_number() over (partition by Procedure_id order by Procedure_name ) as procedure_row_number
    from medical_procedure

)

select Procedure_id, Procedure_name, Appointment_id
from pre_final
where procedure_row_number = 1