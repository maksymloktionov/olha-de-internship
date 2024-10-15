with medical_procedure as (
    select 
        "ProcedureID" as Procedure_id,
        "ProcedureName" as Procedure_name,
        "AppointmentID" as Appointment_id
        from "my_db"."public"."Medical_Procedure"
)
select distinct
    Procedure_id,
    Procedure_name,
    count(*) over (partition by Procedure_name) as procedure_count,
    Appointment_id
from medical_procedure
order by Procedure_name