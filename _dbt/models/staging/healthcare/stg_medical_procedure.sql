select  "ProcedureID" as Procedure_id,
        "ProcedureName" as Procedure_name,
        "AppointmentID" as Appointment_id
    from {{ref('Medical_Procedure')}}        
