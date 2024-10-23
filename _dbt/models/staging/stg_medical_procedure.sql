    {{
        config(
            materialized ='incremental',
            unique_key = "Procedure_id"
        )
    }}
    select  "ProcedureID" as Procedure_id,
            "ProcedureName" as Procedure_name,
            "AppointmentID" as Appointment_id,
            "created_at" ::timestamp as created_at
        from {{ref('medical_procedure')}}        

    {% if is_incremental() %}
        where "created_at"::timestamp >= (select coalesce(max("created_at"::timestamp),'1970-01-01'::timestamp)from {{this}})

    {% endif %}    