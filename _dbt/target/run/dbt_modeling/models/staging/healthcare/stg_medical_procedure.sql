
  create view "my_db"."public"."stg_medical_procedure__dbt_tmp"
    
    
  as (
    select  "ProcedureID" as Procedure_id,
        "ProcedureName" as Procedure_name,
        "AppointmentID" as Appointment_id
    from "my_db"."public"."Medical_Procedure"
  );