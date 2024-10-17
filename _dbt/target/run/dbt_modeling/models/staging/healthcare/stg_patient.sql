
  create view "my_db"."public"."stg_patient__dbt_tmp"
    
    
  as (
    select  "PatientID" as Patient_id,
        "firstname" as First_name,
        "lastname" as Last_name,
        "email" as Email
    from "my_db"."public"."Patient"
  );