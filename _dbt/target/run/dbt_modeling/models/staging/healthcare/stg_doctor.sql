
  create view "my_db"."public"."stg_doctor__dbt_tmp"
    
    
  as (
    select  "DoctorID" as Doctor_id,
        "DoctorName" as Doctor_name,
        "Specialization",
        "DoctorContact" as Doctor_contact
    from "my_db"."public"."Doctor"
  );