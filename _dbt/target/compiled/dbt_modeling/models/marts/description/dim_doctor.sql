with doctor as (
    select  "DoctorID" as Doctor_id,
            "DoctorName" as Doctor_name,
            "Specialization",
            "DoctorContact" as Doctor_contact
    from "my_db"."public"."Doctor"       
   
)

select distinct Doctor_id from doctor 
order by doctor_id