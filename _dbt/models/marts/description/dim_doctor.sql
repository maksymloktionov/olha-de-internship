with doctor as (
    select  "DoctorID" as Doctor_id,
            "DoctorName" as Doctor_name,
            "Specialization",
            "DoctorContact" as Doctor_contact
    from {{ref('doctor')}}       
   
)

select distinct Doctor_id,
                Doctor_name, 
                "Specialization", 
                Doctor_contact

from doctor 
--order by doctor_id
