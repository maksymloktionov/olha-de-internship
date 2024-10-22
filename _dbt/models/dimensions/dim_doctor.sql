with doctor as (
    select distinct Doctor_id,
                    Doctor_name,
                    "Specialization",
                    Doctor_contact
    from {{ref('stg_doctor')}}       
   
)

select  Doctor_id,
        Doctor_name, 
        "Specialization", 
        Doctor_contact

from doctor 
--order by doctor_id