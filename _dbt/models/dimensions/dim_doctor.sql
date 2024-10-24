select distinct 
    Doctor_id,
    Doctor_name,
    "Specialization",
    Doctor_contact
from {{ref('stg_doctor')}}
