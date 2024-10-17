select  "DoctorID" as Doctor_id,
        "DoctorName" as Doctor_name,
        "Specialization",
        "DoctorContact" as Doctor_contact
    from {{ref('doctor')}}        
