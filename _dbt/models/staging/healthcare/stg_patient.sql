select  "PatientID" as Patient_id,
        "firstname" as First_name,
        "lastname" as Last_name,
        "email" as Email
    from {{ref('Patient')}}        