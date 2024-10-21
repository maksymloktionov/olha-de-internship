{{ config(
    materialized = 'incremental',
    unique_key = "Patient_id"
)}}
select  "PatientID" as Patient_id,
        "firstname" as First_name,
        "lastname" as Last_name,
        "email" as Email,
        "created_at"
    from {{ref('patient')}}        

{% if is_incremental() %}
    where "created_at" >= (select coalesce(max(created_at),'1970-01-01'::timestamp)from {{this}})

{% endif %}        