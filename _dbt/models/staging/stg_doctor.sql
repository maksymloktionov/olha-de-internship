{{
    config(
        materialized = 'incremental',
        unique_key = "Doctor_id"
    )
}}

select  "DoctorID" as Doctor_id,
        "DoctorName" as Doctor_name,
        "Specialization",
        "DoctorContact" as Doctor_contact,
        "created_at" ::timestamp as created_at
    from {{ref('doctor')}}        

{% if is_incremental() %}
    where "created_at" ::timestamp >= (select coalesce (max("created_at" ::timestamp ),'1970-01-01'::timestamp) from {{this}})

{%endif%}
