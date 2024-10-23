{{ config(
    materialized = 'incremental',
    unique_key = "Patient_id"
)}}

select 
    "PatientID" as Patient_id,
    "firstname" as First_name,
    "lastname" as Last_name,
    "email" as Email,
    "created_at"::timestamp as created_at,
    '{{ run_started_at }}' as run_started_at,
    '{{ invocation_id }}' as invocation_id
    
from {{ ref('patient') }}

{% if is_incremental() %}
    where "created_at"::timestamp >= (
        select coalesce(max("created_at"::timestamp), '1970-01-01'::timestamp)
        from {{ this }}
    )
{% endif %}
