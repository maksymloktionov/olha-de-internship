{{
    config(
        materialized='incremental',
        unique_key = "Appointment_id"
    )
}}

select distinct 
            "AppointmentID" as Appointment_id,
            "PatientID" as Patient_id,
            "DoctorID" as Doctor_id,
            "Time"::timestamp as Appointment_date,
            '{{ run_started_at }}' as run_started_at,
            '{{ invocation_id }}' as invocation_id
from {{ ref('appointment') }}
{% if is_incremental() %}
    where "Time" >= (
        select coalesce(max(Appointment_date), '1970-01-01'::timestamp) from {{ this }}
    )
{% endif %}
