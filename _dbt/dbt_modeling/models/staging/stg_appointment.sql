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
            "Time"::timestamp as Appointment_date
from {{ ref('appointment') }}
{% if is_incremental() %}
    where "Time" >= (
        select coalesce(max(Appointment_date), '1970-01-01'::timestamp) from {{ this }}
    )
{% endif %}
