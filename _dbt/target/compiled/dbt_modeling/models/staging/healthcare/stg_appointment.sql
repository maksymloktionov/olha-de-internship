select distinct "AppointmentID" as Appointment_id,
        "PatientID" as Patient_id,
        "DoctorID" as Doctor_id,
        "Time"::timestamp as Appointment_date
from "my_db"."public"."Appointment"