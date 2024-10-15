select  "AppointmentID" as Appointment_id,
        "PatientID" as Patient_id,
        "DoctorID" as Doctor_id,
        to_timestamp("Date" || ' ' || "Time", 'YYYY-MM-DD HH24:MI:SS') as Appointment_date 
    from "my_db"."public"."Appointment"