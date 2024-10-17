with appointment_data as(
    select  Appointment_id,
            Patient_id,
            Doctor_id,
            Appointment_date
    from "my_db"."public"."stg_appointment"              
),
patient_data as (
    select 
        Patient_id,
        First_name,
        Last_name
    from "my_db"."public"."dim_patient"
),
doctor_data as (
    select Doctor_id
    from "my_db"."public"."dim_doctor"
),

procedure_data as (
    select  Procedure_id,
            Appointment_id
    from "my_db"."public"."dim_medical_procedure"
),
billing_data as (
    select  Invoice_id,
            Patient_id 
    from "my_db"."public"."dim_billing"
)

select
    a.Appointment_id,
    a.Patient_id,
    a.Doctor_id,
    pr.Procedure_id,
    b.Invoice_id,
    a.Appointment_date
from appointment_data a

join patient_data p on a.Patient_id = p.Patient_id
join doctor_data d on a.Doctor_id = d.Doctor_id
join procedure_data pr on a.Appointment_id = pr.Appointment_id
join billing_data b on a.Patient_id = b.Patient_id

order by a.Appointment_id