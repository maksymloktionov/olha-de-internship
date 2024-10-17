
    
    

select
    Appointment_id as unique_field,
    count(*) as n_records

from "my_db"."public"."fct_appointment"
where Appointment_id is not null
group by Appointment_id
having count(*) > 1


