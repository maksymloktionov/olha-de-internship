
    
    

select
    appointment_id as unique_field,
    count(*) as n_records

from "my_db"."public"."fct_appointment"
where appointment_id is not null
group by appointment_id
having count(*) > 1


