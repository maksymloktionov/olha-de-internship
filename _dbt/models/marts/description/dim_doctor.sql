with doctor as (
    select * from {{ref ('stg_doctor')}}
)

select * from doctor 
order by doctor_id