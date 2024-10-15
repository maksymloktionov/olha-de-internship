with doctor as (
    select * from "my_db"."public"."stg_doctor"
)

select * from doctor 
order by doctor_id