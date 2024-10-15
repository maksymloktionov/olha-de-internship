
    
    

with child as (
    select procedure_id as from_field
    from "my_db"."public"."fct_appointment"
    where procedure_id is not null
),

parent as (
    select procedure_id as to_field
    from "my_db"."public"."dim_medical_procedure"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


