
    
    

with child as (
    select invoice_id as from_field
    from "my_db"."public"."fct_appointment"
    where invoice_id is not null
),

parent as (
    select invoice_id as to_field
    from "my_db"."public"."dim_billing"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


