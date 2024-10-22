
with billing as (
    select  Invoice_id,
            Patient_id,
            Procedure,
            "Amount"
    from {{ref ('stg_billing')}}   
)

select distinct
    Invoice_id,
    Patient_id,
    Procedure,
    sum("Amount") over (partition by Procedure) as total_amount
from billing

--order by total_amount desc ,Patient_id