select 
    Invoice_id,
    Patient_id,
    Procedure,
    sum("Amount") as total_amount
from {{ref('stg_billing')}}
group by Invoice_id, Patient_id, Procedure
