{{
    config (
        materialized = 'incremental',
        unique_key = "Invoice_id"
    )
}}



select 
    "InvoiceID" as Invoice_id,
    "PatientID" as Patient_id,
    "Items" as Procedure,
    "Amount",
    "created_at" 
from {{ ref('billing') }}  

{% if is_incremental() %}
    where "created_at" >= (select coalesce (max(created_at),'1970-01-01'::timestamp) from {{this}})

{% endif %}