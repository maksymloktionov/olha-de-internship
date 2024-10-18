{{
    config (
        materialized = 'incremental'
    )
}}

with latest_increment as (
    select coalesce(max("created_at"), '1970-01-01') as last_incremental_time
    from {{ this }}
)

select 
    "InvoiceID" as Invoice_id,
    "PatientID" as Patient_id,
    "Items" as Procedure,
    "Amount",
    "created_at"  
from {{ ref('billing') }}  

{% if is_incremental() %}
    where "created_at" > (select last_incremental_time from latest_increment)
{% endif %}

