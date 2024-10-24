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
    "created_at"::timestamp as created_at,
    '{{ run_started_at }}' as run_started_at,
    '{{ invocation_id }}' as invocation_id

from {{ ref('billing') }}  

{% if is_incremental() %}
    where"created_at"::timestamp >= (select coalesce (max("created_at"::timestamp),'1970-01-01'::timestamp) from {{this}})

{% endif %}
