
  create view "my_db"."public"."dim_billing__dbt_tmp"
    
    
  as (
    with billing as (
    select  "InvoiceID" as Invoice_id,
        "PatientID"  as Patient_id,
        "Items" as Procedure,
        "Amount"
    from "my_db"."public"."Billing"   
)

select distinct
    Invoice_id,
    Patient_id,
    Procedure,
    sum("Amount") over (partition by Procedure) as total_amount
from billing
order by total_amount desc ,Patient_id
  );