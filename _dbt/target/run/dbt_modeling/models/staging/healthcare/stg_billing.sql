
  create view "my_db"."public"."stg_billing__dbt_tmp"
    
    
  as (
    select  "InvoiceID" as Invoice_id,
        "PatientID"  as Patient_id,
        "Items" as Procedure,
        "Amount"
    from "my_db"."public"."Billing"
  );