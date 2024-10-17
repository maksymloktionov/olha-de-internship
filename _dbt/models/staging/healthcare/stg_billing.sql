select  "InvoiceID" as Invoice_id,
        "PatientID"  as Patient_id,
        "Items" as Procedure,
        "Amount"
    from {{ref ('billing')}}      
     
