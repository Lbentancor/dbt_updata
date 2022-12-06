with source as (

    select * from `data-53f1`.`dl_northwind`.`invoices`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source