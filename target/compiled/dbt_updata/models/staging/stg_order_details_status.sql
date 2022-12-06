with source as (

    select * from `data-53f1`.`dl_northwind`.`order_details_status`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source