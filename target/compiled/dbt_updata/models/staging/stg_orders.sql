with source as (

    select * from `data-53f1`.`dl_northwind`.`orders`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source