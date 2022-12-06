

  create or replace view `data-53f1`.`dl_northwind`.`stg_purchase_orders`
  OPTIONS()
  as with source as (

    select * from `data-53f1`.`dl_northwind`.`purchase_orders`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source;

