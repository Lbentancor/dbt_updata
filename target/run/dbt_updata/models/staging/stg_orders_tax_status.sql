

  create or replace view `data-53f1`.`dl_northwind`.`stg_orders_tax_status`
  OPTIONS()
  as with source as (

    select * from `data-53f1`.`dl_northwind`.`orders_tax_status`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source;

