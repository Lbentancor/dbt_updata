

  create or replace view `data-53f1`.`dl_northwind`.`stg_inventory_transactions`
  OPTIONS()
  as with source as (

    select * from `data-53f1`.`dl_northwind`.`inventory_transactions`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source;

