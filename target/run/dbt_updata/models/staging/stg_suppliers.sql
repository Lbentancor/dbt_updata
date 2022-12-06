

  create or replace view `data-53f1`.`dl_northwind`.`stg_suppliers`
  OPTIONS()
  as with source as (

    select * from `data-53f1`.`dl_northwind`.`suppliers`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source;

