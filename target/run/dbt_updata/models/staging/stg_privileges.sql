

  create or replace view `data-53f1`.`dl_northwind`.`stg_privileges`
  OPTIONS()
  as with source as (

    select * from `data-53f1`.`dl_northwind`.`privileges`
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source;

