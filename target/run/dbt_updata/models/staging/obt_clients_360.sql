

  create or replace view `data-53f1`.`Curated_zone`.`obt_clients_360`
  OPTIONS()
  as with source as (
  SELECT * FROM
  `data-53f1.Landing_zone_teradata.clients_360`
)
select
    *,
    'xxxxxxxxxxxx@xxxxx.xxx' as mail
from source;

