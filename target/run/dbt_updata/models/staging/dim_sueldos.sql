

  create or replace view `data-53f1`.`Curated_zone`.`dim_sueldos`
  OPTIONS()
  as with source as (

  select
  event_name,
  event_timestamp,
  params.key as pa_key,
  key,
  params.value.string_value as pa_s_value,
  params.value.int_value  as pa_i_value,
  geo.continent,
  geo.country,
  geo.city,
  geo.sub_continent,
  geo.metro,
  app_info.id,
  app_info.version,
  app_info.firebase_app_id,
  stream_id,
  platform,
  key,
  value.string_value,
  value.int_value
  FROM `data-53f1.analytics_277539890.events_*`,
       UNNEST(event_params) as params 
)
select
    *,
    current_timestamp() as ingestion_timestamp
from source;

