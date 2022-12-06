{{
 config(
   materialized = 'incremental',
   partition_by = {'field': 'event_date_2'},
   incremental_strategy = 'insert_overwrite',
   tags=['incremental']
 )
}}


with source as (

  SELECT *
  from(
  SELECT
  parse_DATE('%Y%m%d',
    event_date) AS event_date_2,
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp_2,
  TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL -3 HOUR) AS actual_time,
  event_name,
  param.value.int_value as session,
  user_id,
  --user_properties,
  ecommerce.transaction_id  as id_order,
  items.item_id as item_id,
  items.item_name as item_name,
  items.item_brand as item_brand,
  items.item_category as item_category,
  items.item_category2,
  items.price_in_usd,
  items.price
  FROM `data-53f1.analytics_334322316.events_*`,
      unnest(items) as items,
      unnest(event_params) as param
  WHERE 1=1
  and event_name in ('purchase','view_item','remove_from_cart','add_to_cart')
  and (param.value.int_value is not null and param.key ='ga_session_id')
  ORDER BY
  event_timestamp desc)
)
select
    *,
    current_timestamp() as ingestion_timestamp
from source
