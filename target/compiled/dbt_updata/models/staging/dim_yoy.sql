


with source as (
  select parse_DATE('%Y%m%d',
         event_date) AS event_date_2,
         event_timestamp,
         event_name,
         user_pseudo_id,
         device.vendor_id,
         param.key,
         param.value.string_value,
         param.value.int_value
  FROM `data-53f1.analytics_306108715.events_*`,
      unnest(event_params) as param
  where event_name in ('call_to_action_exploracion') and
        param.key in ('sessionID','af_id')
  union all
  select parse_DATE('%Y%m%d',
         event_date) AS event_date_2,
         event_timestamp,
         event_name,
         user_pseudo_id,
         device.vendor_id,
         param.key,
         param.value.string_value,
         param.value.int_value
  FROM `data-53f1.analytics_306108715.events_*`,
      unnest(event_params) as param
  where event_name in ('session_start_m') and
        param.key= 'sessionID'
  union all
  select parse_DATE('%Y%m%d',
         event_date) AS event_date_2,
         event_timestamp,
         event_name,
         user_pseudo_id,
         device.vendor_id,
         param.key,
         param.value.string_value,
         param.value.int_value
  FROM `data-53f1.analytics_306108715.events_*`,
      unnest(event_params) as param
  where event_name in ('screen_view') and
        param.key in ('sessionID','firebase_screen_id','firebase_previous_id') and
        param.value.int_value is not null
)
select
    *,
    current_timestamp() as ingestion_timestamp
from source