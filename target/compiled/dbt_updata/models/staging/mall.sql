with source as (

SELECT *
from(
SELECT
parse_DATE('%Y%m%d',
  event_date) AS event_date_2,
TIMESTAMP_MICROS(event_timestamp) AS event_timestamp_2,
TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL -3 HOUR) AS actual_time,
event_name,
user_id,
--user_properties,
ecommerce.transaction_id  as id_order,
items
FROM `data-53f1.analytics_334322316.events_*`
WHERE 1=1
--and user_properties is not null
--and user_id = '19951507'
--and ecommerce.transaction_id is not null
--and (select value.string_value from unnest(event_params) where key = 'page_location') like '%AdminOrdersupdate%'
--and event_name not like '%scroll%'
--and event_name not like '%ux_timer%'
and event_name in ('purchase','view_item','remove_from_cart','add_to_cart')
--and event_name in ('view_cart','purchase','refund','begin_checkout','add_payment_info','add_shipping_info','remove_from_cart','view_item','add_to_cart','login', 'login_step_1','add_to_whishlist','search','select_item','select_promotion','sign_up','view_item_list','view_promotion','')
ORDER BY
event_timestamp desc)
)
select
    *,
    current_timestamp() as ingestion_timestamp
from source