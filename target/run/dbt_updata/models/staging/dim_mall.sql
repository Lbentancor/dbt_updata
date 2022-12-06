

   

      -- generated script to merge partitions into `data-53f1`.`Curated_zone`.`dim_mall`
      declare dbt_partitions_for_replacement array<date>;

      
      
        

        -- 1. create a temp table
        
  
    

    create or replace table `data-53f1`.`Curated_zone`.`dim_mall__dbt_tmp`
    partition by event_date_2
    
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      


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
    );
  
      

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct event_date_2)
          from `data-53f1`.`Curated_zone`.`dim_mall__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `data-53f1`.`Curated_zone`.`dim_mall` as DBT_INTERNAL_DEST
        using (
        select * from `data-53f1`.`Curated_zone`.`dim_mall__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and DBT_INTERNAL_DEST.event_date_2 in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`event_date_2`, `event_timestamp_2`, `actual_time`, `event_name`, `session`, `user_id`, `id_order`, `item_id`, `item_name`, `item_brand`, `item_category`, `item_category2`, `price_in_usd`, `price`, `ingestion_timestamp`)
    values
        (`event_date_2`, `event_timestamp_2`, `actual_time`, `event_name`, `session`, `user_id`, `id_order`, `item_id`, `item_name`, `item_brand`, `item_category`, `item_category2`, `price_in_usd`, `price`, `ingestion_timestamp`)

;

      -- 4. clean up the temp table
      drop table if exists `data-53f1`.`Curated_zone`.`dim_mall__dbt_tmp`

  


    