

   

      -- generated script to merge partitions into `data-53f1`.`Curated_zone`.`dim_yoy`
      declare dbt_partitions_for_replacement array<date>;

      
      
        

        -- 1. create a temp table
        
  
    

    create or replace table `data-53f1`.`Curated_zone`.`dim_yoy__dbt_tmp`
    partition by event_date_2
    
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      


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
    );
  
      

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct event_date_2)
          from `data-53f1`.`Curated_zone`.`dim_yoy__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `data-53f1`.`Curated_zone`.`dim_yoy` as DBT_INTERNAL_DEST
        using (
        select * from `data-53f1`.`Curated_zone`.`dim_yoy__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and DBT_INTERNAL_DEST.event_date_2 in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`event_date_2`, `event_timestamp`, `event_name`, `user_pseudo_id`, `vendor_id`, `key`, `string_value`, `int_value`, `ingestion_timestamp`)
    values
        (`event_date_2`, `event_timestamp`, `event_name`, `user_pseudo_id`, `vendor_id`, `key`, `string_value`, `int_value`, `ingestion_timestamp`)

;

      -- 4. clean up the temp table
      drop table if exists `data-53f1`.`Curated_zone`.`dim_yoy__dbt_tmp`

  


    