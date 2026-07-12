{{
  config(
    materialized='table'
  )
}}

with source as (

    select
        record_content:payload:after:id::number            as event_id,
        record_content:payload:after:device_id::string      as device_id,
        record_content:payload:after:latitude::float        as latitude,
        record_content:payload:after:longitude::float       as longitude,
        to_timestamp_ntz(record_content:payload:after:event_timestamp::number, 6) as event_timestamp,
        to_timestamp_ntz(record_content:payload:after:ingested_at::number, 6)     as ingested_at,
        record_content:payload:op::string                   as cdc_operation,
        to_timestamp_ntz(record_content:payload:source:ts_ms::number, 3)         as source_captured_at,
        to_timestamp_ntz(record_content:payload:ts_ms::number, 3)                as cdc_processed_at
    from {{ source('raw', 'iot_events') }}
    where record_content:payload:after is not null   -- drop delete events (no "after" state)

),

validated as (

    select
        event_id,
        device_id,
        latitude,
        longitude,
        event_timestamp,
        ingested_at,
        cdc_operation,
        source_captured_at,
        cdc_processed_at,
        datediff('second', event_timestamp, ingested_at) as ingestion_lag_seconds,

        case
            when latitude is null or longitude is null then 'invalid_location'
            when datediff('second', event_timestamp, ingested_at) > 30 then 'delayed'
            else 'on_time'
        end as data_quality_severity

    from source
    where device_id is not null
      and latitude is not null
      and longitude is not null
      and event_timestamp is not null

)

select * from validated
