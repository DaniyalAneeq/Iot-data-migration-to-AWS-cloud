{{
  config(
    materialized='table'
  )
}}

with silver as (

    select * from {{ ref('silver_iot_events') }}

),

daily_aggregates as (

    select
        device_id,
        cast(event_timestamp as date)                                              as event_date,
        count(*)                                                                     as reading_count,
        avg(latitude)                                                                as avg_latitude,
        avg(longitude)                                                               as avg_longitude,
        min(latitude)                                                                as min_latitude,
        max(latitude)                                                                as max_latitude,
        min(longitude)                                                               as min_longitude,
        max(longitude)                                                               as max_longitude,
        min(event_timestamp)                                                         as first_reading_at,
        max(event_timestamp)                                                         as last_reading_at,
        avg(ingestion_lag_seconds)                                                   as avg_ingestion_lag_seconds,
        max(ingestion_lag_seconds)                                                   as max_ingestion_lag_seconds,
        sum(case when data_quality_severity = 'on_time' then 1 else 0 end)          as on_time_readings,
        sum(case when data_quality_severity = 'delayed' then 1 else 0 end)          as delayed_readings,
        sum(case when data_quality_severity = 'invalid_location' then 1 else 0 end) as invalid_location_readings
    from silver
    group by device_id, cast(event_timestamp as date)

)

select
    device_id,
    event_date,
    reading_count,
    round(avg_latitude, 6)  as avg_latitude,
    round(avg_longitude, 6) as avg_longitude,
    min_latitude,
    max_latitude,
    min_longitude,
    max_longitude,
    first_reading_at,
    last_reading_at,
    round(avg_ingestion_lag_seconds, 2) as avg_ingestion_lag_seconds,
    max_ingestion_lag_seconds,
    on_time_readings,
    delayed_readings,
    invalid_location_readings,
    round(100.0 * on_time_readings / nullif(reading_count, 0), 2) as on_time_pct
from daily_aggregates
order by device_id, event_date
