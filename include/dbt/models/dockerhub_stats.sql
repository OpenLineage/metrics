{{
    config(
        materialized='view'
    )
}}

select
    timestamp,
    json_value(http_response,'$.namespace') || '/' || json_value(http_response,'$.name') as image,
    cast(json_value(http_response,'$.pull_count') as int) as pull_count
from {{ source('metrics', 'dockerhub_stats_snapshot') }}
