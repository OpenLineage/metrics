{{
    config(
        materialized='view'
    )
}}

select
    timestamp,
    json_value(http_response,'$.full_name') as project,
    cast(json_value(http_response,'$.watchers') as int) as stars,
    cast(json_value(http_response,'$.forks') as int) as forks,
from {{ source('metrics', 'github_stats_snapshot') }}