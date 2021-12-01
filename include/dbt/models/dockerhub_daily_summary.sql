{{
    config(
        materialized='table'
    )
}}

with combined_stats as (
    select DATE_TRUNC(DATE(timestamp), DAY) AS day, image, pull_count
    from {{ ref('dockerhub_stats') }}
    union all
    select * from {{ ref('dockerhub_daily_summary_history') }}
)

select
    day,
    image,
    max(pull_count) AS total_pulls
from combined_stats
group by day, image