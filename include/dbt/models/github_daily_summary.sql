{{
    config(
        materialized='table'
    )
}}

with combined_stats as (
    select DATE_TRUNC(DATE(timestamp), DAY) AS day, project, stars, forks
    from {{ ref('github_stats') }}
    union all
    select *, null as forks from {{ ref('github_daily_summary_history') }}
)

select
    day,
    project,
    max(stars) AS stars,
    max(forks) AS forks
from combined_stats
group by day, project
