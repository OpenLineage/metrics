with combined_stars as (
    select DATE_TRUNC(DATE(timestamp), DAY) AS day, project, stars
    from {{ source('metrics', 'github_stars') }}
    union all
    select * from {{ ref('github_daily_summary_history') }}
)

select
    day,
    project,
    max(stars) AS stars
from combined_stars
group by day, project
