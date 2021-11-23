

with combined_stars as (
    select DATE_TRUNC(DATE(timestamp), DAY) AS day, project, stars
    from `openlineage`.`metrics`.`github_stars`
    union all
    select * from `openlineage`.`metrics`.`github_daily_summary_history`
)

select
    day,
    project,
    max(stars) AS stars
from combined_stars
group by day, project