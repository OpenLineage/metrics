

with pypi_summary as (
    select
        day,
        sum(num_downloads) as pypi_downloads
    from `openlineage`.`metrics`.`pypi_daily_summary`
    group by day
),
dockerhub_summary as (
    select
        day,
        sum(total_pulls) as dockerhub_pulls
    from `openlineage`.`metrics`.`dockerhub_daily_summary`
    group by day
),
github_summary as (
    select
        day,
        sum(stars) as github_stars
    from `openlineage`.`metrics`.`github_daily_summary`
    group by day
),
slack_summary as (
    select
        day,
        sum(messages) as slack_messages
    from `openlineage`.`metrics`.`slack_daily_summary`
    group by day
)

select
    github_summary.day,
    pypi_summary.pypi_downloads,
    dockerhub_summary.dockerhub_pulls,
    github_summary.github_stars,
    slack_summary.slack_messages
from github_summary
left join dockerhub_summary on dockerhub_summary.day = github_summary.day
left join pypi_summary on pypi_summary.day = github_summary.day
left join slack_summary on slack_summary.day = github_summary.day
order by day desc