{{
    config(
        materialized='view'
    )
}}

select
    DATE_TRUNC(DATE(message_time), DAY) AS day,
    domain,
    count(*) AS messages
from {{ source('metrics', 'slack_messages') }}
where username != 'github'
and username != 'github2'
group by day, domain

union all

select * from {{ ref('slack_daily_summary_history') }}
