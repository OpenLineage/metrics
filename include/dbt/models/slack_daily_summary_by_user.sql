{{
    config(
        materialized='table'
    )
}}

select
    DATE_TRUNC(DATE(message_time), DAY) AS day,
    domain,
    username,
    count(*) AS messages
from {{ source('metrics', 'slack_messages') }}

where username != 'github'
and username != 'github2'

group by day, domain, username
order by day desc
