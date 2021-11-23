{{
    config(
        materialized='view'
    )
}}

select
    DATE_TRUNC(DATE(timestamp), DAY) AS day,
    image,
    max(pull_count) AS total_pulls
from {{ source('metrics', 'dockerhub_pulls') }}

group by day, image
