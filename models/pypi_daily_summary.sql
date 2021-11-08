{{
    config(
        materialized='table'
    )
}}

select
    DATE_TRUNC(DATE(timestamp), DAY) AS day,
    project,
    version,
    count(*) AS num_downloads
from {{ ref('pypi_downloads') }}

group by day, project, version
order by day desc
