{{
    config(
        materialized='incremental'
    )
}}

select
    timestamp,
    country_code,
    url,
    file.project as project,
    file.version as version,
    file.type as type

from {{ source('pypi', 'file_downloads') }}

where (
    file.project = 'marquez-python'
    or file.project = 'marquez-airflow'
)

-- this is the earliest data that is known in the public dataset
-- we need this for partition elimination
and timestamp > TIMESTAMP_SECONDS(1549497600)

{% if is_incremental() %}
  and timestamp > (select max(timestamp) from {{ this }})
{% endif %}
