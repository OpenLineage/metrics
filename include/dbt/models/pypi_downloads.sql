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
    or file.project = 'openlineage-python'
    or file.project = 'openlineage-integration'
    or file.project = 'openlineage-dbt'
    or file.project = 'openlineage-airflow'
)

-- this is the earliest known data in the dataset
and timestamp > TIMESTAMP_SECONDS(1549497600)

{% if is_incremental() %}
  and timestamp > (select max(timestamp) from {{ this }})
{% endif %}
