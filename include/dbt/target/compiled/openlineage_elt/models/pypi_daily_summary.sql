

select
    DATE_TRUNC(DATE(timestamp), DAY) AS day,
    project,
    version,
    count(*) AS num_downloads
from `openlineage`.`metrics`.`pypi_downloads`

group by day, project, version
order by day desc