

select
    DATE_TRUNC(DATE(timestamp), DAY) AS day,
    image,
    max(pull_count) AS total_pulls
from `openlineage`.`metrics`.`dockerhub_pulls`

group by day, image
order by day desc