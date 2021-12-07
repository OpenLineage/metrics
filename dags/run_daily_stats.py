import os
from datetime import datetime

from airflow import DAG
from airflow_dbt.operators.dbt_operator import (DbtSeedOperator, DbtRunOperator)

from http_to_bigquery import HttpToBigQueryOperator

with DAG(
    dag_id='openlineage_metrics',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    DBT_DIR=os.environ['AIRFLOW_HOME']+'/include/dbt/'

    dbt_seed = DbtSeedOperator(
        task_id='dbt_seed',
        dbt_bin='dbt-ol',
        profiles_dir=DBT_DIR,
        dir=DBT_DIR,
    )

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        dbt_bin='dbt-ol',
        profiles_dir=DBT_DIR,
        dir=DBT_DIR,
    )

    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ['AIRFLOW_HOME']+'/openlineage.json'
 
    os.environ['AIRFLOW_CONN_HTTP_GITHUB'] = 'https://api.github.com'

    # Pull stats for these projects
    projects = ['marquezproject/marquez', 'openlineage/openlineage']

    for project in projects: 
        shortname = project.split('/')[1]

        load_gh_stats = HttpToBigQueryOperator(
            task_id='load_github_stats_' + shortname,
            http_conn_id='http_github',
            endpoint='/repos/' + project,
            gcp_conn_id='openlineage',
            project_id='openlineage',
            dataset_id='metrics',
            table_id='github_stats_snapshot',
        )

        load_gh_stats >> dbt_run

    os.environ['AIRFLOW_CONN_HTTP_DOCKER'] = 'https://hub.docker.com'

    # Pull stats for these images
    images = ['marquezproject/marquez', 'marquezproject/marquez-web']

    for image in images:
        shortname = image.split('/')[1]

        load_dh_stats = HttpToBigQueryOperator(
            task_id='load_docker_stats_' + shortname,
            http_conn_id='http_docker',
            endpoint='/v2/repositories/' + image + '/',
            gcp_conn_id='openlineage',
            project_id='openlineage',
            dataset_id='metrics',
            table_id='dockerhub_stats_snapshot',
        )

        load_dh_stats >> dbt_run

    dbt_seed >> dbt_run
