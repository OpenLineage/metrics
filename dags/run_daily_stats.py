import requests
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtRunOperator,
)

with DAG(
    dag_id='openlineage_metrics',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ['AIRFLOW_HOME']+'/openlineage.json'
    DBT_DIR=os.environ['AIRFLOW_HOME']+'/include/dbt/'

    def get_github_stats(**kwargs):
        """Get GitHub stats for the given project."""

        response = requests.get('https://api.github.com/repos/' + kwargs['project'])
        stars = response.json()['watchers']
        forks = response.json()['forks']

        ti = kwargs['ti']
        ti.xcom_push(key='project', value=kwargs['project'])
        ti.xcom_push(key='stars', value=stars)
        ti.xcom_push(key='forks', value=forks)

    def get_docker_stats(**kwargs):
        """Get Docker Hub stats for the given image."""

        response = requests.get('https://hub.docker.com/v2/repositories/' + kwargs['image'])
        total_pulls = response.json()['pull_count']

        ti = kwargs['ti']
        ti.xcom_push(key='project', value=kwargs['image'])
        ti.xcom_push(key='total_pulls', value=total_pulls)

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

    # Pull stats for these projects
    projects = ['MarquezProject/marquez', 'OpenLineage/openlineage']

    for project in projects: 
        shortname = project.split('/')[1]

        get_gh_stats = PythonOperator(
            task_id='get_github_stats_' + shortname,
            python_callable=get_github_stats,
            op_kwargs={'project': project},
        )

        # load_gh_stats = BigQueryInsertJobOperator(
        #     task_id='load_github_stats_' + shortname,
        #     gcp_conn_id='openlineage',
        #     configuration={
        #         "query": {
        #             "query": '''
        #                 INSERT `openlineage`.`metrics`.`github_stats` VALUES
        #                 (
        #                     CURRENT_TIMESTAMP(),
        #                     '{{{{ task_instance.xcom_pull(task_ids='get_github_stats_{}', key='project') }}}}',
        #                     {{{{ task_instance.xcom_pull(task_ids='get_github_stats_{}', key='stars') }}}},
        #                     {{{{ task_instance.xcom_pull(task_ids='get_github_stats_{}', key='forks') }}}}
        #                 )
        #             '''.format(shortname,shortname,shortname),
        #             "useLegacySql": False,
        #         }
        #     },
        # )
        load_gh_stats = BigQueryExecuteQueryOperator(
            task_id='load_github_stats_' + shortname,
            gcp_conn_id='openlineage',
            use_legacy_sql=False,
            sql='''
                INSERT `openlineage`.`metrics`.`github_stats` VALUES
                (
                    CURRENT_TIMESTAMP(),
                    '{{{{ task_instance.xcom_pull(task_ids='get_github_stats_{}', key='project') }}}}',
                    {{{{ task_instance.xcom_pull(task_ids='get_github_stats_{}', key='stars') }}}},
                    {{{{ task_instance.xcom_pull(task_ids='get_github_stats_{}', key='forks') }}}}
                )
            '''.format(shortname,shortname,shortname),
        )

        get_gh_stats >> load_gh_stats >> dbt_seed 

    # Pull stats for these images
    images = ['marquezproject/marquez', 'marquezproject/marquez-web']

    for image in images:
        shortname = image.split('/')[1]

        get_dh_stats = PythonOperator(
            task_id='get_docker_stats_' + shortname,
            python_callable=get_docker_stats,
            op_kwargs={'image': image},
        )

        # load_dh_stats = BigQueryInsertJobOperator(
        #     task_id='load_docker_stats_' + shortname,
        #     gcp_conn_id='openlineage',
        #     configuration={
        #         "query": {
        #             "query": '''
        #                 INSERT `openlineage`.`metrics`.`dockerhub_stats` VALUES
        #                 (
        #                     CURRENT_TIMESTAMP(),
        #                     '{{{{ task_instance.xcom_pull(task_ids='get_docker_stats_{}', key='project') }}}}',
        #                     {{{{ task_instance.xcom_pull(task_ids='get_docker_stats_{}', key='total_pulls') }}}}
        #                 )
        #             '''.format(shortname,shortname,shortname),
        #             "useLegacySql": False,
        #         }
        #     },
        # )
        load_dh_stats = BigQueryExecuteQueryOperator(
            task_id='load_docker_stats_' + shortname,
            gcp_conn_id='openlineage',
            use_legacy_sql=False,
            sql='''
                INSERT `openlineage`.`metrics`.`dockerhub_stats` VALUES
                (
                    CURRENT_TIMESTAMP(),
                    '{{{{ task_instance.xcom_pull(task_ids='get_docker_stats_{}', key='project') }}}}',
                    {{{{ task_instance.xcom_pull(task_ids='get_docker_stats_{}', key='total_pulls') }}}}
                )
            '''.format(shortname,shortname,shortname),
        )

        get_dh_stats >> load_dh_stats >> dbt_seed

    dbt_seed >> dbt_run