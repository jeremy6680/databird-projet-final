from functools import partial

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

from dbt_monitor import capture_run_results
from slack_callbacks import on_failure_slack_alert

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_alert,
    'email': ['jerem9911@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='ex6_1_staging',
    default_args=default_args,
    description='Couche staging : dbt run + test',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'staging'],
) as dag:

    run_staging = BashOperator(
        task_id='run_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging.bike_database --profiles-dir ./.dbt_profiles',
    )

    monitor_staging = PythonOperator(
        task_id='monitor_staging',
        python_callable=partial(capture_run_results, "staging"),
    )

    test_staging = BashOperator(
        task_id='test_staging',
        bash_command='cd /opt/airflow/dbt && dbt test --select staging.bike_database --profiles-dir ./.dbt_profiles',
    )

    trigger_intermediate = TriggerDagRunOperator(
        task_id='trigger_intermediate',
        trigger_dag_id='ex6_2_intermediate',
        wait_for_completion=False,
    )

    run_staging >> monitor_staging >> test_staging >> trigger_intermediate
