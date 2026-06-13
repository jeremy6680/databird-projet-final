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
    dag_id='ex6_2_intermediate',
    default_args=default_args,
    description='Couche intermediate : dbt run + test',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'intermediate'],
) as dag:

    run_intermediate = BashOperator(
        task_id='run_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --select intermediate.bike_database --profiles-dir ./.dbt_profiles',
    )

    monitor_intermediate = PythonOperator(
        task_id='monitor_intermediate',
        python_callable=partial(capture_run_results, "intermediate"),
    )

    test_intermediate = BashOperator(
        task_id='test_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt test --select intermediate.bike_database --profiles-dir ./.dbt_profiles',
    )

    trigger_mart = TriggerDagRunOperator(
        task_id='trigger_mart',
        trigger_dag_id='ex6_3_mart',
        wait_for_completion=False,
    )

    run_intermediate >> monitor_intermediate >> test_intermediate >> trigger_mart
