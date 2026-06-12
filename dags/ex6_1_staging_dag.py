from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
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

    test_staging = BashOperator(
        task_id='test_staging',
        bash_command='cd /opt/airflow/dbt && dbt test --select staging.bike_database --profiles-dir ./.dbt_profiles',
    )

    trigger_intermediate = TriggerDagRunOperator(
        task_id='trigger_intermediate',
        trigger_dag_id='ex6_2_intermediate',
        wait_for_completion=False,
    )

    run_staging >> test_staging >> trigger_intermediate
