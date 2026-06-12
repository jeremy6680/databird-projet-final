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
    dag_id='ex6_3_mart',
    default_args=default_args,
    description='Couche mart : dbt run + test',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'mart'],
) as dag:

    run_mart = BashOperator(
        task_id='run_mart',
        bash_command='cd /opt/airflow/dbt && dbt run --select mart.operations --profiles-dir ./.dbt_profiles',
    )

    test_mart = BashOperator(
        task_id='test_mart',
        bash_command='cd /opt/airflow/dbt && dbt test --select mart.operations --profiles-dir ./.dbt_profiles',
    )

    trigger_docs = TriggerDagRunOperator(
        task_id='trigger_docs',
        trigger_dag_id='ex6_4_docs',
        wait_for_completion=False,
    )

    run_mart >> test_mart >> trigger_docs
