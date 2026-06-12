from airflow import DAG
from airflow.operators.bash import BashOperator
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
    dag_id='ex6_4_docs',
    default_args=default_args,
    description='Génération de la documentation dbt',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'docs'],
) as dag:

    generate_docs = BashOperator(
        task_id='generate_docs',
        bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir ./.dbt_profiles',
    )

    generate_docs
