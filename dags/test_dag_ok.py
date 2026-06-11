from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_run_test_model',
    default_args=default_args,
    description='Exécute uniquement le modèle test_model avec dbt',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'bigquery', 'test_model']
) as dag:

    dbt_run_test = BashOperator(
        task_id='run_test_model',
        bash_command="cd /opt/airflow/dbt && dbt run --select test_model --profiles-dir ./.dbt_profiles",
    )

    dbt_run_test
