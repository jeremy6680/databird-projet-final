from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Paramètres par défaut
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ex5_dbt_docs_pipeline',
    default_args=default_args,
    description='Générer et servir la documentation dbt',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'docs']
) as dag:

    generate_docs = BashOperator(
        task_id='generate_docs',
        bash_command='cd /opt/airflow/dbt && dbt docs generate --profiles-dir ./.dbt_profiles'
    )


    generate_docs
