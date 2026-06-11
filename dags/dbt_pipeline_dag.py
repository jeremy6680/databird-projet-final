from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_run_pipeline',
    default_args=default_args,
    description='Exécuter un dbt run via BashOperator',
    schedule_interval=None,  # Exécution manuelle uniquement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'bigquery']
) as dag:

    # Tâche Airflow pour lancer dbt run
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir ./.dbt_profiles',
        dag=dag
    )
    
    dbt_run
