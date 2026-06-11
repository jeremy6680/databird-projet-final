#dags/ex2_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ex2_dbt_run_staging_layer',
    default_args=default_args,
    description='Exécution globale de la couche Staging avec dbt',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'staging', 'bigquery']
) as dag:

    # Lancer tous les modèles du dossier staging
    dbt_run_staging = BashOperator(
        task_id='run_staging_models',
        bash_command="cd /opt/airflow/dbt && dbt run --select staging.bike_database --profiles-dir ./.dbt_profiles"
    )

    # Tester tous les modèles du dossier staging
    dbt_test_staging = BashOperator(
        task_id='test_staging_models',
        bash_command="cd /opt/airflow/dbt && dbt test --select staging.bike_database --profiles-dir ./.dbt_profiles"
    )

    dbt_run_staging >> dbt_test_staging
