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
    dag_id='ex4_mart_pipeline',
    default_args=default_args,
    description='Créer le mart et exécuter dbt pour la couche mart',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'bigquery']
) as dag:

    # Étape 1 : dbt run sur le mart
    run_mart = BashOperator(
        task_id='run_mart',
        bash_command='cd /opt/airflow/dbt && dbt run --select mart.operations --profiles-dir ./.dbt_profiles'
    )

    # Étape 2 : dbt test sur le mart
    test_mart = BashOperator(
        task_id='test_mart',
        bash_command='cd /opt/airflow/dbt && dbt test --select mart.operations --profiles-dir ./.dbt_profiles'
    )
    
    run_mart >> test_mart