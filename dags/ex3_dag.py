
#./dags/ex3_dag.py

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
    dag_id='ex3_intermediate_pipeline',
    default_args=default_args,
    description='Exécuter dbt intermediate avec vérification dataset',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'bigquery']
) as dag:


    # Étape 1 : dbt run sur l'intermediate
    run_intermediate = BashOperator(
        task_id='run_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt run --select intermediate.bike_database --profiles-dir ./.dbt_profiles'
    )

    # Étape 2 : dbt test sur l'intermediate
    test_intermediate = BashOperator(
        task_id='test_intermediate',
        bash_command='cd /opt/airflow/dbt && dbt test --select intermediate.bike_database --profiles-dir ./.dbt_profiles'
    )

    # Orchestration
    run_intermediate >> test_intermediate

