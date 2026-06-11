from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
		dag_id='ex1_dbt_test_tables_brand_customers',
    default_args=default_args,
    description='Exécuter dbt run et dbt test table par table en staging',
    schedule=None,  # Exécution manuelle
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'staging', 'table-by-table']
) as dag:

    # ----------- TABLE brands -----------

    dbt_run_brands = BashOperator(
        task_id='run_staging_brands',
        bash_command="cd /opt/airflow/dbt && dbt run --select staging.bike_database.stg_bike_database__brands --profiles-dir ./.dbt_profiles"
    )

    dbt_test_brands = BashOperator(
        task_id='test_staging_brands',
        bash_command="cd /opt/airflow/dbt && dbt test --select staging.bike_database.stg_bike_database__brands --profiles-dir ./.dbt_profiles"
    )

    # ----------- TABLE customers -----------

    dbt_run_customers = BashOperator(
        task_id='run_staging_customers',
        bash_command="cd /opt/airflow/dbt && dbt run --select staging.bike_database.stg_bike_database__customers --profiles-dir ./.dbt_profiles"
    )

    dbt_test_customers = BashOperator(
        task_id='test_staging_customers',
        bash_command="cd /opt/airflow/dbt && dbt test --select staging.bike_database.stg_bike_database__customers --profiles-dir ./.dbt_profiles"
    )

    # Dépendances
    dbt_run_brands >> dbt_test_brands
    dbt_run_customers >> dbt_test_customers
