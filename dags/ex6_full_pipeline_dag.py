from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from slack_callbacks import on_failure_slack_alert

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_slack_alert,
    'email': ['jerem9911@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='ex6_full_dbt_pipeline',
    default_args=default_args,
    description='Pipeline complet dbt : staging -> intermediate -> mart -> docs',
    schedule='@daily',  # Planification quotidienne
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'orchestration']
) as dag:

    # RUN + TEST Staging
    run_staging = BashOperator(
        task_id='run_staging',
        bash_command="cd /opt/airflow/dbt && dbt run --select staging.bike_database --profiles-dir ./.dbt_profiles"
    )

    test_staging = BashOperator(
        task_id='test_staging',
        bash_command="cd /opt/airflow/dbt && dbt test --select staging.bike_database --profiles-dir ./.dbt_profiles"
    )

    # RUN + TEST Intermediate
    run_intermediate = BashOperator(
        task_id='run_intermediate',
        bash_command="cd /opt/airflow/dbt && dbt run --select intermediate.bike_database --profiles-dir ./.dbt_profiles"
    )

    test_intermediate = BashOperator(
        task_id='test_intermediate',
        bash_command="cd /opt/airflow/dbt && dbt test --select intermediate.bike_database --profiles-dir ./.dbt_profiles"
    )

    # RUN + TEST Mart
    run_mart = BashOperator(
        task_id='run_mart',
        bash_command="cd /opt/airflow/dbt && dbt run --select mart.operations --profiles-dir ./.dbt_profiles"
    )

    test_mart = BashOperator(
        task_id='test_mart',
        bash_command="cd /opt/airflow/dbt && dbt test --select mart.operations --profiles-dir ./.dbt_profiles"
    )

    # Génération docs
    generate_docs = BashOperator(
        task_id='generate_docs',
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir ./.dbt_profiles"
    )

    # Définir l’ordre d’exécution
    run_staging >> test_staging >> run_intermediate >> test_intermediate >> run_mart >> test_mart >> generate_docs
