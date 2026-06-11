#!/bin/bash

# Start the Airflow standalone container with dbt project mounted
docker run -d --name airflow-standalone \
  --env-file .env \
  -p 8080:8080 \
  -p 8081:8081 \
  -v ./dags:/opt/airflow/dags \
  -v ./dbt:/opt/airflow/dbt \
  dbt_pipeline standalone