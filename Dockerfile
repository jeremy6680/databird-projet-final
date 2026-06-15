# Étape 1 : Base image Airflow
FROM apache/airflow:3.1.7-python3.11

# Étape 2 : Définir l'utilisateur pour Airflow - Installer dbt + clients BigQuery
USER airflow
RUN pip install --no-cache-dir dbt-bigquery==1.7.5

# Étape 3 : Copier le code Airflow et dbt dans le conteneur
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root include/ /opt/airflow/include/
# Si votre projet dbt local n’est pas nommé "dbt", modifiez cette ligne en conséquence
# --chown : sans ça, COPY assigne les fichiers à root même si USER airflow est actif
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/

ENV PYTHONPATH=/opt/airflow/include

# Étape 4 : Pré-installer les packages dbt dans l’image
# En K8s chaque pod a son propre filesystem — on ne peut pas partager un volume monté depuis l’hôte.
# dbt deps télécharge les packages (packages.yml) sans avoir besoin de profiles.
RUN cd /opt/airflow/dbt && dbt deps --no-use-colors

# Étape 5 : Installer les dépendances Airflow additionnelles
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# La commande est définie par chaque service dans docker-compose.yml