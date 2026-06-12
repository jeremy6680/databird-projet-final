# Projet Final — Pipeline dbt orchestré avec Airflow & BigQuery

Pipeline de données complet qui transforme des données brutes d'une base de vélos en tables analytiques, en passant par 3 couches dbt (staging → intermediate → mart), le tout orchestré par Apache Airflow dans Docker et stocké dans Google BigQuery.

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Prérequis](#prérequis)
4. [Installation et démarrage](#installation-et-démarrage)
5. [Configuration BigQuery](#configuration-bigquery)
6. [Structure du projet](#structure-du-projet)
7. [Les DAGs Airflow](#les-dags-airflow)
8. [Les modèles dbt](#les-modèles-dbt)
9. [Monitoring & Reporting](#monitoring--reporting)
10. [Lancer le pipeline complet](#lancer-le-pipeline-complet)
11. [Documentation dbt](#documentation-dbt)

---

## Vue d'ensemble

Ce projet implémente un pipeline ELT (Extract → Load → **Transform**) :

- **Source** : tables brutes d'une base de données vélos (`bike_database`) déjà chargées dans BigQuery
- **Transformation** : dbt s'occupe de nettoyer, joindre et agréger les données en 3 couches
- **Orchestration** : Apache Airflow planifie et enchaîne les étapes
- **Infrastructure** : tout tourne dans un conteneur Docker

---

## Architecture

```
Source BigQuery (bike_database)
        │
        ▼
┌─────────────────────────────────────────┐
│  STAGING  (vues BigQuery)               │
│  stg_bike_database__*                   │
│  Nettoyage basique, renommage, cast      │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│  INTERMEDIATE  (vues BigQuery)          │
│  int_bike_database__*                   │
│  Jointures métier, calculs intermédiaires│
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│  MART  (tables BigQuery)                │
│  mrt_operations__*                      │
│  Tables analytiques finales             │
└─────────────────────────────────────────┘
```

Chaque étape est orchestrée par un DAG Airflow qui exécute `dbt run` puis `dbt test` avant de passer à la couche suivante.

---

## Prérequis

- [Docker](https://www.docker.com/get-started) installé et en cours d'exécution
- Un projet Google Cloud avec BigQuery activé
- Un compte de service GCP avec les droits BigQuery (`roles/bigquery.dataEditor` + `roles/bigquery.jobUser`)
- La clé JSON du compte de service

---

## Installation et démarrage

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd projet_final
```

### 2. Ajouter la clé BigQuery

Placer le fichier JSON de la clé de service dans :

```
dbt/.dbt_profiles/bigqueryKey.json
```

### 3. Vérifier le fichier `.env`

Le fichier `.env` à la racine contient les variables pour Docker :

```env
AIRFLOW_UID=501
AIRFLOW_GID=0
```

Adapter `AIRFLOW_UID` à votre UID local (`id -u` sur Mac/Linux).

### 4. Construire l'image Docker

```bash
docker build -t dbt_pipeline .
```

Cette image embarque Airflow 3.1.7, Python 3.11, et `dbt-bigquery==1.7.5`.

### 5. Démarrer le conteneur

```bash
bash init_airflow.sh
```

Ou manuellement :

```bash
docker run -d --name airflow-standalone \
  --env-file .env \
  -p 8080:8080 \
  -p 8081:8081 \
  -v ./dags:/opt/airflow/dags \
  -v ./dbt:/opt/airflow/dbt \
  dbt_pipeline standalone
```

> Les volumes `-v` permettent de modifier les DAGs et les modèles dbt **sans reconstruire l'image**.

### 6. Accéder à l'interface Airflow

Ouvrir [http://localhost:8080](http://localhost:8080)

Les identifiants par défaut d'Airflow standalone sont affichés dans les logs au premier démarrage :

```bash
docker logs airflow-standalone | grep "Login with username"
```

---

## Configuration BigQuery

Le fichier `dbt/.dbt_profiles/profiles.yml` définit la connexion :

```yaml
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: databird-prep-work-ae      # ← votre projet GCP
      dataset: local_bike_final_project   # ← dataset de sortie
      keyfile: /opt/airflow/dbt/.dbt_profiles/bigqueryKey.json
```

Les modèles dbt créeront automatiquement les datasets `stg_bike_database`, `int_bike_database`, et `mrt_operations` dans ce projet BigQuery.

---

## Structure du projet

```
projet_final/
├── Dockerfile                  # Image Airflow + dbt-bigquery
├── init_airflow.sh             # Script de démarrage Docker
├── requirements.txt            # Providers Airflow supplémentaires
├── .env                        # Variables d'environnement Docker
│
├── dags/                       # DAGs Airflow (montés en volume)
│   ├── slack_callbacks.py      # Callback d'alerte Slack sur échec de tâche
│   ├── dbt_monitor.py          # Module de monitoring : parsing artefacts dbt + BigQuery
│   ├── ex1_test_dag.py         # Exercice 1 : run/test table par table (staging)
│   ├── ex2_dag.py              # Exercice 2 : run/test couche staging complète
│   ├── ex3_dag.py              # Exercice 3 : run/test couche intermediate
│   ├── ex4_mart_dag.py         # Exercice 4 : run/test couche mart
│   ├── ex5_docs_dag.py         # Exercice 5 : génération de la doc dbt
│   ├── ex6_1_staging_dag.py    # Exercice 6a : staging seul, déclenche ex6_2
│   ├── ex6_2_intermediate_dag.py # Exercice 6b : intermediate seul, déclenche ex6_3
│   ├── ex6_3_mart_dag.py       # Exercice 6c : mart seul, déclenche ex6_4
│   ├── ex6_4_docs_dag.py       # Exercice 6d : génération docs (déclenchement final)
│   └── ex6_full_pipeline_dag.py # Exercice 6 full : pipeline complet avec monitoring (@daily)
│
└── dbt/                        # Projet dbt (monté en volume)
    ├── dbt_project.yml         # Configuration du projet dbt
    ├── packages.yml            # Dépendances dbt (dbt_utils)
    ├── .dbt_profiles/
    │   ├── profiles.yml        # Connexion BigQuery
    │   └── bigqueryKey.json    # ⚠️ À ajouter manuellement (non versionné)
    └── models/
        ├── sources.yml         # Déclaration des tables sources BigQuery
        ├── staging/bike_database/      # Couche 1 : nettoyage
        ├── intermediate/bike_database/ # Couche 2 : logique métier
        └── mart/operations/            # Couche 3 : tables analytiques
```

---

## Les DAGs Airflow

Les DAGs sont progressifs — chaque exercice introduit un nouveau concept.

| DAG | Déclenchement | Description |
|-----|--------------|-------------|
| `ex1_dbt_test_tables_brand_customers` | Manuel | run + test sur `brands` et `customers` séparément |
| `ex2_dbt_run_staging_layer` | Manuel | run + test sur toute la couche staging |
| `ex3_intermediate_pipeline` | Manuel | run + test sur toute la couche intermediate |
| `ex4_mart_pipeline` | Manuel | run + test sur toute la couche mart |
| `ex5_dbt_docs_pipeline` | Manuel | Génère la documentation dbt |
| `ex6_1_staging` | `@daily` | Staging seul ; déclenche automatiquement `ex6_2_intermediate` |
| `ex6_2_intermediate` | Déclenché par ex6_1 | Intermediate seul ; déclenche `ex6_3_mart` |
| `ex6_3_mart` | Déclenché par ex6_2 | Mart seul ; déclenche `ex6_4_docs` |
| `ex6_4_docs` | Déclenché par ex6_3 | Génération de la documentation dbt |
| `ex6_full_dbt_pipeline` | `@daily` | Pipeline monolithique avec monitoring BigQuery et rapport Slack |

### Graphe d'exécution de `ex6_full_dbt_pipeline` (pipeline complet avec monitoring)

```
run_staging → monitor_staging → test_staging
    → run_intermediate → monitor_intermediate → test_intermediate
    → run_mart → monitor_mart → test_mart
    → generate_docs → report_pipeline
```

> `monitor_*` : lit `dbt/target/run_results.json` après chaque `dbt run` et insère les métriques dans BigQuery.
> `report_pipeline` : agrège les résultats XCom de chaque étape et envoie un rapport Slack. S'exécute même en cas d'échec partiel (`trigger_rule='all_done'`).

### DAGs chaînés ex6_1 → ex6_4

L'alternative modulaire découpe le pipeline en 4 DAGs indépendants enchaînés via `TriggerDagRunOperator`. Chaque DAG peut être déclenché ou rejoué séparément.

```
ex6_1_staging ──trigger──▶ ex6_2_intermediate ──trigger──▶ ex6_3_mart ──trigger──▶ ex6_4_docs
```

---

## Les modèles dbt

### Staging (`stg_bike_database__*`)

Nettoyage minimal des données brutes : renommage de colonnes, cast de types, gestion des valeurs nulles.

| Modèle | Table source |
|--------|-------------|
| `stg_bike_database__brands` | `bike_database.brands` |
| `stg_bike_database__categories` | `bike_database.categories` |
| `stg_bike_database__customers` | `bike_database.customers` |
| `stg_bike_database__order_items` | `bike_database.order_items` |
| `stg_bike_database__orders` | `bike_database.orders` |
| `stg_bike_database__products` | `bike_database.products` |
| `stg_bike_database__staffs` | `bike_database.staffs` |
| `stg_bike_database__stocks` | `bike_database.stocks` |
| `stg_bike_database__stores` | `bike_database.stores` |

### Intermediate (`int_bike_database__*`)

Jointures et calculs intermédiaires à partir du staging.

| Modèle | Description |
|--------|-------------|
| `int_bike_database__orders` | Commandes enrichies avec montants et localisation client |
| `int_bike_database__order_items` | Lignes de commande avec montant total par ligne |
| `int_bike_database__products` | Produits avec catégorie et marque |
| `int_bike_database__stocks` | Stocks avec informations produit et magasin |

### Mart (`mrt_operations__*`)

Tables finales matérialisées en **tables** BigQuery (les autres couches sont des vues).

| Modèle | Description |
|--------|-------------|
| `mrt_operations__customers_order_summary` | Résumé par client : nb commandes, montant total, durée de vie |
| `mrt_operations__daily_order_performance` | Performance des commandes par jour |
| `mrt_operations__product_performance` | Performance des produits |
| `mrt_operations__store_product_stock_summary` | Stocks par magasin et produit |

---

## Monitoring & Reporting

### Module `dbt_monitor.py`

Après chaque `dbt run`, une tâche `PythonOperator` appelle `capture_run_results()` qui :

1. Lit `dbt/target/run_results.json` (artefact natif dbt)
2. Parse les métriques par modèle (statut, temps d'exécution, lignes affectées)
3. Insère les lignes dans BigQuery
4. Pousse un résumé en XCom pour le rapport final

### Table BigQuery `airflow_monitoring.dbt_run_metrics`

Le dataset et la table sont créés automatiquement au premier run.

| Colonne | Type | Description |
|---------|------|-------------|
| `run_id` | STRING | Identifiant du DAG run Airflow |
| `dag_id` | STRING | Nom du DAG |
| `logical_date` | TIMESTAMP | Date logique Airflow (clé de partition) |
| `step` | STRING | Couche dbt (`staging`, `intermediate`, `mart`) |
| `model_name` | STRING | Nom court du modèle |
| `model_unique_id` | STRING | ID complet dbt (ex: `model.projet_final.stg_...`) |
| `status` | STRING | `success`, `error`, `skipped` |
| `execution_time_seconds` | FLOAT64 | Durée d'exécution du modèle |
| `rows_affected` | INT64 | Lignes créées/modifiées |
| `dbt_version` | STRING | Version dbt utilisée |
| `generated_at` | TIMESTAMP | Horodatage de l'artefact dbt |
| `inserted_at` | TIMESTAMP | Horodatage d'insertion dans BigQuery |

> La table est **partitionnée par `logical_date`** (partition journalière) pour limiter les coûts de scan lors des requêtes analytiques.

### Alertes Slack

Deux types de notifications Slack sont envoyés si la variable d'environnement `SLACK_WEBHOOK_URL` est définie :

- **Alerte d'échec** (`slack_callbacks.py`) : envoyée sur chaque tâche en erreur, avec le lien vers les logs Airflow.
- **Rapport de fin de pipeline** (`send_pipeline_report` dans `dbt_monitor.py`) : envoyé à la fin de `ex6_full_dbt_pipeline`, résumant le statut, la durée totale et les métriques par couche.

Pour activer les notifications, ajouter dans `.env` :

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
```

---

## Lancer le pipeline complet

### Via l'interface Airflow (recommandé)

1. Ouvrir [http://localhost:8080](http://localhost:8080)
2. Activer le DAG `ex6_full_dbt_pipeline`
3. Cliquer sur **Trigger DAG** pour une exécution immédiate

### En ligne de commande depuis le conteneur

```bash
# Entrer dans le conteneur
docker exec -it airflow-standalone bash

# Lancer manuellement le pipeline complet
airflow dags trigger ex6_full_dbt_pipeline

# Ou lancer dbt directement
cd /opt/airflow/dbt
dbt run --profiles-dir ./.dbt_profiles
dbt test --profiles-dir ./.dbt_profiles
```

### Arrêter et supprimer le conteneur

```bash
docker stop airflow-standalone
docker rm airflow-standalone
```

---

## Documentation dbt

Pour consulter la documentation interactive des modèles :

1. Déclencher le DAG `ex5_dbt_docs_pipeline` (ou `ex6_full_dbt_pipeline`)
2. Depuis le conteneur, servir la doc :

```bash
docker exec -it airflow-standalone bash
cd /opt/airflow/dbt
dbt docs serve --profiles-dir ./.dbt_profiles --port 8081
```

3. Ouvrir [http://localhost:8081](http://localhost:8081)

---

## Notes importantes

- Le fichier `dbt/.dbt_profiles/bigqueryKey.json` est **intentionnellement exclu du git** (`.gitignore`). Ne jamais le committer.
- Les DAGs ex1 à ex5 sont en déclenchement **manuel** (`schedule=None`) — ils servent à valider chaque couche séparément.
- `ex6_1_staging` est planifié `@daily` et déclenche en cascade `ex6_2` → `ex6_3` → `ex6_4` via `TriggerDagRunOperator`. Ne pas activer `ex6_1` et `ex6_full_dbt_pipeline` simultanément pour éviter de doubler les exécutions.
- `ex6_full_dbt_pipeline` est planifié `@daily` avec `catchup=False` — il ne rattrapera pas les exécutions passées au premier démarrage.
- Le module `dbt_monitor.py` nécessite que le package `google-cloud-bigquery` soit installé dans l'image Docker. Il est inclus dans `requirements.txt`.
- La variable `SLACK_WEBHOOK_URL` est optionnelle — si absente, les fonctions de notification se terminent silencieusement sans erreur.
