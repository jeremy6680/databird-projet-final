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
9. [Lancer le pipeline complet](#lancer-le-pipeline-complet)
10. [Documentation dbt](#documentation-dbt)

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
│   ├── ex1_test_dag.py         # Exercice 1 : run/test table par table (staging)
│   ├── ex2_dag.py              # Exercice 2 : run/test couche staging complète
│   ├── ex3_dag.py              # Exercice 3 : run/test couche intermediate
│   ├── ex4_mart_dag.py         # Exercice 4 : run/test couche mart
│   ├── ex5_docs_dag.py         # Exercice 5 : génération de la doc dbt
│   └── ex6_full_pipeline_dag.py # Exercice 6 : pipeline complet (planifié @daily)
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
| `ex6_full_dbt_pipeline` | `@daily` | Pipeline complet staging → intermediate → mart → docs |

### Graphe d'exécution de `ex6_full_dbt_pipeline`

```
run_staging → test_staging → run_intermediate → test_intermediate → run_mart → test_mart → generate_docs
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
- Le DAG ex6 est planifié **quotidiennement** (`@daily`) avec `catchup=False`, ce qui signifie qu'il ne rattrapera pas les exécutions passées au premier démarrage.
