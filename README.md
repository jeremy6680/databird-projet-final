# Projet Final — Pipeline dbt orchestré avec Airflow & BigQuery

Pipeline de données complet qui transforme des données brutes d'une base de vélos en tables analytiques, en passant par 3 couches dbt (staging → intermediate → mart), le tout orchestré par Apache Airflow en multi-conteneurs et stocké dans Google BigQuery.

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
- **Infrastructure** : architecture multi-conteneurs Docker Compose (6 services)

---

## Architecture

### Pipeline de données

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

### Infrastructure Docker Compose (Airflow 3.x)

```
┌────────────────────────────────────────────────────────┐
│  docker-compose.yml                                    │
│                                                        │
│  postgres          ← base de données Airflow           │
│  airflow-init      ← setup unique (migration DB, user) │
│  airflow-webserver ← UI React + API REST (port 8080)   │
│  airflow-scheduler ← planifie et exécute les tâches    │
│  airflow-triggerer ← gère les opérateurs asynchrones   │
│  airflow-dag-processor ← parse les fichiers DAG        │
└────────────────────────────────────────────────────────┘
```

> **Airflow 3.x** introduit le `dag-processor` comme service indépendant (séparé du scheduler) et remplace `airflow webserver` par `airflow api-server`.

Chaque étape est orchestrée par un DAG Airflow qui exécute `dbt run` puis `dbt test` avant de passer à la couche suivante.

---

## Prérequis

- [Docker Desktop](https://www.docker.com/get-started) installé et en cours d'exécution
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

Le fichier `.env` à la racine doit contenir :

```env
AIRFLOW_UID=501          # UID local (vérifier avec : id -u)
AIRFLOW_GID=0

# Clé de chiffrement des connexions Airflow (ne pas changer après le premier démarrage)
AIRFLOW__CORE__FERNET_KEY=<générer avec : python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">

# Clé JWT partagée entre le scheduler et l'api-server
# DOIT être identique sur tous les containers
AIRFLOW__API_AUTH__JWT_SECRET=<générer avec : python3 -c "import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).rstrip(b'=').decode())">

# Slack (optionnel)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz

# SMTP (optionnel)
AIRFLOW__SMTP__SMTP_HOST=...
```

> **Important** : La `FERNET_KEY` et le `JWT_SECRET` doivent être générés **une seule fois** et rester stables. Les changer invalide les connexions chiffrées et les tokens en cours.

### 4. Construire les images et démarrer

```bash
docker compose up --build -d
```

Cette commande :
1. Construit l'image custom (Airflow 3.1.7 + dbt-bigquery + providers)
2. Démarre PostgreSQL et attend qu'il soit prêt
3. Lance `airflow-init` (migration DB + création de l'utilisateur admin)
4. Démarre le webserver, scheduler, triggerer et dag-processor

Le premier démarrage prend environ 2-3 minutes.

### 5. Accéder à l'interface Airflow

Ouvrir [http://localhost:8080](http://localhost:8080)

```
Identifiant : admin
Mot de passe : admin
```

### 6. Commandes utiles

```bash
# Voir l'état de tous les services
docker compose ps

# Voir les logs d'un service
docker compose logs airflow-scheduler
docker compose logs airflow-dag-processor

# Arrêter tous les services (données conservées)
docker compose down

# Arrêter et supprimer la base de données PostgreSQL
docker compose down -v

# Redémarrer après une modification du .env ou du Dockerfile
docker compose up --build -d
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
├── docker-compose.yml          # Orchestration multi-conteneurs (6 services)
├── requirements.txt            # Providers Airflow supplémentaires
├── .env                        # Variables d'environnement (secrets, UID, SMTP)
├── logs/                       # Logs Airflow (montés depuis les containers)
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
| `ex6_4_docs` | Déclenché par ex6_3 | Génération de la documentation dbt + rapport Slack final |
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

Chaque DAG embarque le même pattern de monitoring que `ex6_full_dbt_pipeline` :

```
# ex6_1_staging
run_staging → monitor_staging → test_staging → trigger_intermediate

# ex6_2_intermediate
run_intermediate → monitor_intermediate → test_intermediate → trigger_mart

# ex6_3_mart
run_mart → monitor_mart → test_mart → trigger_docs

# ex6_4_docs
generate_docs → report_pipeline
```

> `monitor_*` insère les métriques dbt dans BigQuery (`airflow_monitoring.dbt_run_metrics`).
> `report_pipeline` envoie le rapport Slack final (`trigger_rule='all_done'`).
> **Limite** : le `trigger_rule='all_done'` de `report_pipeline` ne couvre que les échecs internes à `ex6_4_docs` — les erreurs des DAGs précédents s'exécutent dans des DAG Runs séparés et ne sont pas visibles depuis `ex6_4_docs`.

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

Deux types de notifications Slack sont envoyés si la variable `SLACK_WEBHOOK_URL` est définie dans `.env` :

- **Alerte d'échec** (`slack_callbacks.py`) : envoyée sur chaque tâche en erreur, avec le lien vers les logs Airflow.
- **Rapport de fin de pipeline** (`send_pipeline_report` dans `dbt_monitor.py`) : envoyé à la fin de `ex6_full_dbt_pipeline` et de `ex6_4_docs`, résumant le statut, la durée totale et les métriques par couche.

---

## Lancer le pipeline complet

### Via l'interface Airflow (recommandé)

1. Ouvrir [http://localhost:8080](http://localhost:8080) (`admin` / `admin`)
2. Activer le DAG `ex6_full_dbt_pipeline`
3. Cliquer sur **Trigger DAG** pour une exécution immédiate

### En ligne de commande depuis un conteneur

```bash
# Entrer dans le scheduler (qui exécute les tâches)
docker compose exec airflow-scheduler bash

# Lancer manuellement le pipeline complet
airflow dags trigger ex6_full_dbt_pipeline

# Ou lancer dbt directement
cd /opt/airflow/dbt
dbt run --profiles-dir ./.dbt_profiles
dbt test --profiles-dir ./.dbt_profiles
```

---

## Documentation dbt

Pour consulter la documentation interactive des modèles :

1. Déclencher le DAG `ex5_dbt_docs_pipeline` (ou `ex6_full_dbt_pipeline`)
2. Depuis le scheduler, servir la doc :

```bash
docker compose exec airflow-scheduler bash
cd /opt/airflow/dbt
dbt docs serve --profiles-dir ./.dbt_profiles --port 8081
```

3. Ouvrir [http://localhost:8081](http://localhost:8081)

> Note : exposer le port 8081 depuis le scheduler nécessite d'ajouter `- "8081:8081"` sous `ports:` du service `airflow-scheduler` dans `docker-compose.yml`.

---

## Notes importantes

- Le fichier `dbt/.dbt_profiles/bigqueryKey.json` est **intentionnellement exclu du git** (`.gitignore`). Ne jamais le committer.
- Les DAGs ex1 à ex5 sont en déclenchement **manuel** (`schedule=None`) — ils servent à valider chaque couche séparément.
- `ex6_1_staging` est planifié `@daily` et déclenche en cascade `ex6_2` → `ex6_3` → `ex6_4` via `TriggerDagRunOperator`. Ne pas activer `ex6_1` et `ex6_full_dbt_pipeline` simultanément pour éviter de doubler les exécutions.
- `ex6_full_dbt_pipeline` est planifié `@daily` avec `catchup=False` — il ne rattrapera pas les exécutions passées au premier démarrage.
- La variable `SLACK_WEBHOOK_URL` est optionnelle — si absente, les fonctions de notification se terminent silencieusement sans erreur.

### Spécificités Airflow 3.x

| Changement | Airflow 2.x | Airflow 3.x |
|---|---|---|
| Commande UI | `airflow webserver` | `airflow api-server` |
| Parse des DAGs | Intégré au scheduler | Service `dag-processor` séparé |
| Auth utilisateurs | `airflow users create` (core) | Provider `apache-airflow-providers-fab` |
| Exécution des tâches | Directe | Via Execution API (JWT) |
| JWT signing key | Non requis | `AIRFLOW__API_AUTH__JWT_SECRET` (partagé entre containers) |
| Execution API URL | Non requis | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` |
