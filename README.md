# Projet Final — Pipeline dbt orchestré avec Airflow & BigQuery

Pipeline de données complet qui transforme des données brutes d'une base de vélos en tables analytiques, en passant par 3 couches dbt (staging → intermediate → mart), le tout orchestré par Apache Airflow en multi-conteneurs et stocké dans Google BigQuery.

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Prérequis](#prérequis)
4. [Installation et démarrage — Docker Compose](#installation-et-démarrage--docker-compose)
5. [Déploiement Kubernetes](#déploiement-kubernetes)
   - [5a. Local — minikube (développement)](#5a-local--minikube-développement)
   - [5b. Production — Hetzner + Helm](#5b-production--hetzner--helm)
6. [Configuration BigQuery](#configuration-bigquery)
7. [Structure du projet](#structure-du-projet)
8. [Les DAGs Airflow](#les-dags-airflow)
9. [Les modèles dbt](#les-modèles-dbt)
10. [Monitoring & Reporting](#monitoring--reporting)
11. [Lancer le pipeline complet](#lancer-le-pipeline-complet)
12. [Documentation dbt](#documentation-dbt)

---

## Vue d'ensemble

Ce projet implémente un pipeline ELT (Extract → Load → **Transform**) :

- **Source** : tables brutes d'une base de données vélos (`bike_database`) déjà chargées dans BigQuery
- **Transformation** : dbt s'occupe de nettoyer, joindre et agréger les données en 3 couches
- **Orchestration** : Apache Airflow planifie et enchaîne les étapes
- **Infrastructure** : architecture multi-conteneurs Docker Compose (6 services)

> **Périmètre de l'exercice** : L'objectif principal était la **création de la pipeline et l'orchestration Airflow**. Les modèles dbt (couches staging, intermediate et mart) sont issus du dépôt de correction [`Carolinemestre/correction_projet_final`](https://github.com/Carolinemestre/correction_projet_final) et ont été intégrés tels quels via `git clone`. Le travail réalisé ici porte sur les DAGs Airflow, l'infrastructure Docker/Kubernetes, le module de monitoring et les alertes Slack.

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

### Infrastructure — Docker Compose (Airflow 3.x)

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

### Infrastructure — Kubernetes local (minikube)

Manifests manuels dans `k8s/`, pour le développement et les tests.

```
namespace: airflow
├── ConfigMap: airflow-config          ← variables d'env non-sensibles
├── Secret: airflow-secrets            ← Fernet key, JWT, Slack, SMTP
├── PVC: postgres-pvc (5Gi)            ← données PostgreSQL persistées
├── Deployment/Service: postgres       ← base de données Airflow
├── Job: airflow-init                  ← migration DB + création admin (1 fois)
├── Deployment/Service: airflow-webserver   ← UI React + API REST
├── Deployment: airflow-scheduler      ← planifie et exécute les tâches
├── Deployment: airflow-dag-processor  ← parse les fichiers DAG
└── Deployment: airflow-triggerer      ← opérateurs asynchrones
```

### Infrastructure — Kubernetes production (Hetzner + Helm)

Déployé via le chart officiel `apache-airflow/airflow` sur un VPS Hetzner ARM64 (k3s).

```
VPS Hetzner ARM64 — k3s v1.35 — IP: 91.98.66.197
└── namespace: airflow  (Chart: apache-airflow/airflow 1.22.0)
    ├── Secret: airflow-fernet-key         ← Fernet key (chiffrement connexions)
    ├── Secret: airflow-jwt-secret         ← JWT (auth inter-composants Airflow 3.x)
    ├── Secret: airflow-api-secret-key     ← clé session Flask
    ├── Secret: gcp-credentials            ← bigqueryKey.json (monté dans les pods)
    ├── StatefulSet: airflow-postgresql    ← PostgreSQL avec PVC auto-provisionné
    ├── Job: airflow-run-migrations        ← migrations DB (one-shot)
    ├── Job: airflow-create-user           ← création admin (one-shot)
    ├── Deployment: airflow-api-server     ← UI React + API REST (port 8080)
    ├── StatefulSet: airflow-scheduler     ← planification + log-groomer (2 conteneurs)
    ├── Deployment: airflow-dag-processor  ← parse les fichiers DAG (2 conteneurs)
    ├── StatefulSet: airflow-triggerer     ← opérateurs asynchrones (2 conteneurs)
    └── Deployment: airflow-statsd         ← collecte de métriques
```

> Les packages dbt sont pré-installés dans l'image Docker (`dbt deps` au build) plutôt qu'au démarrage, conformément au principe d'**image immuable** en K8s.

Chaque étape est orchestrée par un DAG Airflow qui exécute `dbt run` puis `dbt test` avant de passer à la couche suivante.

---

## Prérequis

- [Docker Desktop](https://www.docker.com/get-started) installé et en cours d'exécution
- Un projet Google Cloud avec BigQuery activé
- Un compte de service GCP avec les droits BigQuery (`roles/bigquery.dataEditor` + `roles/bigquery.jobUser`)
- La clé JSON du compte de service
- *(Kubernetes uniquement)* [kubectl](https://kubernetes.io/docs/tasks/tools/) et [minikube](https://minikube.sigs.k8s.io/docs/start/) installés

---

## Installation et démarrage — Docker Compose

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

## Déploiement Kubernetes

Deux environnements Kubernetes sont disponibles : local avec minikube (développement/tests) et production sur un VPS Hetzner avec Helm.

Les deux coexistent grâce aux **contextes kubectl** — on bascule entre eux sans aucun conflit :
```bash
kubectl config use-context minikube   # ← local
kubectl config use-context hetzner    # ← production
kubectl config current-context        # ← voir lequel est actif
```

---

### 5a. Local — minikube (développement)

Déploiement sur Kubernetes local via les manifests manuels dans `k8s/`. Idéal pour tester sans toucher à la prod.

#### Prérequis

```bash
brew install kubectl minikube
minikube start --driver=docker
```

#### 1. Construire l'image dans minikube

Minikube a son propre daemon Docker interne. L'image doit y être construite pour être accessible aux pods.

```bash
# Brancher le terminal sur le Docker de minikube (valable pour ce terminal uniquement)
eval $(minikube docker-env)

# Construire l'image
docker build -t airflow-projet:latest .
```

#### 2. Configurer les secrets

Copier le template et remplir les valeurs :

```bash
cp k8s/02-secret.yaml.template k8s/02-secret.yaml
```

Editer `k8s/02-secret.yaml` avec les vraies valeurs (Fernet key, JWT secret, Slack webhook, SMTP password).

> `k8s/02-secret.yaml` est dans `.gitignore` — ne jamais le committer.

#### 3. Déployer dans l'ordre

```bash
# Namespace + configuration
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-configmap.yaml
kubectl apply -f k8s/02-secret.yaml

# Base de données
kubectl apply -f k8s/03-postgres.yaml
kubectl wait --for=condition=ready pod -l app=postgres -n airflow --timeout=60s

# Initialisation Airflow (migration DB + création utilisateur admin)
kubectl apply -f k8s/04-airflow-init-job.yaml
kubectl wait --for=condition=complete job/airflow-init -n airflow --timeout=120s

# Services Airflow
kubectl apply -f k8s/05-airflow-webserver.yaml
kubectl apply -f k8s/06-airflow-scheduler.yaml
kubectl apply -f k8s/07-airflow-dag-processor.yaml
kubectl apply -f k8s/08-airflow-triggerer.yaml
```

#### 4. Accéder à l'interface

```bash
kubectl port-forward svc/airflow-webserver-svc 8080:8080 -n airflow
```

Ouvrir [http://localhost:8080](http://localhost:8080) — login `admin` / `admin`.

#### Commandes utiles (minikube)

```bash
kubectl get pods -n airflow
kubectl logs -n airflow deployment/airflow-scheduler
kubectl describe pod -n airflow <nom-du-pod>
kubectl exec -it -n airflow deployment/airflow-scheduler -- bash
minikube stop && minikube start
```

---

### 5b. Production — Hetzner + Helm

Le même projet est aussi déployé en production sur un VPS Hetzner Cloud (ARM64) sous **k3s**, via le chart Helm officiel `apache-airflow/airflow` (v1.22.0, Airflow 3.2.2). Image custom poussée sur Docker Hub, secrets gérés via `kubectl create secret` + `helm-values.yaml` (non versionné).

Le runbook complet (installation k3s, configuration kubeconfig, secrets, commandes Helm, troubleshooting) est gardé dans une doc interne plutôt que dans ce README — c'est un guide opérationnel, pas une présentation du projet.

> Bascule entre environnements : `kubectl config use-context hetzner` / `minikube`.

---

### Différences Docker Compose → Kubernetes (Helm)

| Docker Compose | Kubernetes manuel (`k8s/`) | Helm (`helm-values.yaml`) |
|---|---|---|
| `environment:` / `env_file:` | `ConfigMap` + `Secret` | Géré automatiquement par le chart |
| `volumes: ./dags:/...` | `COPY` dans le Dockerfile | `COPY` dans le Dockerfile |
| `volumes: postgres-db-volume:` | `PersistentVolumeClaim` | PVC créé automatiquement par le chart |
| `ports: "8080:8080"` | `Service ClusterIP` + `port-forward` | `Service ClusterIP` + `port-forward` / Ingress |
| `depends_on:` | `initContainers` | `initContainers` gérés par le chart |
| `restart: always` | `restartPolicy: Always` | Géré par le chart + liveness probes |
| Service `airflow-init` | `Job` Kubernetes | Jobs `run-migrations` + `create-user` |
| — | ~10 fichiers YAML à maintenir | 1 fichier `helm-values.yaml` |

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
├── Dockerfile                  # Image Airflow + dbt-bigquery (dbt deps pré-installé)
├── docker-compose.yml          # Orchestration multi-conteneurs (6 services)
├── requirements.txt            # Providers Airflow supplémentaires
├── .env                        # Variables d'environnement (secrets, UID, SMTP)
├── logs/                       # Logs Airflow (montés depuis les containers)
│
├── include/                    # Utilitaires partagés entre DAGs (sur PYTHONPATH)
│   ├── slack_callbacks.py      # Callback d'alerte Slack sur échec de tâche
│   └── dbt_monitor.py          # Monitoring : parsing artefacts dbt + écriture BigQuery
│
├── k8s/                        # Manifests Kubernetes
│   ├── 00-namespace.yaml       # Namespace "airflow"
│   ├── 01-configmap.yaml       # Variables d'env non-sensibles
│   ├── 02-secret.yaml          # ⚠️ À créer depuis le template (non versionné)
│   ├── 02-secret.yaml.template # Template des secrets (valeurs vides)
│   ├── 03-postgres.yaml        # PVC + Deployment + Service PostgreSQL
│   ├── 04-airflow-init-job.yaml     # Job d'initialisation (1 fois)
│   ├── 05-airflow-webserver.yaml    # Deployment + Service webserver
│   ├── 06-airflow-scheduler.yaml    # Deployment scheduler
│   ├── 07-airflow-dag-processor.yaml # Deployment dag-processor
│   └── 08-airflow-triggerer.yaml    # Deployment triggerer
│
├── dags/                       # DAGs Airflow uniquement (montés en volume)
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

### DAGs chaînés ex6_1 → ex6_4

`ex6_full_dbt_pipeline` exécute tout le pipeline dans un seul DAG monolithique. `ex6_1` → `ex6_4` découpent ce même pipeline en 4 DAGs indépendants enchaînés via `TriggerDagRunOperator`, chacun pouvant être déclenché ou rejoué séparément :

```
ex6_1_staging ──trigger──▶ ex6_2_intermediate ──trigger──▶ ex6_3_mart ──trigger──▶ ex6_4_docs
```

Chaque DAG suit le même pattern run → monitor → test :

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

Issus du dépôt de correction (voir [Vue d'ensemble](#vue-densemble)), organisés en 3 couches :

| Couche | Préfixe | Matérialisation | Rôle | Nb modèles |
|--------|---------|------------------|------|-----------|
| Staging | `stg_bike_database__*` | Vue | Renommage, cast, nettoyage des 9 tables sources brutes | 9 |
| Intermediate | `int_bike_database__*` | Vue | Jointures et calculs métier (commandes, produits, stocks) | 4 |
| Mart | `mrt_operations__*` | Table | Tables analytiques finales (résumé client, performance produit/magasin) | 4 |

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

- **Alerte d'échec** (`include/slack_callbacks.py`) : envoyée sur chaque tâche en erreur, avec le lien vers les logs Airflow.
- **Rapport de fin de pipeline** (`send_pipeline_report` dans `include/dbt_monitor.py`) : envoyé à la fin de `ex6_full_dbt_pipeline` et de `ex6_4_docs`, résumant le statut, la durée totale et les métriques par couche.

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
