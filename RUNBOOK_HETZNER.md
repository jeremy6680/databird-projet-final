# Runbook — Déploiement Airflow sur Hetzner (k3s + Helm)

Guide de reproduction du déploiement production, étape par étape.

---

## Ce qu'on a mis en place

| Composant | Choix | Pourquoi |
|-----------|-------|----------|
| Kubernetes | k3s | Distribution légère, 1 commande, certifiée CNCF |
| Chart | apache-airflow/airflow 1.22.0 | Chart officiel, gère tous les composants automatiquement |
| Exécuteur | LocalExecutor | Simple, adapté à un cluster mono-nœud |
| Image | jeremy6680/airflow-projet (Docker Hub) | Image custom avec dbt-bigquery + providers + DAGs |
| Serveur | Hetzner CAX (ARM64) — IP : 91.98.66.197 | |

---

## Étape 1 — Installer k3s sur le VPS Hetzner

> Se connecter au serveur via SSH ou le terminal Coolify.

```bash
curl -sfL https://get.k3s.io | sh -
```

Vérifier :
```bash
kubectl get nodes
# → STATUS: Ready
```

---

## Étape 2 — Connecter kubectl (Mac → Hetzner)

### 2a. Récupérer le kubeconfig et l'IP publique du serveur

```bash
cat /etc/rancher/k3s/k3s.yaml   # copier tout le contenu
curl -s -4 ifconfig.me           # noter l'IP publique IPv4
```

### 2b. Créer le fichier kubeconfig sur le Mac

Créer `~/.kube/hetzner.yaml` avec le contenu copié, en faisant deux modifications :
- Remplacer `127.0.0.1` par l'IP publique du serveur
- Remplacer les 5 occurrences de `default` par `hetzner`

### 2c. Fusionner avec la config existante (minikube)

```bash
KUBECONFIG=~/.kube/config:~/.kube/hetzner.yaml kubectl config view --flatten > /tmp/kube_merged \
  && mv /tmp/kube_merged ~/.kube/config && chmod 600 ~/.kube/config
```

### 2d. Vérifier depuis le Mac

```bash
kubectl config use-context hetzner
kubectl get nodes
# → doit afficher le nœud Hetzner
```

> Pour revenir sur minikube : `kubectl config use-context minikube`

---

## Étape 3 — Publier l'image Docker

> Le serveur est ARM64. Un Mac Apple Silicon construit nativement pour ARM64 — pas de cross-compilation nécessaire.

```bash
docker login -u <docker-username>
docker build -t <docker-username>/airflow-projet:latest .
docker push <docker-username>/airflow-projet:latest
```

---

## Étape 4 — Préparer le cluster

```bash
# Namespace Airflow
kubectl create namespace airflow

# Secret BigQuery (monté comme fichier dans les pods)
kubectl create secret generic gcp-credentials \
  --from-file=bigqueryKey.json=./dbt/.dbt_profiles/bigqueryKey.json \
  -n airflow
```

---

## Étape 5 — Configurer Helm

### 5a. Ajouter le dépôt Helm Airflow (une seule fois)

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

### 5b. Créer helm-values.yaml depuis le template

```bash
cp helm-values.yaml.template helm-values.yaml
```

Remplir les 3 clés dans `helm-values.yaml` (voir les commandes de génération dans le template) :
- `fernetKey`
- `webserverSecretKey`

> `helm-values.yaml` est dans `.gitignore` — ne jamais le committer.

---

## Étape 6 — Déployer

```bash
helm upgrade --install airflow apache-airflow/airflow \
  -n airflow \
  -f helm-values.yaml \
  --timeout 10m
```

Surveiller le démarrage :
```bash
kubectl get pods -n airflow -w
```

État final attendu :
```
airflow-api-server-*       1/1   Running    ← UI + API
airflow-scheduler-0        2/2   Running    ← planification + log-groomer
airflow-dag-processor-*    2/2   Running    ← parsing des DAGs
airflow-triggerer-0        2/2   Running    ← opérateurs asynchrones
airflow-postgresql-0       1/1   Running    ← base de données
airflow-statsd-*           1/1   Running    ← métriques
airflow-run-migrations-*   0/1   Completed  ← job one-shot OK
airflow-create-user-*      0/1   Completed  ← job one-shot OK
```

> L'`api-server` peut faire 1 restart au premier démarrage (startup probe trop courte) — c'est normal, il se rétablit seul.

---

## Étape 7 — Accéder à l'interface

```bash
kubectl port-forward svc/airflow-api-server -n airflow 8080:8080
```

Ouvrir http://localhost:8080 — login `admin` / `admin`

> `port-forward` est un tunnel temporaire (debug). Pour un accès public permanent, configurer un Ingress Traefik (k3s l'inclut par défaut).

---

## Commandes du quotidien

```bash
# Voir l'état de tous les pods
kubectl get pods -n airflow

# Logs d'un composant (les 50 dernières lignes)
kubectl logs -n airflow -l component=scheduler --tail=50
kubectl logs -n airflow -l component=dag-processor --tail=50
kubectl logs -n airflow -l component=api-server --tail=50

# Diagnostiquer un pod qui ne démarre pas
kubectl describe pod -n airflow <nom-du-pod>

# Shell interactif dans un pod
kubectl exec -it -n airflow deployment/airflow-dag-processor -- bash

# Voir les releases Helm
helm list -n airflow
```

---

## Mettre à jour après un changement

### Nouveau code (DAGs, dbt, Dockerfile)

```bash
# Rebuilder et pousser l'image
docker build -t <docker-username>/airflow-projet:latest .
docker push <docker-username>/airflow-projet:latest

# Forcer le rechargement de la nouvelle image dans le cluster
kubectl rollout restart deployment -n airflow
kubectl rollout restart statefulset -n airflow
```

### Changement de configuration Helm (helm-values.yaml)

```bash
helm upgrade airflow apache-airflow/airflow \
  -n airflow \
  -f helm-values.yaml \
  --timeout 10m
```

---

## Tout supprimer (reset propre)

```bash
# Supprimer le déploiement Helm (garde le namespace et les secrets)
helm uninstall airflow -n airflow

# Supprimer tout le namespace (données PostgreSQL perdues)
kubectl delete namespace airflow
```
