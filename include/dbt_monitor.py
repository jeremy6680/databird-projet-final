import datetime
import json
import os
import requests
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

DBT_TARGET_DIR = "/opt/airflow/dbt/target"
KEYFILE_PATH = "/opt/airflow/dbt/.dbt_profiles/bigqueryKey.json"
BQ_PROJECT = "databird-prep-work-ae"
BQ_DATASET = "airflow_monitoring"
BQ_TABLE = "dbt_run_metrics"


# ---------------------------------------------------------------------------
# dbt artifact parsing
# ---------------------------------------------------------------------------

def load_run_results(target_dir: str = DBT_TARGET_DIR) -> dict:
    path = Path(target_dir) / "run_results.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def parse_run_results(run_results: dict) -> dict:
    results = run_results.get("results", [])
    metadata = run_results.get("metadata", {})

    models = []
    for r in results:
        unique_id = r.get("unique_id", "")
        models.append({
            "name": unique_id.split(".")[-1],
            "unique_id": unique_id,
            "status": r.get("status"),
            "execution_time": round(r.get("execution_time", 0), 2),
            "rows_affected": r.get("adapter_response", {}).get("rows_affected"),
            "message": r.get("message", ""),
            "failures": r.get("failures"),
        })

    return {
        "generated_at": metadata.get("generated_at"),
        "dbt_version": metadata.get("dbt_version"),
        "elapsed_time": round(run_results.get("elapsed_time", 0), 2),
        "total": len(results),
        "success": sum(1 for r in results if r["status"] in ("success", "pass")),
        "error": sum(1 for r in results if r["status"] == "error"),
        "warn": sum(1 for r in results if r["status"] == "warn"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
        "models": sorted(models, key=lambda x: x["execution_time"], reverse=True),
    }


# ---------------------------------------------------------------------------
# BigQuery persistence
# ---------------------------------------------------------------------------

def _get_bq_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(KEYFILE_PATH)
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


def _ensure_table_exists(client: bigquery.Client) -> str:
    """Cree le dataset et la table si inexistants. Retourne le table ref complet."""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        client.get_dataset(BQ_DATASET)
    except Exception:
        dataset = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)

    schema = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dag_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("logical_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("step", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("model_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("model_unique_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("execution_time_seconds", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("rows_affected", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dbt_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("generated_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    # Partitionnement par jour sur logical_date pour limiter les couts de scan
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="logical_date",
    )
    client.create_table(table, exists_ok=True)
    return table_id


def write_metrics_to_bigquery(summary: dict, run_id: str, dag_id: str, logical_date) -> None:
    """Insere les metriques par modele dans BigQuery."""
    client = _get_bq_client()
    table_id = _ensure_table_exists(client)

    inserted_at = datetime.datetime.utcnow().isoformat()
    rows = [
        {
            "run_id": run_id,
            "dag_id": dag_id,
            "logical_date": logical_date.isoformat(),
            "step": summary["step"],
            "model_name": model["name"],
            "model_unique_id": model["unique_id"],
            "status": model["status"],
            "execution_time_seconds": model["execution_time"],
            "rows_affected": model.get("rows_affected"),
            "message": (model.get("message") or "")[:500],
            "dbt_version": summary.get("dbt_version"),
            "generated_at": summary.get("generated_at"),
            "inserted_at": inserted_at,
        }
        for model in summary.get("models", [])
    ]

    if not rows:
        return

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


# ---------------------------------------------------------------------------
# Airflow task callables
# ---------------------------------------------------------------------------

def capture_run_results(step_name: str, target_dir: str = DBT_TARGET_DIR, **context) -> dict:
    """Lit run_results.json, persiste dans BigQuery et pousse le resume en XCom."""
    raw = load_run_results(target_dir)
    if not raw:
        return {"step": step_name, "error": "run_results.json introuvable"}

    summary = {**parse_run_results(raw), "step": step_name}

    write_metrics_to_bigquery(
        summary=summary,
        run_id=context["run_id"],
        dag_id=context["dag"].dag_id,
        logical_date=context["logical_date"],
    )

    context["ti"].xcom_push(key=f"dbt_summary_{step_name}", value=summary)
    return summary


def send_pipeline_report(**context):
    """Agregre les resultats XCom de chaque etape et envoie un rapport Slack."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    ti = context["ti"]
    dag_id = context["dag"].dag_id
    logical_date = context["logical_date"]

    steps = ["staging", "intermediate", "mart"]
    summaries = [
        s for s in (
            ti.xcom_pull(key=f"dbt_summary_{step}", task_ids=f"monitor_{step}")
            for step in steps
        )
        if s
    ]

    total_elapsed = sum(s.get("elapsed_time", 0) for s in summaries)
    total_errors = sum(s.get("error", 0) for s in summaries)
    total_models = sum(s.get("total", 0) for s in summaries)
    total_success = sum(s.get("success", 0) for s in summaries)

    status_icon = ":white_check_mark:" if total_errors == 0 else ":warning:"
    lines = [
        f"{status_icon} *Rapport pipeline dbt — {dag_id}*",
        f"Date: `{logical_date.strftime('%Y-%m-%d %H:%M')} UTC` | Duree totale: *{total_elapsed:.1f}s*",
        f"Modeles: *{total_success}/{total_models}* reussis\n",
    ]

    for summary in summaries:
        step = summary.get("step", "?").capitalize()
        icon = ":white_check_mark:" if summary["error"] == 0 else ":red_circle:"
        lines.append(f"{icon} *{step}* — {summary['elapsed_time']}s")

        for model in summary.get("models", []):
            status_emoji = ":x:" if model["status"] == "error" else ":clock1:"
            rows = f", {model['rows_affected']} lignes" if model.get("rows_affected") else ""
            lines.append(
                f"  {status_emoji} `{model['name']}` — {model['execution_time']}s{rows}"
            )

    requests.post(webhook_url, json={"text": "\n".join(lines)}, timeout=10)


def send_pipeline_report_from_bq(**context):
    """Pour les DAGs chainés ex6_1→ex6_4 : lit les métriques depuis BigQuery car XCom n'est pas accessible cross-DAG."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    from collections import defaultdict

    dag_id = context["dag"].dag_id
    logical_date = context["logical_date"]

    client = _get_bq_client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    query = f"""
        WITH latest_runs AS (
            SELECT step, MAX(inserted_at) AS latest_insert
            FROM `{table_id}`
            WHERE step IN ('staging', 'intermediate', 'mart')
              AND inserted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY step
        )
        SELECT t.step, t.model_name, t.status, t.execution_time_seconds, t.rows_affected
        FROM `{table_id}` t
        JOIN latest_runs lr ON t.step = lr.step AND t.inserted_at = lr.latest_insert
        ORDER BY t.step, t.execution_time_seconds DESC
    """
    rows = list(client.query(query).result())

    steps_models = defaultdict(list)
    for row in rows:
        steps_models[row.step].append(row)

    summaries = []
    for step in ["staging", "intermediate", "mart"]:
        models = steps_models.get(step, [])
        if not models:
            continue
        success_count = sum(1 for m in models if m.status in ("success", "pass"))
        error_count = sum(1 for m in models if m.status == "error")
        elapsed = round(sum(m.execution_time_seconds or 0 for m in models), 2)
        summaries.append({
            "step": step,
            "total": len(models),
            "success": success_count,
            "error": error_count,
            "elapsed_time": elapsed,
            "models": [
                {
                    "name": m.model_name,
                    "status": m.status,
                    "execution_time": round(m.execution_time_seconds or 0, 2),
                    "rows_affected": m.rows_affected,
                }
                for m in models
            ],
        })

    total_elapsed = sum(s["elapsed_time"] for s in summaries)
    total_models = sum(s["total"] for s in summaries)
    total_success = sum(s["success"] for s in summaries)
    total_errors = sum(s["error"] for s in summaries)

    status_icon = ":white_check_mark:" if total_errors == 0 else ":warning:"
    lines = [
        f"{status_icon} *Rapport pipeline dbt — {dag_id}*",
        f"Date: `{logical_date.strftime('%Y-%m-%d %H:%M')} UTC` | Duree totale: *{total_elapsed:.1f}s*",
        f"Modeles: *{total_success}/{total_models}* reussis\n",
    ]

    for summary in summaries:
        step = summary["step"].capitalize()
        icon = ":white_check_mark:" if summary["error"] == 0 else ":red_circle:"
        lines.append(f"{icon} *{step}* — {summary['elapsed_time']}s")
        for model in summary["models"]:
            status_emoji = ":x:" if model["status"] == "error" else ":clock1:"
            rows_str = f", {model['rows_affected']} lignes" if model.get("rows_affected") else ""
            lines.append(f"  {status_emoji} `{model['name']}` — {model['execution_time']}s{rows_str}")

    requests.post(webhook_url, json={"text": "\n".join(lines)}, timeout=10)
