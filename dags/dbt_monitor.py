import json
import os
import requests
from pathlib import Path

DBT_TARGET_DIR = "/opt/airflow/dbt/target"


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


def capture_run_results(step_name: str, target_dir: str = DBT_TARGET_DIR, **context) -> dict:
    """Lit run_results.json et pousse le résumé dans XCom."""
    raw = load_run_results(target_dir)
    if not raw:
        return {"step": step_name, "error": "run_results.json introuvable"}
    summary = {**parse_run_results(raw), "step": step_name}
    context["ti"].xcom_push(key=f"dbt_summary_{step_name}", value=summary)
    return summary


def send_pipeline_report(**context):
    """Agrège les résultats XCom de chaque étape et envoie un rapport Slack."""
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
        f"Date: `{logical_date.strftime('%Y-%m-%d %H:%M')} UTC` | Durée totale: *{total_elapsed:.1f}s*",
        f"Modèles: *{total_success}/{total_models}* réussis\n",
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
