import os
import requests


def on_failure_slack_alert(context):
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    logical_date = context["logical_date"]
    log_url = context["task_instance"].log_url

    message = (
        f":red_circle: *Échec dans le pipeline dbt*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Tâche:* `{task_id}`\n"
        f"*Date d'exécution:* {logical_date}\n"
        f"*Logs:* <{log_url}|Voir les logs>"
    )

    requests.post(webhook_url, json={"text": message}, timeout=10)
