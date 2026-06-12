import os
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def on_failure_slack_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    message = (
        f":red_circle: *Échec dans le pipeline dbt*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Tâche:* `{task_id}`\n"
        f"*Date d'exécution:* {exec_date}\n"
        f"*Logs:* <{log_url}|Voir les logs>"
    )

    hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
    hook.send(text=message)
