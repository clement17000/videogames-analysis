"""
dags/elt_vgsales.py

DAG Airflow — Pipeline ELT pour le dataset VGSales (Kaggle CSV).
Orchestration : extraction dlt (load vers Snowflake) → transformations dbt.
Déclenchement : manuel (schedule=None) — à lancer ponctuellement, le CSV est stable.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args = {
    "owner": "clement",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda context: print(
        f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}"
    ),
}


@dag(
    dag_id="elt_vgsales_csv_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vgsales", "elt", "dlt", "dbt"],
)
def elt_vgsales_csv_dag():
    @task
    def trigger_dlt_pipeline():
        """Déclenche l'extraction et le chargement dlt pour le CSV vgsales."""
        from dlt_pipelines.vgsales_pipeline import run_pipeline

        run_pipeline()

    @task.bash
    def run_dbt():
        """Exécute et teste tout le graphe vgsales : staging + dimensions + facts."""
        dbt_dir = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_videogames")
        return f"cd {dbt_dir} && dbt build --select +fct_vgsales --profiles-dir ."

    trigger_dlt_pipeline() >> run_dbt()


elt_vgsales_csv_dag()
