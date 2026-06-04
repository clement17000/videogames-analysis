"""
dags/elt_rawg_api.py

DAG Airflow — Pipeline ELT pour l'API RAWG Video Games.
Orchestration : extraction dlt (load vers Snowflake) → transformations dbt.
Déclenchement : manuel (schedule=None) — pas de planification automatique.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args = {
    "owner": "clement",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda context: print(
        f"Task {context['task_instance'].task_id} failed in DAG {context['dag'].dag_id}"
    ),
}


@dag(
    dag_id="elt_rawg_api_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rawg", "api", "elt", "dlt", "dbt"],
)
def elt_rawg_api_dag():
    @task
    def trigger_dlt_rawg():
        """Déclenche l'extraction dlt pour l'API RAWG."""
        from dlt_pipelines.rawg_pipeline import run_pipeline

        run_pipeline()

    @task.bash
    def run_dbt():
        """Exécute et teste tout le graphe RAWG : staging + facts."""
        dbt_dir = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_videogames")
        return f"cd {dbt_dir} && dbt build --select +fct_rawg_games --profiles-dir ."

    trigger_dlt_rawg() >> run_dbt()


elt_rawg_api_dag()
