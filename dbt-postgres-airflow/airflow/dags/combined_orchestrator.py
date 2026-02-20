# ---------------------------------------------
# combined_orchestrator.py
# DAG Airflow combinant ingestion API + dbt
# ---------------------------------------------

import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys

# -----------------------------
# Ajouter le dossier utilities pour l'import
# -----------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utilities')))
from helper_functions import main  # ton script d'ingestion

# -----------------------------
# Définir les chemins dbt
# -----------------------------
HOME = os.environ["HOME"]
dbt_path = os.path.join(HOME, "repos/dbt-postgres-airflow/dbt/my_project")
manifest_path = os.path.join(dbt_path, "target/manifest.json")

# -----------------------------
# Lire le manifest dbt
# -----------------------------
with open(manifest_path) as f:
    manifest = json.load(f)
    nodes = manifest["nodes"]

# -----------------------------
# Définir le DAG combiné
# -----------------------------
with DAG(
    dag_id="combined_orchestrator",
    start_date=pendulum.today(),
    schedule="@daily",
    catchup=False
) as dag:

    # -----------------------------
    # 1️⃣ Task : Ingestion des données
    # -----------------------------
    ingest_data = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=main
    )

    # -----------------------------
    # 2️⃣ Tasks : dbt
    # -----------------------------
    dbt_tasks = dict()
    for node_id, node_info in nodes.items():
        if node_info["resource_type"] == "model":
            dbt_tasks[node_id] = BashOperator(
                task_id=".".join([node_info["resource_type"], node_info["package_name"], node_info["name"]]),
                bash_command=f"cd {dbt_path} && /home/calvin/.pyenv/versions/demo_dbt/bin/dbt run --select {node_info['name']}"
            )

    # -----------------------------
    # 3️⃣ Définir les dépendances dbt
    # -----------------------------
    for node_id, node_info in nodes.items():
        upstream_nodes = node_info["depends_on"]["nodes"]
        if upstream_nodes:
            for upstream_node in upstream_nodes:
                if upstream_node in dbt_tasks:
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]

    # -----------------------------
    # 4️⃣ Lier l'ingestion des données au dbt
    # -----------------------------
    for task in dbt_tasks.values():
        ingest_data >> task
