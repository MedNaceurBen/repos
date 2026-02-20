# ---------------------------------------------
# dbt_orchestrator.py
# DAG Airflow pour orchestrer un projet dbt
# Chaque modèle dbt devient une task Airflow
# Les dépendances sont lues depuis le manifest.json
# ---------------------------------------------

import os           # Pour manipuler les chemins et variables d'environnement
import json         # Pour lire le fichier manifest.json
import pendulum     # Pour gérer les dates dans Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator  # Operator pour lancer des commandes bash

# -----------------------------
# 1️⃣ Définir les chemins
# -----------------------------

# HOME = répertoire personnel de l'utilisateur
HOME = os.environ["HOME"]

# Chemin vers le projet dbt
dbt_path = os.path.join(HOME, "repos/dbt-postgres-airflow/dbt/my_project")

# Chemin vers le fichier manifest.json généré par dbt
manifest_path = os.path.join(dbt_path, "target/manifest.json")

# -----------------------------
# 2️⃣ Lire le manifest dbt
# -----------------------------

with open(manifest_path) as f:
    # Charger le JSON dans un dictionnaire Python
    manifest = json.load(f)
    # Extraire uniquement les nodes (modèles, tests, snapshots)
    nodes = manifest["nodes"]

# -----------------------------
# 3️⃣ Définir le DAG Airflow
# -----------------------------

with DAG(
    dag_id="dbt_orchestrator",     # Nom du DAG
    start_date=pendulum.today(),    # Date de début du DAG
    schedule = "@hourly",
    catchup=False,                  # Ne pas exécuter les runs passés
) as dag:

    # Dictionnaire pour stocker toutes les tasks Airflow
    dbt_tasks = dict()

    # -----------------------------
    # 4️⃣ Créer une task par modèle dbt
    # -----------------------------
    for node_id, node_info in nodes.items():
        if node_info["resource_type"] == 'model':
            # BashOperator exécute le modèle dbt
            dbt_tasks[node_id] = BashOperator(
                # task_id unique et lisible : type_projet_nom
                task_id=".".join([
                    node_info["resource_type"],   # model / test / snapshot
                    node_info["package_name"],    # nom du projet dbt
                    node_info["name"],            # nom du modèle
                ]),
                # Commande bash à exécuter pour ce modèle
                bash_command=(
                    f"cd {dbt_path} && "  # Aller dans le projet dbt
                    f"/home/calvin/.pyenv/versions/demo_dbt/bin/dbt run --select {node_info['name']}"  
                    # Lancer uniquement ce modèle
                ),
            )

    # -----------------------------
    # 5️⃣ Définir les dépendances Airflow
    # -----------------------------
    for node_id, node_info in nodes.items():
        # Récupérer les nodes dbt dont ce modèle dépend
        upstream_nodes = node_info["depends_on"]["nodes"]
        if upstream_nodes:
            # Pour chaque dépendance, relier la task upstream à la task courante
            for upstream_node in upstream_nodes:
                if upstream_node in dbt_tasks:
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]  
                    # ">>" définit l'ordre d'exécution dans Airflow

# -----------------------------
# 6️⃣ Permettre le debug CLI
# -----------------------------
if __name__ == "__main__":
    # Permet de lancer le DAG depuis la ligne de commande
    dag.cli()





# ---------------------------------------------
# ✅ Explication globale
#
# manifest.json → contient tous les modèles dbt et leurs dépendances
# nodes → dictionnaire avec chaque modèle (node) et ses infos
# dbt_tasks → dictionnaire avec chaque task Airflow correspondante
# task_id → identifiant unique pour chaque task, combinant type+package+nom
# upstream_nodes → liste des modèles dbt à exécuter avant la task courante
# >> → Airflow relie les tasks selon les dépendances dbt
# --select model → exécute seulement un modèle à la fois pour respecter le DAG
#
# 💡 Astuce mentale :
# dbt_tasks = toutes les tâches Airflow
# upstream_nodes = qui doit passer avant qui
#
# Résultat → DAG Airflow reproduit exactement le DAG dbt, task par task,
# avec ordre et parallélisme respectés
# ---------------------------------------------
