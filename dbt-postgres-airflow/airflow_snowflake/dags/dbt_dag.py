import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

HOME = os.environ["HOME"]
DBT_PROJECT_DIR = os.path.join(HOME, "repos/dbt-postgres-airflow/dbt/snowflake_data_project")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_workflow',
    default_args=default_args,
    description='Run dbt models using dbt core',
    schedule='@daily',
    catchup=False,
)

# Task pour exécuter dbt run
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=(
        f"cd {DBT_PROJECT_DIR} && "
        f"/home/calvin/.pyenv/versions/demo_dbt/bin/dbt run"
    ),
    dag=dag,
)

# Task pour exécuter dbt test
dbt_test = BashOperator(
    task_id='dbt_test',  # ✅ correction ici
    bash_command=(
        f"cd {DBT_PROJECT_DIR} && "
        f"/home/calvin/.pyenv/versions/demo_dbt/bin/dbt test"
    ),
    dag=dag,
)

# Définir l’ordre d’exécution
dbt_run >> dbt_test
