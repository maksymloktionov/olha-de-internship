from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

DBT_VENV_PATH = "/usr/local/airflow/dbt_venv/bin/activate"
DBT_PROJECT_DIR = "/usr/local/airflow/dbt/_dbt_weather"

with DAG(
    "dbt_dag",
    start_date=datetime(2024, 12, 1),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"source {DBT_VENV_PATH} && "
                     f"dbt deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"source {DBT_VENV_PATH} && "
                     f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"source {DBT_VENV_PATH} && "
                     f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_deps >> dbt_run >> dbt_test
