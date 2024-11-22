import os
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, DbtDag
# from airflow.providers.tableau.operators.tableau import TableauOperator

from cosmos.profiles import SnowflakeUserPasswordProfileMapping

@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 22),
    catchup=False,
    dag_id="dbt_dag",
    default_args={"owner": "airflow"}
)
def weather_pipeline():
    profile_config = ProfileConfig(
        profile_name='default',
        target_name='dev',
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_con",
            profile_args={"database": "data_analytics", "schema": "transformations"},
        )
    )

    dbt_snowflake_dag = DbtDag(
        project_config=ProjectConfig("/home/olha/olha-de-internship/personal_project/dbt-dag/dbt_weather"),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt-weather"),
    )

    # tableau_refresh = TableauOperator(
    #     task_id=
    #     workbook_name=
    #     site_id=
    #     tableau_conn_id=
    # )

    validate_data = SnowflakeOperator(
        task_id="validate_data",
        sql="SELECT COUNT(*) FROM stg_cities WHERE city_id IS NULL",
        snowflake_conn_id="snowflake_con",
    )

    validate_data >> dbt_snowflake_dag

weather_dag = weather_pipeline()
