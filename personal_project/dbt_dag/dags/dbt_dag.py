import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
        profile_name='weather_analysis',
        target_name='dev',
        profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_con",
        profile_args={"database": "data_analytics", "schema": "transformations"}
        )
    )


dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt_weather",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 22),
    catchup=False,
    dag_id="dbt_dag",
)