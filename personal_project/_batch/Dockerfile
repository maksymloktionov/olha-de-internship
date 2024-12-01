FROM quay.io/astronomer/astro-runtime:12.4.0

WORKDIR "/usr/local/airflow"

RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate
