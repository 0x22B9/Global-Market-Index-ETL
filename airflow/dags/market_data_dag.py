from __future__ import annotations


import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT_IN_CONTAINER = '/opt/airflow/projects/major_world_indices'
MAIN_SCRIPT_PATH_IN_CONTAINER = f'{PROJECT_ROOT_IN_CONTAINER}/src/main.py'
PYTHON_EXECUTABLE = 'python3'

with DAG(
    dag_id = 'market_indices_pipeline',
    schedule = '0 */6 * * *',
    start_date = pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup = False,
    tags=["market-data", "etl", "docker"],
    doc_md="""
    ### Market Indices Data Pipeline (Docker Version)

    DAG for collecting, processing and loading data on major global market indices, running within Docker.
    - Source: Yahoo Finance (via yfinance)
    - Processing: Standardize columns, UTC timestamp, convert to USD.
    - Storage: Project's PostgreSQL DB (managed via Docker Compose or externally)
    - Frequency: Every 6 hours.
    - Dependencies: Installed via Dockerfile into the Airflow image.
    - Configuration:
        - Project code is mounted into `/opt/airflow/projects/major_world_indices` inside containers.
        - **Crucially:** Expects PostgreSQL credentials for the *project* database to be configured via Airflow Connection `postgres_marketdata`.
    """,
) as dag:
    run_etl_script = BashOperator(
        task_id="run_market_data_etl",
        bash_command=f"cd {PROJECT_ROOT_IN_CONTAINER} && {PYTHON_EXECUTABLE} {MAIN_SCRIPT_PATH_IN_CONTAINER}",
        append_env=True,
    )
