"""Daily ingestion of all OLTP Postgres tables into the S3 Delta lake.

One task per source, generated from config/sources.yml — so adding a source
to the YAML automatically adds a task here (config-driven orchestration).
"""
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator

# Read the source registry at DAG-parse time -> one task per source.
with open("/opt/airflow/config/sources.yml") as f:
    SOURCES = yaml.safe_load(f)["sources"]

with DAG(
    dag_id="oltp_ingestion",
    description="Ingest OLTP Postgres tables into the S3 Delta lake",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",          # run once a day
    catchup=False,              # don't backfill old dates
    max_active_tasks=1,         # ONE task at a time -> RAM safety on this laptop
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ingestion", "phase1"],
) as dag:
    for s in SOURCES:
        BashOperator(
            task_id=f"ingest_{s['entity']}",
            bash_command=(
                "cd /opt/airflow && "
                f"python -m src.ingestion.run_ingestion --source {s['source_id']}"
            ),
        )
