"""Airflow DAG: run dbt on Amazon EMR Serverless via Spark Connect.

Runs the same runner/run_dbt.py used locally. Each task shells out to
mwaa_run_dbt.sh, which installs dbt (from the packaged, non-public dbt-spark
wheel) into an isolated venv, opens an EMR Serverless Spark Connect session,
and runs dbt against the S3 Tables Iceberg catalog.

`dbt build` runs seeds, models and tests in a single Spark Connect session, so we
model the pipeline as one build task bracketed by markers. Split into separate
seed/run/test tasks only if you want them isolated (each would open its own
session).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAGS_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), "dags")
BOOTSTRAP = os.path.join(DAGS_DIR, "mwaa_run_dbt.sh")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_emr_serverless_spark_connect",
    description="Transform S3 Tables data with dbt on EMR Serverless (Spark Connect)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "emr-serverless", "spark-connect", "s3-tables"],
) as dag:

    start = EmptyOperator(task_id="start")

    # `dbt build` = seed (load raw_* into S3 Tables) + run (staging + marts) + test,
    # all inside a single EMR Serverless Spark Connect session.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"bash {BOOTSTRAP} build ",
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_build >> end
