"""Airflow DAG: refresh the dbt docs site (lineage + data catalog).

Runs `dbt docs generate --static` in an EMR Serverless Spark Connect session
(the same runner used everywhere else), producing a single self-contained
target/static_index.html, then uploads it to the docs S3 bucket as index.html.
CloudFront serves it immediately at the docs URL.

The upload is done by run_dbt.py `--publish-docs` (triggered here via the
PUBLISH_DOCS=1 environment variable read by mwaa_run_dbt.sh). The MWAA execution
role is granted s3:PutObject on the docs bucket in terraform/mwaa.tf.

Schedule: daily. It is also safe to trigger manually ("Trigger DAG") whenever you
want a fresh catalog, e.g. right after a data/model change. `dbt docs generate`
introspects the warehouse, so run this after the tables exist (i.e. after the
dbt_emr_serverless_spark_connect build DAG has run at least once).
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
    dag_id="dbt_docs_refresh",
    description="Generate the dbt docs (lineage + catalog) and publish them to the CloudFront docs site",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "emr-serverless", "spark-connect", "docs"],
) as dag:

    start = EmptyOperator(task_id="start")

    # PUBLISH_DOCS=1 makes mwaa_run_dbt.sh pass --publish-docs to run_dbt.py, which
    # uploads target/static_index.html to s3://<docs_bucket>/index.html after
    # `dbt docs generate --static` succeeds. MINIMAL=1 requests a small Spark
    # footprint (docs generation is metadata-only), so it fits under the app's
    # maximumCapacity even next to a running build session.
    docs_refresh = BashOperator(
        task_id="dbt_docs_generate_publish",
        bash_command=f"PUBLISH_DOCS=1 MINIMAL=1 bash {BOOTSTRAP} docs generate --static",
    )

    end = EmptyOperator(task_id="end")

    start >> docs_refresh >> end
