#!/usr/bin/env python3
"""Run dbt against Amazon EMR Serverless over Spark Connect.

Usage (local, via uv):

    uv run python run_dbt.py                       # dbt build (seed + run + test)
    uv run python run_dbt.py -- seed
    uv run python run_dbt.py -- run --select fct_orders
    uv run python run_dbt.py --no-terminate -- run  # keep the session for debugging

The script:
  * reads generated/emr_config.json (produced by Terraform),
  * starts an EMR Serverless Spark Connect session with the S3 Tables catalog
    configuration,
  * exports SPARK_REMOTE (Spark Connect URL + x-aws-proxy-auth token) and DBT_SPARK_SCHEMA,
  * invokes the dbt CLI,
  * terminates the session afterwards (unless --no-terminate).

The SAME script runs under MWAA (see airflow/dags/mwaa_run_dbt.sh).

Note: under MWAA this file lives at dags/runner/run_dbt.py. Airflow's DAG
processor is told to skip it (and the dbt project) via dags/.airflowignore,
because this is a CLI tool invoked by mwaa_run_dbt.sh, not an Airflow DAG.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from emr_serverless_session import EmrServerlessSparkConnect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("run_dbt")

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
DEFAULT_CONFIG = REPO_ROOT / "generated" / "emr_config.json"
DEFAULT_PROJECT_DIR = REPO_ROOT / "dbt"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=os.environ.get("EMR_CONFIG_FILE", str(DEFAULT_CONFIG)),
        help="Path to emr_config.json (default: generated/emr_config.json or $EMR_CONFIG_FILE).",
    )
    parser.add_argument(
        "--project-dir",
        default=os.environ.get("DBT_PROJECT_DIR", str(DEFAULT_PROJECT_DIR)),
        help="dbt project directory.",
    )
    parser.add_argument(
        "--profiles-dir",
        default=os.environ.get("DBT_PROFILES_DIR"),
        help="dbt profiles directory (defaults to the project dir).",
    )
    parser.add_argument(
        "--no-terminate",
        action="store_true",
        help="Do not terminate the EMR Serverless session at the end (for debugging).",
    )
    parser.add_argument(
        "--publish-docs",
        action="store_true",
        help=(
            "After the dbt command succeeds, upload <project>/target/static_index.html "
            "to s3://<docs_bucket>/index.html (docs_bucket comes from the config). "
            "Use with `-- docs generate --static`."
        ),
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help=(
            "Request a minimal Spark footprint for the session (small driver + at most "
            "one small executor). Use for lightweight, metadata-only work such as "
            "`docs generate`, so it fits under the app's maximumCapacity next to other sessions."
        ),
    )
    parser.add_argument(
        "dbt_command",
        nargs=argparse.REMAINDER,
        help="dbt command and args after `--` (default: build).",
    )
    return parser.parse_args(argv)


def load_config(path: str) -> dict:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise SystemExit(
            f"Config file not found: {cfg_path}\n"
            "Run `terraform apply` first (it generates generated/emr_config.json), "
            "or pass --config."
        )
    with cfg_path.open() as fh:
        return json.load(fh)


def resolve_dbt_executable() -> str:
    dbt = shutil.which("dbt")
    if dbt:
        return dbt
    candidate = Path(sys.executable).parent / "dbt"
    if candidate.exists():
        return str(candidate)
    raise SystemExit("Could not find the `dbt` executable on PATH or next to the Python interpreter.")


def publish_docs(config: dict, project_dir: str) -> None:
    """Upload the self-contained dbt docs page to the docs bucket as index.html.

    The bucket is fronted by CloudFront (see terraform/docs_site.tf), so the new
    page is live at the docs URL as soon as the object is replaced. Produced by
    `dbt docs generate --static`, which writes target/static_index.html.
    """
    docs_bucket = config.get("docs_bucket")
    if not docs_bucket:
        raise SystemExit(
            "--publish-docs was set but 'docs_bucket' is missing from the config. "
            "Set enable_docs_site = true and re-run `terraform apply` to regenerate it."
        )
    static_file = Path(project_dir) / "target" / "static_index.html"
    if not static_file.exists():
        raise SystemExit(
            f"--publish-docs: expected {static_file} but it does not exist. "
            "Run a static docs command first, e.g. `-- docs generate --static`."
        )

    import boto3

    s3 = boto3.client("s3", region_name=config["region"])
    logger.info("Publishing docs: %s -> s3://%s/index.html", static_file, docs_bucket)
    s3.upload_file(
        str(static_file),
        docs_bucket,
        "index.html",
        ExtraArgs={"ContentType": "text/html; charset=utf-8"},
    )
    logger.info("Docs published to s3://%s/index.html", docs_bucket)


# Minimal EMR Serverless footprint for lightweight, metadata-only work such as
# `dbt docs generate` (catalog introspection). We cap the executor COUNT to one
# (scaling to zero when idle) and keep EMR Serverless's default, known-valid worker
# sizes. This bounds the session to a driver + at most one executor, so it fits
# comfortably under the application's maximumCapacity next to a concurrent build.
#
# We deliberately do NOT hand-set spark.{driver,executor}.memory/cores: EMR
# Serverless requires container memory (heap + ~10% overhead) to land exactly on a
# supported worker size, and off-by-a-fraction values fail to schedule
# ("Session scheduling timed out"). Capping the count is the safe lever.
MINIMAL_SPARK_DEFAULTS = {
    "spark.driver.cores": "2",
    "spark.driver.memory": "4g",
    "spark.executor.cores": "2",
    "spark.executor.memory": "4g",
}


def apply_minimal_capacity(runtime_configuration: list | None) -> list:
    """Merge the minimal-capacity Spark defaults into the session's spark-defaults.

    Preserves the existing spark-defaults (e.g. the S3 Tables catalog config) and
    just layers the small driver/executor sizing on top.
    """
    import copy

    cfg = copy.deepcopy(runtime_configuration) if runtime_configuration else []
    spark_defaults = next(
        (e for e in cfg if e.get("classification") == "spark-defaults"), None
    )
    if spark_defaults is None:
        spark_defaults = {"classification": "spark-defaults", "properties": {}}
        cfg.append(spark_defaults)
    spark_defaults.setdefault("properties", {})
    spark_defaults["properties"].update(MINIMAL_SPARK_DEFAULTS)
    return cfg


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    config = load_config(args.config)

    dbt_command = args.dbt_command
    if dbt_command and dbt_command[0] == "--":
        dbt_command = dbt_command[1:]
    if not dbt_command:
        dbt_command = ["build"]

    project_dir = str(Path(args.project_dir).resolve())
    profiles_dir = str(Path(args.profiles_dir).resolve()) if args.profiles_dir else project_dir

    schema = config.get("dbt_target_schema") or config.get("namespace", "analytics")

    session = EmrServerlessSparkConnect(
        application_id=config["application_id"],
        region=config["region"],
    )

    runtime_configuration = config.get("spark_runtime_configuration")
    if args.minimal:
        runtime_configuration = apply_minimal_capacity(runtime_configuration)
        logger.info("Using a minimal Spark footprint for this session.")

    try:
        session.ensure_application_started()
        session.start_session(
            execution_role_arn=config["execution_role_arn"],
            runtime_configuration=runtime_configuration,
            idle_timeout_minutes=int(config.get("session_idle_timeout_minutes", 30)),
            name="dbt-spark-connect",
        )
        session.wait_until_ready()
        endpoint = session.get_endpoint()

        env = os.environ.copy()
        # The stock dbt-spark `session` method calls SparkSession.builder.getOrCreate();
        # SPARK_REMOTE makes that a remote Spark Connect session against EMR Serverless.
        env["SPARK_REMOTE"] = endpoint.spark_remote_url
        env["DBT_SPARK_SCHEMA"] = schema
        # dbt-spark's session method calls enableHiveSupport(), which Spark Connect
        # rejects (catalogImplementation is a static config). It's a harmless warning
        # per statement; silence just that noisy source.
        env.setdefault("PYTHONWARNINGS", "ignore::UserWarning:pyspark.sql.connect.conf")

        cmd = [
            resolve_dbt_executable(),
            *dbt_command,
            "--project-dir",
            project_dir,
            "--profiles-dir",
            profiles_dir,
        ]
        logger.info("Running: %s", " ".join(dbt_command))
        result = subprocess.run(cmd, env=env)
        if result.returncode == 0 and args.publish_docs:
            publish_docs(config, project_dir)
        return result.returncode
    finally:
        if args.no_terminate:
            logger.warning(
                "Leaving session %s running (--no-terminate). Terminate it to stop billing.",
                session.session_id,
            )
        else:
            session.terminate()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
