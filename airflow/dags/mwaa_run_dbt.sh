#!/bin/bash
# MWAA task bootstrap: install the STOCK dbt-spark adapter (from PyPI) into an
# ISOLATED venv, then run the shared runner/run_dbt.py.
#
# SPARK_REMOTE env var (set by run_dbt.py) open a remote Spark Connect session on
# EMR Serverless. Running dbt in its own venv keeps its heavy dependency tree
# (dbt-core, pyspark[connect]) away from Airflow's own environment.
set -euo pipefail

DBT_CMD="${1:-build}"
shift || true
EXTRA_ARGS="$*"

# Set PUBLISH_DOCS=1 to also upload the generated static docs page to the docs
# bucket (used by the dbt_docs_refresh DAG with `docs generate --static`).
# Set MINIMAL=1 to request a minimal Spark footprint (small driver + one small
# executor) — appropriate for lightweight, metadata-only work like docs generate.
RUNNER_FLAGS=""
if [ "${PUBLISH_DOCS:-}" = "1" ]; then
  RUNNER_FLAGS="$RUNNER_FLAGS --publish-docs"
fi
if [ "${MINIMAL:-}" = "1" ]; then
  RUNNER_FLAGS="$RUNNER_FLAGS --minimal"
fi

DAGS_DIR="${AIRFLOW_HOME:-/usr/local/airflow}/dags"
CONFIG="$DAGS_DIR/emr_config.json"

# Version specs come from emr_config.json (rendered by Terraform) so they stay in
# one place. Fall back to sensible defaults if absent.
DBT_SPARK_SPEC="$(python3 -c "import json;print(json.load(open('$CONFIG')).get('dbt_spark_pip_spec','dbt-spark[session]==1.11.0b2'))")"
PYSPARK_SPEC="$(python3 -c "import json;print(json.load(open('$CONFIG')).get('pyspark_pip_spec','pyspark[connect]==4.0.2'))")"

# Key the venv path to the exact package specs so a stale venv from a different
# spec (or an older bootstrap) is never reused. Also health-check it.
SPEC_TAG="$(printf '%s' "$DBT_SPARK_SPEC $PYSPARK_SPEC" | md5sum | cut -c1-10)"
VENV="/tmp/dbt-venv-$SPEC_TAG"

echo "[mwaa_run_dbt] config=$CONFIG dbt='$DBT_CMD $EXTRA_ARGS' venv=$VENV"

if [ ! -x "$VENV/bin/dbt" ] || ! "$VENV/bin/dbt" --version >/dev/null 2>&1; then
  echo "[mwaa_run_dbt] building venv: $DBT_SPARK_SPEC $PYSPARK_SPEC"
  rm -rf "$VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --upgrade pip wheel >/dev/null
  "$VENV/bin/pip" install "$DBT_SPARK_SPEC" "$PYSPARK_SPEC" boto3
else
  echo "[mwaa_run_dbt] reusing healthy venv"
fi

# MWAA syncs dags/ read-only, but dbt must write target/ and logs/. Run from a
# writable copy of the project.
WORKDIR="/tmp/dbt-project"
rm -rf "$WORKDIR"
cp -r "$DAGS_DIR/dbt_project" "$WORKDIR"

# Run the shared runner (starts the Spark Connect session, sets SPARK_REMOTE, runs dbt).
exec "$VENV/bin/python" "$DAGS_DIR/runner/run_dbt.py" \
  --config "$CONFIG" \
  --project-dir "$WORKDIR" \
  $RUNNER_FLAGS \
  -- $DBT_CMD $EXTRA_ARGS
