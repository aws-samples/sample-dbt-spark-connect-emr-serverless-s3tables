############################################
# Single source of truth: emr_config.json
#
# Rendered by Terraform and consumed by BOTH the local runner and the MWAA DAG.
# It carries every value the runner needs to open a Spark Connect session and run
# dbt, including the exact Spark runtimeConfiguration for the S3 Tables catalog.
############################################

locals {
  repo_root = abspath("${path.module}/..")

  emr_config = {
    region                       = var.region
    application_id               = aws_emrserverless_application.this.id
    application_arn              = aws_emrserverless_application.this.arn
    execution_role_arn           = aws_iam_role.emr_exec.arn
    table_bucket_arn             = local.table_bucket_arn
    spark_catalog_name           = var.spark_catalog_name
    namespace                    = var.s3tables_namespace
    dbt_target_schema            = var.dbt_target_schema
    artifacts_bucket             = aws_s3_bucket.artifacts.bucket
    docs_bucket                  = local.docs_enabled ? aws_s3_bucket.docs[0].bucket : null
    session_idle_timeout_minutes = var.session_idle_timeout_minutes

    # pip specs MWAA installs into its isolated dbt venv (stock adapter, no fork).
    # Keep in sync with runner/pyproject.toml and the EMR release Spark version.
    dbt_spark_pip_spec = var.dbt_spark_pip_spec
    pyspark_pip_spec   = "pyspark[connect]==${var.pyspark_version}"

    # Passed verbatim to StartSession's configurationOverrides.
    spark_runtime_configuration = [
      {
        classification = "spark-defaults"
        properties     = local.spark_catalog_conf
      }
    ]
  }
}

resource "local_file" "emr_config" {
  content              = jsonencode(local.emr_config)
  filename             = "${local.repo_root}/generated/emr_config.json"
  file_permission      = "0644"
  directory_permission = "0755"
}

############################################
# Upload DAG + dbt project + runner + config to the MWAA "dags/" prefix
#
# Only needed when MWAA is enabled. Local runs read these files from the repo.
############################################

locals {
  # (relative path within repo) => (S3 key under dags/)
  # .airflowignore tells the Airflow DAG processor to skip the CLI runner and the
  # dbt project that are synced under dags/ (they are not DAGs). Without it, MWAA
  # tries to import dags/runner/run_dbt.py as a DAG and fails.
  mwaa_dag_files = var.enable_mwaa ? merge({
    "airflow/dags/dbt_emr_serverless_dag.py" = "dags/dbt_emr_serverless_dag.py"
    "airflow/dags/mwaa_run_dbt.sh"           = "dags/mwaa_run_dbt.sh"
    "airflow/dags/.airflowignore"            = "dags/.airflowignore"
    }, local.docs_enabled ? {
    # Docs-refresh DAG is only useful when the docs site exists.
    "airflow/dags/dbt_docs_refresh_dag.py" = "dags/dbt_docs_refresh_dag.py"
  } : {}) : {}

  dbt_files = var.enable_mwaa ? {
    for f in fileset("${local.repo_root}/dbt", "**") : f => "dags/dbt_project/${f}"
    if !can(regex("(target/|dbt_packages/|logs/|\\.user\\.yml|\\.DS_Store)", f))
  } : {}

  runner_files = var.enable_mwaa ? {
    for f in fileset("${local.repo_root}/runner", "*.py") : f => "dags/runner/${f}"
  } : {}
}

resource "aws_s3_object" "dag_files" {
  for_each = local.mwaa_dag_files

  bucket = aws_s3_bucket.artifacts.id
  key    = each.value
  source = "${local.repo_root}/${each.key}"
  etag   = filemd5("${local.repo_root}/${each.key}")
}

resource "aws_s3_object" "dbt_files" {
  for_each = local.dbt_files

  bucket = aws_s3_bucket.artifacts.id
  key    = each.value
  source = "${local.repo_root}/dbt/${each.key}"
  etag   = filemd5("${local.repo_root}/dbt/${each.key}")
}

resource "aws_s3_object" "runner_files" {
  for_each = local.runner_files

  bucket = aws_s3_bucket.artifacts.id
  key    = each.value
  source = "${local.repo_root}/runner/${each.key}"
  etag   = filemd5("${local.repo_root}/runner/${each.key}")
}

resource "aws_s3_object" "emr_config_mwaa" {
  count = var.enable_mwaa ? 1 : 0

  bucket  = aws_s3_bucket.artifacts.id
  key     = "dags/emr_config.json"
  content = jsonencode(local.emr_config)
  etag    = md5(jsonencode(local.emr_config))
}
