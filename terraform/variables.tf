variable "region" {
  description = "AWS region to deploy into. Must support Amazon EMR Serverless, S3 Tables and (optionally) MWAA. emr-spark-8.0.0 and S3 Tables are available in the major commercial regions (e.g. us-east-1, us-west-2, eu-west-1)."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Named AWS CLI/SDK profile to use. Set to null to use the default credential chain (AWS_PROFILE env var, instance role, etc.). Set explicitly to override an inherited AWS_PROFILE."
  type        = string
  default     = null
}

variable "name_prefix" {
  description = "Prefix applied to the names of all created resources. Keep it short (<= 20 chars) and lowercase."
  type        = string
  default     = "dbt-emrs"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,19}$", var.name_prefix))
    error_message = "name_prefix must be lowercase alphanumeric/hyphen and start with a letter (2-20 chars)."
  }
}

variable "tags" {
  description = "Additional tags applied to every resource."
  type        = map(string)
  default     = {}
}

############################################
# Networking
############################################

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "az_count" {
  description = "Number of Availability Zones to spread the subnets across. MWAA requires at least 2."
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be 2 or 3 (MWAA needs at least two private subnets in distinct AZs)."
  }
}

variable "single_nat_gateway" {
  description = "If true, provision a single shared NAT gateway (cheaper). If false, one NAT gateway per AZ (highly available)."
  type        = bool
  default     = true
}

variable "enable_interface_endpoints" {
  description = "Create interface VPC endpoints (STS, CloudWatch Logs, EMR Serverless, Glue, S3 Tables) so private-subnet workloads can reach AWS APIs without traversing the NAT gateway. The S3 gateway endpoint is always created."
  type        = bool
  default     = true
}

############################################
# S3 Tables
############################################

variable "s3tables_namespace" {
  description = "S3 Tables namespace (schema) that dbt writes into. S3 Tables namespaces are single-level."
  type        = string
  default     = "analytics"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,62}$", var.s3tables_namespace))
    error_message = "s3tables_namespace must be lowercase alphanumeric/underscore and start with a letter."
  }
}

variable "spark_catalog_name" {
  description = "Name of the Spark catalog that maps to the S3 Tables bucket. dbt uses this as the default catalog."
  type        = string
  default     = "s3tablesbucket"
}

variable "s3tables_spark_packages" {
  description = <<-EOT
    Optional Maven coordinates passed to `spark.jars.packages` to load the Amazon S3 Tables
    Catalog for Apache Iceberg client. Leave empty ("") to rely on the S3 Tables classes that
    are bundled natively with emr-spark-8.0.0 (recommended). If your release does not bundle
    them, set this to e.g. "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.8".
    NOTE: adding an external package that ships its own Iceberg build can conflict with the
    Iceberg version bundled in EMR, so prefer the bundled classes when available.
  EOT
  type        = string
  default     = ""
}

############################################
# EMR Serverless
############################################

variable "emr_release_label" {
  description = "Amazon EMR release label. emr-spark-8.0.0 ships Apache Spark 4.0.2 and native S3 Tables (Iceberg v3) integration."
  type        = string
  default     = "emr-spark-8.0.0"
}

variable "emr_architecture" {
  description = "CPU architecture for EMR Serverless workers: X86_64 or ARM64."
  type        = string
  default     = "ARM64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.emr_architecture)
    error_message = "emr_architecture must be X86_64 or ARM64."
  }
}

variable "emr_max_cpu" {
  description = "Maximum cumulative vCPU the EMR Serverless application may allocate."
  type        = string
  default     = "16 vCPU"
}

variable "emr_max_memory" {
  description = "Maximum cumulative memory the EMR Serverless application may allocate."
  type        = string
  default     = "64 GB"
}

variable "emr_idle_timeout_minutes" {
  description = "Auto-stop the EMR Serverless application after this many minutes with no active sessions or jobs."
  type        = number
  default     = 15
}

variable "enable_pre_initialized_capacity" {
  description = "Pre-initialize one Spark driver + executors so interactive Spark Connect sessions start faster (warm pool). Increases idle cost while the application is started."
  type        = bool
  default     = false
}

############################################
# MWAA (managed Airflow)
############################################

variable "enable_mwaa" {
  description = "Whether to create an Amazon MWAA environment for scheduling. Set to false to deploy only the local-run stack (VPC + S3 Tables + EMR Serverless)."
  type        = bool
  default     = true
}

variable "mwaa_airflow_version" {
  description = "Airflow version for the MWAA environment. 2.10.3 runs on Python 3.11, which matches the emr-spark-8.0.0 worker Python."
  type        = string
  default     = "2.10.3"
}

variable "mwaa_environment_class" {
  description = "MWAA environment size class."
  type        = string
  default     = "mw1.small"
}

variable "mwaa_webserver_access_mode" {
  description = "MWAA web server access mode: PUBLIC_ONLY or PRIVATE_ONLY."
  type        = string
  default     = "PUBLIC_ONLY"

  validation {
    condition     = contains(["PUBLIC_ONLY", "PRIVATE_ONLY"], var.mwaa_webserver_access_mode)
    error_message = "mwaa_webserver_access_mode must be PUBLIC_ONLY or PRIVATE_ONLY."
  }
}

############################################
# dbt runtime versions (stock adapter, installed from PyPI)
############################################

variable "dbt_spark_pip_spec" {
  description = "pip requirement for the stock dbt-spark adapter installed into MWAA's isolated dbt venv. Uses the `session` method + SPARK_REMOTE (no fork). Pin to a version that supports the EMR release's Spark line (1.11.x supports Spark 4 / dbt-core 1.12). Keep in sync with runner/pyproject.toml."
  type        = string
  default     = "dbt-spark[session]==1.11.0b2"
}

variable "pyspark_version" {
  description = "PySpark version installed in MWAA's dbt venv (with the [connect] extra). MUST match the EMR Serverless Spark version (4.0.2 for emr-spark-8.0.0)."
  type        = string
  default     = "4.0.2"
}

############################################
# Spark Connect session behaviour
############################################

variable "session_idle_timeout_minutes" {
  description = "Idle timeout (minutes) requested for each interactive Spark Connect session. The session (and its billing) stops after this much inactivity if not terminated first."
  type        = number
  default     = 30
}

variable "dbt_target_schema" {
  description = "dbt target schema. Must match the S3 Tables namespace so dbt writes into it."
  type        = string
  default     = "analytics"
}

############################################
# dbt docs site (CloudFront + password)
############################################

variable "enable_docs_site" {
  description = "Publish the generated dbt docs (lineage + catalog) as a password-protected static site on CloudFront (private S3 origin + OAC + Basic Auth via a CloudFront Function)."
  type        = bool
  default     = false
}

variable "docs_basic_auth_username" {
  description = "HTTP Basic Auth username for the dbt docs site."
  type        = string
  default     = "dbt"
}

variable "docs_basic_auth_password" {
  description = "HTTP Basic Auth password for the dbt docs site. Leave empty to auto-generate a strong password (surfaced as a sensitive Terraform output)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "docs_index_file" {
  description = "Path (relative to the repo root) to the self-contained dbt docs HTML to publish. Produced by `dbt docs generate --static` and copied here by the docs helper."
  type        = string
  default     = "generated/docs/index.html"
}
