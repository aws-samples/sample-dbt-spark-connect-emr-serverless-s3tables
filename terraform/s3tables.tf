############################################
# Amazon S3 Tables (managed Apache Iceberg)
#
# The table bucket is the warehouse for the Spark S3 Tables catalog. dbt writes
# seeds (raw_*) and models (stg_* are ephemeral; dim_*, fct_* are Iceberg tables)
# into the namespace below, executing through the EMR Serverless Spark Connect
# session. The raw_* seed tables are the "tables on S3 Tables used by dbt models".
############################################

resource "aws_s3tables_table_bucket" "this" {
  name = local.table_bucket_name

  # Allow `terraform destroy` to remove the namespaces/tables dbt created.
  # These are NOT recoverable once destroyed - safe for an example, remove for prod.
  force_destroy = true
}

# S3 Tables applies default maintenance (compaction + snapshot/unreferenced-file
# management) automatically. To customise it, add an
# aws_s3tables_table_bucket_maintenance_configuration resource (schema varies by
# provider version) or manage it via the S3 Tables maintenance APIs.

resource "aws_s3tables_namespace" "analytics" {
  namespace        = var.s3tables_namespace
  table_bucket_arn = aws_s3tables_table_bucket.this.arn
}
