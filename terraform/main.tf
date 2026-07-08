locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  region     = var.region

  name = var.name_prefix

  # CLI flag so local-exec provisioners use the same profile as the provider,
  # regardless of any AWS_PROFILE set in the shell running terraform.
  aws_profile_flag = var.aws_profile != null ? "--profile ${var.aws_profile}" : ""

  common_tags = merge(
    {
      Project     = "dbt-emr-serverless-spark-connect"
      ManagedBy   = "terraform"
      Environment = "example"
    },
    var.tags,
  )

  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  # Table bucket names must be 3-63 chars, lowercase, no underscores.
  table_bucket_name = "${var.name_prefix}-tables-${random_id.suffix.hex}"
  table_bucket_arn  = aws_s3tables_table_bucket.this.arn

  # Generic (non-table) S3 bucket that holds the MWAA DAGs, dbt project and runner.
  artifacts_bucket_name = "${var.name_prefix}-artifacts-${random_id.suffix.hex}"

  # Spark configuration injected into every interactive Spark Connect session so that the
  # default catalog resolves to the S3 Tables bucket via the S3 Tables Catalog for Iceberg.
  # This catalog implementation supports CREATE TABLE AS SELECT, which dbt relies on.
  spark_catalog_conf = merge(
    {
      "spark.sql.extensions"                                                  = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      "spark.sql.catalog.${var.spark_catalog_name}"                           = "org.apache.iceberg.spark.SparkCatalog"
      "spark.sql.catalog.${var.spark_catalog_name}.catalog-impl"              = "software.amazon.s3tables.iceberg.S3TablesCatalog"
      "spark.sql.catalog.${var.spark_catalog_name}.warehouse"                 = local.table_bucket_arn
      "spark.sql.catalog.${var.spark_catalog_name}.client.region"             = var.region
      "spark.sql.catalog.${var.spark_catalog_name}.cache-enabled"             = "false"
      "spark.sql.defaultCatalog"                                              = var.spark_catalog_name
      "spark.sql.catalog.${var.spark_catalog_name}.glue.skip-name-validation" = "true"
    },
    var.s3tables_spark_packages != "" ? {
      "spark.jars.packages" = var.s3tables_spark_packages
    } : {},
  )
}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

resource "random_id" "suffix" {
  byte_length = 3
}
