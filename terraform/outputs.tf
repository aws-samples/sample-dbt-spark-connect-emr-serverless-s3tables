output "region" {
  description = "AWS region."
  value       = var.region
}

output "vpc_id" {
  description = "VPC id."
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet ids (EMR Serverless workers, MWAA)."
  value       = module.vpc.private_subnets
}

output "table_bucket_arn" {
  description = "S3 Tables table bucket ARN (the Spark catalog warehouse)."
  value       = local.table_bucket_arn
}

output "table_bucket_name" {
  description = "S3 Tables table bucket name."
  value       = aws_s3tables_table_bucket.this.name
}

output "s3tables_namespace" {
  description = "S3 Tables namespace dbt writes into."
  value       = var.s3tables_namespace
}

output "emr_application_id" {
  description = "EMR Serverless application id."
  value       = aws_emrserverless_application.this.id
}

output "emr_application_arn" {
  description = "EMR Serverless application ARN."
  value       = aws_emrserverless_application.this.arn
}

output "emr_execution_role_arn" {
  description = "EMR Serverless execution role ARN (passed to StartSession)."
  value       = aws_iam_role.emr_exec.arn
}

output "session_caller_policy_arn" {
  description = "Attach this managed policy to the IAM user/role you use locally to run dbt against EMR Serverless."
  value       = aws_iam_policy.session_caller.arn
}

output "artifacts_bucket" {
  description = "S3 bucket holding the MWAA DAGs, the dbt project and the runner."
  value       = aws_s3_bucket.artifacts.bucket
}

output "emr_config_file" {
  description = "Path to the generated config consumed by the local runner."
  value       = local_file.emr_config.filename
}

output "mwaa_environment_name" {
  description = "MWAA environment name (null when disabled)."
  value       = var.enable_mwaa ? aws_mwaa_environment.this[0].name : null
}

output "mwaa_webserver_url" {
  description = "MWAA Airflow UI URL (null when disabled)."
  value       = var.enable_mwaa ? aws_mwaa_environment.this[0].webserver_url : null
}

output "docs_url" {
  description = "URL of the password-protected dbt docs site (null when disabled)."
  value       = local.docs_enabled ? "https://${aws_cloudfront_distribution.docs[0].domain_name}/" : null
}

output "docs_bucket" {
  description = "S3 bucket the dbt docs site is served from (docs refresh uploads index.html here)."
  value       = local.docs_enabled ? aws_s3_bucket.docs[0].bucket : null
}

output "docs_username" {
  description = "HTTP Basic Auth username for the dbt docs site."
  value       = local.docs_enabled ? var.docs_basic_auth_username : null
}

output "docs_password" {
  description = "HTTP Basic Auth password for the dbt docs site."
  value       = local.docs_enabled ? local.docs_password : null
  sensitive   = true
}
