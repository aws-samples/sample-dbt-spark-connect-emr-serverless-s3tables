############################################
# Amazon MWAA (Managed Workflows for Apache Airflow)
#
# Schedules the dbt-on-EMR-Serverless pipeline. The heavy dbt + pyspark[connect]
# dependencies are installed into an isolated venv at task runtime (see
# airflow/dags/mwaa_run_dbt.sh) to avoid conflicts with Airflow's own environment.
############################################

data "aws_iam_policy_document" "mwaa_assume" {
  count = var.enable_mwaa ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa" {
  count              = var.enable_mwaa ? 1 : 0
  name               = "${local.name}-mwaa-exec"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume[0].json
}

data "aws_iam_policy_document" "mwaa" {
  count = var.enable_mwaa ? 1 : 0

  statement {
    sid       = "AirflowPublishMetrics"
    effect    = "Allow"
    actions   = ["airflow:PublishMetrics"]
    resources = ["arn:${local.partition}:airflow:${var.region}:${local.account_id}:environment/${local.name}-airflow"]
  }

  statement {
    sid    = "ReadSourceBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject*",
      "s3:GetBucket*",
      "s3:List*",
    ]
    resources = [
      aws_s3_bucket.artifacts.arn,
      "${aws_s3_bucket.artifacts.arn}/*",
    ]
  }

  # Let the dbt_docs_refresh DAG publish the generated docs page to the docs
  # bucket (run_dbt.py --publish-docs). Only present when the docs site exists.
  dynamic "statement" {
    for_each = local.docs_enabled ? [1] : []
    content {
      sid       = "PublishDocs"
      effect    = "Allow"
      actions   = ["s3:PutObject", "s3:GetObject"]
      resources = ["${aws_s3_bucket.docs[0].arn}/*"]
    }
  }

  statement {
    sid    = "AirflowLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:CreateLogGroup",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:GetLogRecord",
      "logs:GetLogGroupFields",
      "logs:GetQueryResults",
      "logs:DescribeLogGroups",
    ]
    resources = ["arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:airflow-${local.name}-airflow-*"]
  }

  statement {
    sid       = "CloudWatchMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }

  statement {
    sid    = "CeleryQueue"
    effect = "Allow"
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ReceiveMessage",
      "sqs:SendMessage",
    ]
    resources = ["arn:${local.partition}:sqs:${var.region}:*:airflow-celery-*"]
  }

  statement {
    sid    = "KmsForAirflow"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
      "kms:GenerateDataKey*",
      "kms:Encrypt",
    ]
    not_resources = ["arn:${local.partition}:kms:*:${local.account_id}:key/*"]
    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values = [
        "sqs.${var.region}.amazonaws.com",
        "s3.${var.region}.amazonaws.com",
      ]
    }
  }
}

resource "aws_iam_role_policy" "mwaa" {
  count  = var.enable_mwaa ? 1 : 0
  name   = "mwaa-exec"
  role   = aws_iam_role.mwaa[0].id
  policy = data.aws_iam_policy_document.mwaa[0].json
}

# Let the scheduled DAG open EMR Serverless Spark Connect sessions.
resource "aws_iam_role_policy_attachment" "mwaa_session_caller" {
  count      = var.enable_mwaa ? 1 : 0
  role       = aws_iam_role.mwaa[0].name
  policy_arn = aws_iam_policy.session_caller.arn
}

resource "aws_security_group" "mwaa" {
  count       = var.enable_mwaa ? 1 : 0
  name        = "${local.name}-mwaa"
  description = "MWAA environment"
  vpc_id      = module.vpc.vpc_id

  # MWAA reaches S3, SQS (Celery), CloudWatch Logs, KMS, ECR and the EMR
  # Serverless endpoints over HTTPS, so internet-bound egress is pinned to 443.
  egress {
    description = "HTTPS to AWS services"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # MWAA runs its scheduler/worker/web components as Fargate tasks that talk to
  # each other and to the Celery/metadata backends on dynamic ports. AWS's
  # documented least-privilege MWAA security group keeps this self-referencing
  # all-traffic egress alongside the 443 rule.
  egress {
    description = "MWAA internal component communication"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  tags = merge(local.common_tags, { Name = "${local.name}-mwaa" })
}

resource "aws_security_group_rule" "mwaa_self" {
  count                    = var.enable_mwaa ? 1 : 0
  description              = "MWAA self ingress"
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.mwaa[0].id
  source_security_group_id = aws_security_group.mwaa[0].id
}

resource "aws_mwaa_environment" "this" {
  count = var.enable_mwaa ? 1 : 0

  name              = "${local.name}-airflow"
  airflow_version   = var.mwaa_airflow_version
  environment_class = var.mwaa_environment_class

  source_bucket_arn     = aws_s3_bucket.artifacts.arn
  dag_s3_path           = "dags"
  execution_role_arn    = aws_iam_role.mwaa[0].arn
  webserver_access_mode = var.mwaa_webserver_access_mode

  network_configuration {
    security_group_ids = [aws_security_group.mwaa[0].id]
    subnet_ids         = slice(module.vpc.private_subnets, 0, 2)
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  # Surface the pipeline configuration to the DAG as an Airflow variable/env.
  airflow_configuration_options = {
    "core.default_task_retries" = "1"
  }

  tags = local.common_tags

  depends_on = [
    aws_s3_object.dag_files,
    aws_s3_object.dbt_files,
    aws_s3_object.runner_files,
    aws_s3_object.emr_config_mwaa,
    aws_iam_role_policy.mwaa,
    aws_iam_role_policy_attachment.mwaa_session_caller,
    null_resource.enable_spark_connect_sessions,
  ]
}
