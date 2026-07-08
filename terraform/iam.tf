############################################
# EMR Serverless execution role
#
# Assumed by the Spark driver/executors of each interactive session to read and
# write the S3 Tables Iceberg tables.
############################################

data "aws_iam_policy_document" "emr_exec_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    # Guard against the confused-deputy problem.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "emr_exec" {
  name               = "${local.name}-emr-exec"
  assume_role_policy = data.aws_iam_policy_document.emr_exec_assume.json
}

data "aws_iam_policy_document" "emr_exec" {
  # S3 Tables access, scoped to this table bucket and its tables only.
  statement {
    sid    = "S3TablesAccess"
    effect = "Allow"
    actions = [
      "s3tables:GetTableBucket",
      "s3tables:ListTableBuckets",
      "s3tables:CreateNamespace",
      "s3tables:GetNamespace",
      "s3tables:ListNamespaces",
      "s3tables:DeleteNamespace",
      "s3tables:CreateTable",
      "s3tables:GetTable",
      "s3tables:ListTables",
      "s3tables:DeleteTable",
      "s3tables:RenameTable",
      "s3tables:UpdateTableMetadataLocation",
      "s3tables:GetTableMetadataLocation",
      "s3tables:GetTableData",
      "s3tables:PutTableData",
      "s3tables:GetTableMaintenanceConfiguration",
      "s3tables:PutTableMaintenanceConfiguration",
    ]
    resources = [
      local.table_bucket_arn,
      "${local.table_bucket_arn}/*",
    ]
  }

  # Write Spark driver/executor logs to CloudWatch.
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
      "logs:DescribeLogGroups",
    ]
    resources = ["arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:/aws/emr-serverless/*"]
  }
}

resource "aws_iam_role_policy" "emr_exec" {
  name   = "s3tables-and-logs"
  role   = aws_iam_role.emr_exec.id
  policy = data.aws_iam_policy_document.emr_exec.json
}

############################################
# Session caller policy
#
# Grants the permissions needed to open an interactive Spark Connect session on
# the EMR Serverless application and pass the execution role. Attach this to:
#   - the MWAA execution role (done automatically in mwaa.tf)
#   - your local IAM user/role that runs dbt from a laptop (attach manually;
#     the ARN is a Terraform output)
############################################

data "aws_iam_policy_document" "session_caller" {
  statement {
    sid    = "ApplicationLevelAccess"
    effect = "Allow"
    actions = [
      "emr-serverless:StartSession",
      "emr-serverless:ListSessions",
      "emr-serverless:GetApplication",
      "emr-serverless:StartApplication",
    ]
    resources = [aws_emrserverless_application.this.arn]
  }

  statement {
    sid    = "SessionLevelAccess"
    effect = "Allow"
    actions = [
      "emr-serverless:GetSession",
      "emr-serverless:GetSessionEndpoint",
      "emr-serverless:TerminateSession",
      "emr-serverless:GetResourceDashboard",
    ]
    resources = ["${aws_emrserverless_application.this.arn}/sessions/*"]
  }

  statement {
    sid       = "PassExecutionRole"
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_exec.arn]

    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  # Publish the generated docs page to the docs bucket (run_dbt.py --publish-docs).
  # Only present when the docs site is enabled.
  dynamic "statement" {
    for_each = local.docs_enabled ? [1] : []
    content {
      sid       = "PublishDocs"
      effect    = "Allow"
      actions   = ["s3:PutObject", "s3:GetObject"]
      resources = ["${aws_s3_bucket.docs[0].arn}/*"]
    }
  }
}

resource "aws_iam_policy" "session_caller" {
  name        = "${local.name}-session-caller"
  description = "Permissions to open interactive EMR Serverless Spark Connect sessions and run dbt"
  policy      = data.aws_iam_policy_document.session_caller.json
}
