############################################
# EMR Serverless application (Apache Spark)
############################################

resource "aws_security_group" "emr" {
  name        = "${local.name}-emr-serverless"
  description = "EMR Serverless worker ENIs"
  vpc_id      = module.vpc.vpc_id

  egress {
    description = "Allow all egress (S3 gateway endpoint, S3 Tables API via NAT, etc.)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "${local.name}-emr-serverless" })
}

resource "aws_security_group_rule" "emr_self" {
  description              = "Allow intra-application worker communication"
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.emr.id
  source_security_group_id = aws_security_group.emr.id
}

resource "aws_emrserverless_application" "this" {
  name          = "${local.name}-spark"
  release_label = var.emr_release_label
  type          = "SPARK"
  architecture  = var.emr_architecture

  network_configuration {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.emr.id]
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = var.emr_max_cpu
    memory = var.emr_max_memory
  }

  # Warm pool for faster interactive session startup (optional).
  dynamic "initial_capacity" {
    for_each = var.enable_pre_initialized_capacity ? ["DRIVER", "EXECUTOR"] : []
    content {
      initial_capacity_type = initial_capacity.value

      initial_capacity_config {
        worker_count = initial_capacity.value == "DRIVER" ? 1 : 2
        worker_configuration {
          cpu    = "4 vCPU"
          memory = "16 GB"
        }
      }
    }
  }

  # interactive_configuration in the aws provider only exposes livy/studio today,
  # NOT session_enabled (required for Spark Connect). We enable sessions via the
  # null_resource below using the AWS CLI. See enable_spark_connect_sessions.
  lifecycle {
    ignore_changes = [interactive_configuration]
  }

  tags = local.common_tags
}

############################################
# Enable interactive Spark Connect sessions (sessionEnabled=true)
#
# Not yet exposed by the aws/awscc Terraform providers, so we set it with the
# AWS CLI. UpdateApplication is a partial update and works while the freshly
# created application is in the STOPPED state. Requires the AWS CLI + valid
# credentials on the machine running `terraform apply`.
############################################

resource "null_resource" "enable_spark_connect_sessions" {
  triggers = {
    application_id = aws_emrserverless_application.this.id
    region         = var.region
    # Re-assert on EVERY apply. The aws provider does not manage sessionEnabled
    # and clears interactiveConfiguration whenever it updates the application
    # (e.g. a tag or capacity change), which silently disables Spark Connect. The
    # provisioner below is a fast no-op when the flag is already set, so running
    # it each apply is cheap and keeps sessions reliably enabled.
    always_run = timestamp()
  }

  # Fast path: if sessions are already enabled, do nothing (safe even while the
  # app is STARTED). Otherwise UpdateApplication requires the app to be STOPPED,
  # so stop it first, then set AND verify the flag persisted, with retries.
  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -uo pipefail
      APP="${aws_emrserverless_application.this.id}"
      R="${var.region}"

      EN=$(aws emr-serverless get-application ${local.aws_profile_flag} --region "$R" \
        --application-id "$APP" --query "application.interactiveConfiguration.sessionEnabled" --output text 2>/dev/null || echo "")
      if [ "$EN" = "True" ] || [ "$EN" = "true" ]; then
        echo "sessionEnabled already true on $APP; nothing to do"
        exit 0
      fi

      # UpdateApplication only works in CREATED/STOPPED state.
      ST=$(aws emr-serverless get-application ${local.aws_profile_flag} --region "$R" \
        --application-id "$APP" --query "application.state" --output text 2>/dev/null || echo "")
      if [ "$ST" = "STARTED" ] || [ "$ST" = "STARTING" ]; then
        echo "stopping $APP to update interactive configuration..."
        aws emr-serverless stop-application ${local.aws_profile_flag} --region "$R" --application-id "$APP" >/dev/null 2>&1 || true
        for w in $(seq 1 30); do
          ST=$(aws emr-serverless get-application ${local.aws_profile_flag} --region "$R" \
            --application-id "$APP" --query "application.state" --output text 2>/dev/null || echo "")
          [ "$ST" = "STOPPED" ] && break
          echo "  waiting for STOPPED (state=$ST)..."
          sleep 10
        done
      fi

      for attempt in $(seq 1 10); do
        aws emr-serverless update-application ${local.aws_profile_flag} --region "$R" \
          --application-id "$APP" \
          --interactive-configuration '{"sessionEnabled":true,"livyEndpointEnabled":false,"studioEnabled":false}' >/dev/null 2>&1 || true
        sleep 5
        EN=$(aws emr-serverless get-application ${local.aws_profile_flag} --region "$R" \
          --application-id "$APP" --query "application.interactiveConfiguration.sessionEnabled" --output text 2>/dev/null || echo "")
        if [ "$EN" = "True" ] || [ "$EN" = "true" ]; then
          echo "sessionEnabled=true confirmed on $APP"
          exit 0
        fi
        echo "sessionEnabled not persisted yet (attempt $attempt/10), retrying..."
        sleep 10
      done
      echo "ERROR: could not persist sessionEnabled=true on $APP"
      exit 1
    EOT
  }
}
