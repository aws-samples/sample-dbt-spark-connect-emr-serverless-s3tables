provider "aws" {
  region = var.region
  # Explicit profile takes precedence over the AWS_PROFILE environment variable.
  # Leave aws_profile null to fall back to the standard AWS credential chain.
  profile = var.aws_profile

  default_tags {
    tags = local.common_tags
  }
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}
