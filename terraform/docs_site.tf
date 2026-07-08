############################################
# Password-protected dbt docs site
#
# Private S3 bucket (docs HTML) --> CloudFront (OAC) --> browser.
# A CloudFront Function enforces HTTP Basic Auth at the edge. The credential is
# precomputed as base64("user:password") and embedded in the function.
############################################

locals {
  docs_enabled = var.enable_docs_site

  docs_password = var.docs_basic_auth_password != "" ? var.docs_basic_auth_password : (
    local.docs_enabled ? random_password.docs[0].result : ""
  )
  docs_auth_b64 = local.docs_enabled ? base64encode("${var.docs_basic_auth_username}:${local.docs_password}") : ""
}

resource "random_password" "docs" {
  count   = local.docs_enabled && var.docs_basic_auth_password == "" ? 1 : 0
  length  = 20
  special = false # keep it copy/paste friendly for a docs URL login
}

resource "aws_s3_bucket" "docs" {
  count         = local.docs_enabled ? 1 : 0
  bucket        = "${var.name_prefix}-docs-${random_id.suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "docs" {
  count  = local.docs_enabled ? 1 : 0
  bucket = aws_s3_bucket.docs[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Seed the docs site with the initial (placeholder) HTML. Thereafter the content
# is refreshed out-of-band by the dbt_docs_refresh DAG (or `run_dbt.py
# --publish-docs` locally), which overwrites index.html directly. ignore_changes
# stops later `terraform apply` runs from reverting those refreshes back to the
# local placeholder.
resource "aws_s3_object" "docs_index" {
  count        = local.docs_enabled ? 1 : 0
  bucket       = aws_s3_bucket.docs[0].id
  key          = "index.html"
  source       = "${local.repo_root}/${var.docs_index_file}"
  etag         = filemd5("${local.repo_root}/${var.docs_index_file}")
  content_type = "text/html; charset=utf-8"

  lifecycle {
    ignore_changes = [etag, source]
  }
}

resource "aws_cloudfront_origin_access_control" "docs" {
  count                             = local.docs_enabled ? 1 : 0
  name                              = "${var.name_prefix}-docs-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_function" "docs_basic_auth" {
  count   = local.docs_enabled ? 1 : 0
  name    = "${var.name_prefix}-docs-basic-auth"
  runtime = "cloudfront-js-2.0"
  comment = "HTTP Basic Auth for the dbt docs site"
  publish = true
  code = templatefile("${path.module}/templates/basic_auth.js.tftpl", {
    auth_b64 = local.docs_auth_b64
    realm    = "${var.name_prefix} dbt docs"
  })
}

data "aws_cloudfront_cache_policy" "optimized" {
  count = local.docs_enabled ? 1 : 0
  name  = "Managed-CachingOptimized"
}

resource "aws_cloudfront_distribution" "docs" {
  count               = local.docs_enabled ? 1 : 0
  enabled             = true
  default_root_object = "index.html"
  comment             = "${var.name_prefix} dbt docs (password protected)"
  price_class         = "PriceClass_100"

  origin {
    origin_id                = "docs-s3"
    domain_name              = aws_s3_bucket.docs[0].bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.docs[0].id
  }

  default_cache_behavior {
    target_origin_id       = "docs-s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = data.aws_cloudfront_cache_policy.optimized[0].id
    compress               = true

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.docs_basic_auth[0].arn
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = local.common_tags
}

# Allow only this CloudFront distribution (via OAC) to read the docs bucket.
data "aws_iam_policy_document" "docs_bucket" {
  count = local.docs_enabled ? 1 : 0

  statement {
    sid       = "AllowCloudFrontOAC"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.docs[0].arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.docs[0].arn]
    }
  }
}

resource "aws_s3_bucket_policy" "docs" {
  count  = local.docs_enabled ? 1 : 0
  bucket = aws_s3_bucket.docs[0].id
  policy = data.aws_iam_policy_document.docs_bucket[0].json
}
