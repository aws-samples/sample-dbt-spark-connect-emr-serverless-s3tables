############################################
# Best-practices VPC
#
# - Public subnets  : NAT gateway(s)
# - Private subnets : EMR Serverless workers, MWAA (no public IPs)
# - S3 Gateway endpoint : free, keeps S3 / S3 Tables data-file traffic on the AWS backbone
# - Interface endpoints : reach AWS control-plane APIs privately (optional)
############################################

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.19"

  name = "${local.name}-vpc"
  cidr = var.vpc_cidr

  azs = local.azs

  # /20 private + /24 public per AZ, carved out of the VPC CIDR.
  private_subnets = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 8, i + 48)]

  enable_nat_gateway     = true
  single_nat_gateway     = var.single_nat_gateway
  one_nat_gateway_per_az = !var.single_nat_gateway

  enable_dns_hostnames = true
  enable_dns_support   = true

  # Tag subnets so downstream resources can select them by role.
  private_subnet_tags = { Tier = "private" }
  public_subnet_tags  = { Tier = "public" }

  tags = local.common_tags
}

############################################
# Security group for VPC interface endpoints
############################################

resource "aws_security_group" "vpce" {
  count = var.enable_interface_endpoints ? 1 : 0

  name        = "${local.name}-vpce"
  description = "Allow HTTPS from within the VPC to interface endpoints"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "HTTPS from VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  # Interface endpoints only serve in-VPC clients over HTTPS and respond
  # statefully, so egress can be pinned to 443 without affecting connectivity.
  egress {
    description = "HTTPS to AWS services"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "${local.name}-vpce" })
}

############################################
# S3 Gateway endpoint (always on)
############################################

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  tags = merge(local.common_tags, { Name = "${local.name}-s3-gw" })
}

############################################
# Interface endpoints (optional)
############################################

locals {
  interface_endpoint_services = var.enable_interface_endpoints ? [
    "sts",
    "logs",
    "emr-serverless",
    # Data-plane endpoint for interactive Spark Connect sessions. With private DNS
    # enabled, in-VPC clients (MWAA workers) resolve the session endpoint
    # (*.s.emr-serverless-services.<region>.amazonaws.com) to private ENIs and reach
    # it over the VPC instead of NAT - required for the gRPC stream to stay healthy.
    "emr-serverless-services.sessions",
  ] : []
}

resource "aws_vpc_endpoint" "interface" {
  for_each = toset(local.interface_endpoint_services)

  vpc_id              = module.vpc.vpc_id
  service_name        = "com.amazonaws.${var.region}.${each.value}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = module.vpc.private_subnets
  security_group_ids  = [aws_security_group.vpce[0].id]
  private_dns_enabled = true

  tags = merge(local.common_tags, { Name = "${local.name}-${each.value}" })
}
