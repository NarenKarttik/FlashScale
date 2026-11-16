# ==============================================================================
# FlashScale: Production Cloud Infrastructure Definition (AWS / Terraform)
# High-Availability Multi-AZ Architecture
# ==============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

# ------------------------------------------------------------------------------
# 1. Virtual Private Cloud (VPC) with Multi-AZ Subnets
# ------------------------------------------------------------------------------
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.5.0"

  name = "flashscale-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["us-east-1a", "us-east-1b", "us-east-1c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = false # Multi-AZ NAT for FAANG-level fault tolerance
  enable_dns_hostnames = true
}

# ------------------------------------------------------------------------------
# 2. Amazon EKS Kubernetes Cluster
# ------------------------------------------------------------------------------
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.2.0"

  cluster_name    = "flashscale-eks-cluster"
  cluster_version = "1.29"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    general_nodes = {
      instance_types = ["m6i.xlarge"] # 4 vCPU, 16GB RAM for high JVM throughput
      min_size       = 3
      max_size       = 15
      desired_size   = 3
    }
  }
}

# ------------------------------------------------------------------------------
# 3. Amazon ElastiCache Redis Cluster (Multi-AZ with Auto-Failover)
# ------------------------------------------------------------------------------
resource "aws_elasticache_subnet_group" "redis_subnets" {
  name       = "flashscale-redis-subnet-group"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_replication_group" "redis_cluster" {
  replication_group_id          = "flashscale-redis-cluster"
  description                   = "High throughput atomic reservation & rate limiting cluster"
  node_type                     = "cache.m6g.large"
  num_cache_clusters            = 3 # 1 primary + 2 read replicas across 3 AZs
  parameter_group_name          = "default.redis7"
  port                          = 6379
  subnet_group_name             = aws_elasticache_subnet_group.redis_subnets.name
  automatic_failover_enabled    = true
  multi_az_enabled              = true
  at_rest_encryption_enabled    = true
  transit_encryption_enabled    = true
}

# ------------------------------------------------------------------------------
# 4. Amazon RDS PostgreSQL with Multi-AZ & Read Replica
# ------------------------------------------------------------------------------
resource "aws_db_subnet_group" "rds_subnets" {
  name       = "flashscale-db-subnet-group"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_db_instance" "postgres_primary" {
  identifier             = "flashscale-pg-primary"
  engine                 = "postgres"
  engine_version         = "16.1"
  instance_class         = "db.r6g.xlarge" # Memory-optimized
  allocated_storage      = 100
  max_allocated_storage  = 1000 # Storage autoscaling
  db_name                = "flashscale_db"
  username               = "flashscale_admin"
  password               = "vault_managed_password"
  multi_az               = true # Synchronous standby in another AZ
  db_subnet_group_name   = aws_db_subnet_group.rds_subnets.name
  skip_final_snapshot    = true
}
