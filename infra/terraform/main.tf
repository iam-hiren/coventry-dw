# Coventry Building Society Data Warehouse Infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

# Configure AWS Provider
provider "aws" {
  region = var.aws_region
  
  # For local development with LocalStack
  endpoints {
    s3  = var.localstack_endpoint
    rds = var.localstack_endpoint
  }
  
  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack
}

# Configure Docker Provider
provider "docker" {
  host = "npipe:////.//pipe//docker_engine"  # Windows Docker Desktop
}

# Local variables
locals {
  common_tags = {
    Project     = "coventry-dw"
    Environment = var.environment
    Owner       = "DataOps Team"
    CreatedBy   = "Terraform"
  }
  
  s3_bucket_name = "${var.project_name}-data-lake-${var.environment}"
}

# S3 Bucket for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = local.s3_bucket_name
  
  tags = merge(local.common_tags, {
    Name = "Coventry DW Data Lake"
    Type = "Storage"
  })
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Server Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "data_lake_pab" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze_layer_lifecycle"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 2555  # 7 years retention for financial data
    }
  }

  rule {
    id     = "silver_layer_lifecycle"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "gold_layer_lifecycle"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    transition {
      days          = 365
      storage_class = "STANDARD_IA"
    }
  }
}

# RDS Subnet Group
resource "aws_db_subnet_group" "dw_subnet_group" {
  count      = var.create_rds ? 1 : 0
  name       = "${var.project_name}-db-subnet-group"
  subnet_ids = var.subnet_ids

  tags = merge(local.common_tags, {
    Name = "DW DB Subnet Group"
  })
}

# RDS Instance for Data Warehouse
resource "aws_db_instance" "dw_postgres" {
  count = var.create_rds ? 1 : 0
  
  identifier = "${var.project_name}-postgres-${var.environment}"
  
  # Engine configuration
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = var.db_instance_class
  
  # Storage configuration
  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_max_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true
  
  # Database configuration
  db_name  = var.db_name
  username = var.db_username
  password = var.db_password
  port     = 5432
  
  # Network configuration
  db_subnet_group_name   = aws_db_subnet_group.dw_subnet_group[0].name
  vpc_security_group_ids = var.security_group_ids
  publicly_accessible    = false
  
  # Backup configuration
  backup_retention_period = 30
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  # Monitoring
  monitoring_interval = 60
  monitoring_role_arn = var.rds_monitoring_role_arn
  
  # Performance Insights
  performance_insights_enabled = true
  performance_insights_retention_period = 7
  
  # Deletion protection
  deletion_protection = var.environment == "production"
  skip_final_snapshot = var.environment != "production"
  
  tags = merge(local.common_tags, {
    Name = "Coventry DW PostgreSQL"
    Type = "Database"
  })
}

# Docker Network for local development
resource "docker_network" "dw_network" {
  count = var.use_docker ? 1 : 0
  name  = "coventry-dw-network"
}

# PostgreSQL Docker Container for local development
resource "docker_image" "postgres" {
  count = var.use_docker ? 1 : 0
  name  = "postgres:15-alpine"
}

resource "docker_container" "postgres" {
  count = var.use_docker ? 1 : 0
  
  image = docker_image.postgres[0].image_id
  name  = "coventry-dw-postgres"
  
  ports {
    internal = 5432
    external = var.postgres_port
  }
  
  env = [
    "POSTGRES_DB=${var.db_name}",
    "POSTGRES_USER=${var.db_username}",
    "POSTGRES_PASSWORD=${var.db_password}",
    "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256"
  ]
  
  volumes {
    host_path      = "${path.cwd}/data/postgres"
    container_path = "/var/lib/postgresql/data"
  }
  
  networks_advanced {
    name = docker_network.dw_network[0].name
  }
  
  restart = "unless-stopped"
}

# Redis Docker Container for caching (optional)
resource "docker_image" "redis" {
  count = var.use_docker && var.enable_redis ? 1 : 0
  name  = "redis:7-alpine"
}

resource "docker_container" "redis" {
  count = var.use_docker && var.enable_redis ? 1 : 0
  
  image = docker_image.redis[0].image_id
  name  = "coventry-dw-redis"
  
  ports {
    internal = 6379
    external = 6379
  }
  
  networks_advanced {
    name = docker_network.dw_network[0].name
  }
  
  restart = "unless-stopped"
}

# IAM Role for Pipeline Execution
resource "aws_iam_role" "pipeline_role" {
  count = var.create_iam_resources ? 1 : 0
  name  = "${var.project_name}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["ec2.amazonaws.com", "ecs-tasks.amazonaws.com"]
        }
      }
    ]
  })

  tags = local.common_tags
}

# IAM Policy for S3 Access
resource "aws_iam_policy" "s3_access_policy" {
  count = var.create_iam_resources ? 1 : 0
  name  = "${var.project_name}-s3-access-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })

  tags = local.common_tags
}

# Attach S3 Policy to Pipeline Role
resource "aws_iam_role_policy_attachment" "pipeline_s3_access" {
  count      = var.create_iam_resources ? 1 : 0
  role       = aws_iam_role.pipeline_role[0].name
  policy_arn = aws_iam_policy.s3_access_policy[0].arn
}

# CloudWatch Log Group for Pipeline Logs
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  count             = var.create_cloudwatch_resources ? 1 : 0
  name              = "/aws/coventry-dw/pipeline"
  retention_in_days = var.log_retention_days

  tags = merge(local.common_tags, {
    Name = "Pipeline Logs"
    Type = "Logging"
  })
}

# CloudWatch Log Group for Application Logs
resource "aws_cloudwatch_log_group" "application_logs" {
  count             = var.create_cloudwatch_resources ? 1 : 0
  name              = "/aws/coventry-dw/application"
  retention_in_days = var.log_retention_days

  tags = merge(local.common_tags, {
    Name = "Application Logs"
    Type = "Logging"
  })
}
