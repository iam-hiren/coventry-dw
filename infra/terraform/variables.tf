# Terraform Variables for Coventry DW Infrastructure

# General Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "coventry-dw"
}

variable "environment" {
  description = "Environment name (development, staging, production)"
  type        = string
  default     = "development"
  
  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-2"
}

# LocalStack Configuration (for local development)
variable "use_localstack" {
  description = "Use LocalStack for local AWS services"
  type        = bool
  default     = true
}

variable "localstack_endpoint" {
  description = "LocalStack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

# Docker Configuration
variable "use_docker" {
  description = "Deploy Docker containers for local development"
  type        = bool
  default     = true
}

variable "postgres_port" {
  description = "External port for PostgreSQL container"
  type        = number
  default     = 5432
}

variable "enable_redis" {
  description = "Enable Redis container for caching"
  type        = bool
  default     = false
}

# Database Configuration
variable "create_rds" {
  description = "Create RDS PostgreSQL instance"
  type        = bool
  default     = false
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "Initial allocated storage for RDS (GB)"
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "Maximum allocated storage for RDS (GB)"
  type        = number
  default     = 100
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "coventry_dw"
}

variable "db_username" {
  description = "Database username"
  type        = string
  default     = "postgres"
  sensitive   = true
}

variable "db_password" {
  description = "Database password"
  type        = string
  default     = "postgres123"
  sensitive   = true
}

variable "subnet_ids" {
  description = "List of subnet IDs for RDS"
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "List of security group IDs for RDS"
  type        = list(string)
  default     = []
}

variable "rds_monitoring_role_arn" {
  description = "ARN of IAM role for RDS monitoring"
  type        = string
  default     = ""
}

# IAM Configuration
variable "create_iam_resources" {
  description = "Create IAM roles and policies"
  type        = bool
  default     = false
}

# CloudWatch Configuration
variable "create_cloudwatch_resources" {
  description = "Create CloudWatch log groups"
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

# S3 Configuration
variable "enable_s3_lifecycle" {
  description = "Enable S3 lifecycle policies"
  type        = bool
  default     = true
}

# Backup Configuration
variable "backup_retention_days" {
  description = "Number of days to retain backups"
  type        = number
  default     = 30
}

# Monitoring Configuration
variable "enable_monitoring" {
  description = "Enable enhanced monitoring"
  type        = bool
  default     = true
}

# Network Configuration
variable "vpc_id" {
  description = "VPC ID for resources"
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs"
  type        = list(string)
  default     = []
}

variable "public_subnet_ids" {
  description = "List of public subnet IDs"
  type        = list(string)
  default     = []
}

# Security Configuration
variable "enable_encryption" {
  description = "Enable encryption for storage and data in transit"
  type        = bool
  default     = true
}

variable "kms_key_id" {
  description = "KMS key ID for encryption"
  type        = string
  default     = ""
}

# Cost Optimization
variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

# Disaster Recovery
variable "enable_multi_az" {
  description = "Enable Multi-AZ deployment for RDS"
  type        = bool
  default     = false
}

variable "enable_cross_region_backup" {
  description = "Enable cross-region backup"
  type        = bool
  default     = false
}

# Compliance
variable "enable_compliance_logging" {
  description = "Enable compliance and audit logging"
  type        = bool
  default     = true
}

# Development vs Production Settings
variable "development_settings" {
  description = "Map of development-specific settings"
  type = object({
    skip_final_snapshot = bool
    deletion_protection = bool
    backup_retention    = number
  })
  default = {
    skip_final_snapshot = true
    deletion_protection = false
    backup_retention    = 7
  }
}

variable "production_settings" {
  description = "Map of production-specific settings"
  type = object({
    skip_final_snapshot = bool
    deletion_protection = bool
    backup_retention    = number
  })
  default = {
    skip_final_snapshot = false
    deletion_protection = true
    backup_retention    = 30
  }
}
