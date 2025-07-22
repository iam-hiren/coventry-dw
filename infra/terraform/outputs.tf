# Terraform Outputs for Coventry DW Infrastructure

# S3 Outputs
output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "s3_bucket_domain_name" {
  description = "Domain name of the S3 bucket"
  value       = aws_s3_bucket.data_lake.bucket_domain_name
}

# RDS Outputs
output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = var.create_rds ? aws_db_instance.dw_postgres[0].endpoint : null
  sensitive   = true
}

output "rds_port" {
  description = "RDS instance port"
  value       = var.create_rds ? aws_db_instance.dw_postgres[0].port : null
}

output "rds_database_name" {
  description = "RDS database name"
  value       = var.create_rds ? aws_db_instance.dw_postgres[0].db_name : null
}

output "rds_instance_id" {
  description = "RDS instance identifier"
  value       = var.create_rds ? aws_db_instance.dw_postgres[0].identifier : null
}

# Docker Outputs
output "postgres_container_name" {
  description = "Name of the PostgreSQL Docker container"
  value       = var.use_docker ? docker_container.postgres[0].name : null
}

output "postgres_container_port" {
  description = "External port of the PostgreSQL Docker container"
  value       = var.use_docker ? var.postgres_port : null
}

output "docker_network_name" {
  description = "Name of the Docker network"
  value       = var.use_docker ? docker_network.dw_network[0].name : null
}

output "redis_container_name" {
  description = "Name of the Redis Docker container"
  value       = var.use_docker && var.enable_redis ? docker_container.redis[0].name : null
}

# IAM Outputs
output "pipeline_role_arn" {
  description = "ARN of the pipeline execution role"
  value       = var.create_iam_resources ? aws_iam_role.pipeline_role[0].arn : null
}

output "pipeline_role_name" {
  description = "Name of the pipeline execution role"
  value       = var.create_iam_resources ? aws_iam_role.pipeline_role[0].name : null
}

output "s3_access_policy_arn" {
  description = "ARN of the S3 access policy"
  value       = var.create_iam_resources ? aws_iam_policy.s3_access_policy[0].arn : null
}

# CloudWatch Outputs
output "pipeline_log_group_name" {
  description = "Name of the pipeline CloudWatch log group"
  value       = var.create_cloudwatch_resources ? aws_cloudwatch_log_group.pipeline_logs[0].name : null
}

output "application_log_group_name" {
  description = "Name of the application CloudWatch log group"
  value       = var.create_cloudwatch_resources ? aws_cloudwatch_log_group.application_logs[0].name : null
}

# Connection Strings
output "database_connection_string" {
  description = "Database connection string for applications"
  value = var.create_rds ? format("postgresql://%s:%s@%s:%s/%s",
    var.db_username,
    var.db_password,
    aws_db_instance.dw_postgres[0].endpoint,
    aws_db_instance.dw_postgres[0].port,
    aws_db_instance.dw_postgres[0].db_name
  ) : var.use_docker ? format("postgresql://%s:%s@localhost:%s/%s",
    var.db_username,
    var.db_password,
    var.postgres_port,
    var.db_name
  ) : null
  sensitive = true
}

# Environment Information
output "environment" {
  description = "Current environment"
  value       = var.environment
}

output "project_name" {
  description = "Project name"
  value       = var.project_name
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

# Infrastructure Summary
output "infrastructure_summary" {
  description = "Summary of deployed infrastructure"
  value = {
    environment    = var.environment
    project_name   = var.project_name
    aws_region     = var.aws_region
    s3_bucket     = aws_s3_bucket.data_lake.bucket
    rds_created   = var.create_rds
    docker_used   = var.use_docker
    iam_created   = var.create_iam_resources
    logs_created  = var.create_cloudwatch_resources
  }
}

# Configuration for Pipeline
output "pipeline_config" {
  description = "Configuration values for the data pipeline"
  value = {
    s3_bucket_name = aws_s3_bucket.data_lake.bucket
    database_host  = var.create_rds ? aws_db_instance.dw_postgres[0].endpoint : "localhost"
    database_port  = var.create_rds ? aws_db_instance.dw_postgres[0].port : var.postgres_port
    database_name  = var.db_name
    aws_region     = var.aws_region
    environment    = var.environment
  }
  sensitive = false
}
