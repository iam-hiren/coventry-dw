#!/bin/bash
set -e

# Coventry Building Society Data Warehouse Pipeline
# Docker Entrypoint Script

echo "Starting Coventry DW Pipeline..."
echo "Environment: ${ENVIRONMENT:-development}"
echo "Python Path: ${PYTHONPATH}"

# Wait for database to be ready
if [ -n "$DB_HOST" ]; then
    echo "Waiting for database at $DB_HOST:$DB_PORT..."
    while ! nc -z "$DB_HOST" "$DB_PORT"; do
        echo "Database not ready, waiting..."
        sleep 2
    done
    echo "Database is ready!"
fi

# Wait for LocalStack (if using)
if [ "$ENVIRONMENT" = "development" ] && [ -n "$LOCALSTACK_ENDPOINT" ]; then
    echo "Waiting for LocalStack..."
    while ! curl -s "$LOCALSTACK_ENDPOINT/health" > /dev/null; do
        echo "LocalStack not ready, waiting..."
        sleep 2
    done
    echo "LocalStack is ready!"
fi

# Create necessary directories
mkdir -p /app/logs /app/output /app/data /app/schemas

# Set up logging directory permissions
if [ -w /app/logs ]; then
    echo "Logs directory is writable"
else
    echo "Warning: Logs directory is not writable"
fi

# Initialize database schema (if needed)
if [ "$INIT_DB" = "true" ]; then
    echo "Initializing database schema..."
    python -c "
from src.utils.config import config
from sqlalchemy import create_engine
import logging

try:
    engine = create_engine(config.base_config.db.connection_string)
    with engine.connect() as conn:
        conn.execute('SELECT 1')
    print('Database connection successful')
except Exception as e:
    print(f'Database connection failed: {e}')
    exit(1)
"
fi

# Run database migrations (if any)
if [ -f "/app/migrations/run_migrations.py" ]; then
    echo "Running database migrations..."
    python /app/migrations/run_migrations.py
fi

# Validate configuration
echo "Validating configuration..."
python -c "
from src.utils.config import config
print(f'Environment: {config.base_config.environment}')
print(f'Log Level: {config.base_config.log_level}')
print(f'Pipeline Name: {config.base_config.pipeline_name}')
print('Configuration validation successful')
"

# Set up AWS credentials for LocalStack (development)
if [ "$ENVIRONMENT" = "development" ]; then
    export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
    export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
    export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-eu-west-2}
    echo "AWS credentials set for development"
fi

# Health check function
health_check() {
    python -c "
import sys
from src.utils.config import config
from src.monitoring import PipelineMonitor

try:
    monitor = PipelineMonitor()
    health = monitor.get_pipeline_health()
    print(f'Pipeline health: {health[\"status\"]}')
    sys.exit(0 if health['status'] != 'error' else 1)
except Exception as e:
    print(f'Health check failed: {e}')
    sys.exit(1)
"
}

# Handle different run modes
case "${1:-}" in
    "health")
        echo "Running health check..."
        health_check
        ;;
    "test")
        echo "Running tests..."
        python -m pytest tests/ -v
        ;;
    "lint")
        echo "Running linters..."
        flake8 src/
        black --check src/
        ;;
    "format")
        echo "Formatting code..."
        black src/
        isort src/
        ;;
    "shell")
        echo "Starting interactive shell..."
        exec /bin/bash
        ;;
    "jupyter")
        echo "Starting Jupyter Lab..."
        exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
        ;;
    *)
        echo "Starting pipeline with command: $*"
        exec "$@"
        ;;
esac
