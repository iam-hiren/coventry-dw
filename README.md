# Coventry Building Society Data Warehouse Pipeline

A comprehensive, production-ready data pipeline implementing modern DataOps practices for financial services. This pipeline follows the Medallion Architecture (Bronze/Silver/Gold) and includes full CI/CD, monitoring, and compliance features.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Bronze Layer  │    │  Silver Layer   │
│                 │    │                 │    │                 │
│ • CSV Files     │───▶│ • Raw Data      │───▶│ • Cleaned Data  │
│ • JSON Files    │    │ • Schema Stored │    │ • Validated     │
│ • S3 Objects    │    │ • Partitioned   │    │ • Enriched      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             │
│  Quarantine     │    │   Gold Layer    │◀────────────┘
│                 │    │                 │
│ • Failed Data   │    │ • Aggregations  │
│ • Quality Issues│    │ • Business KPIs │
│ • Audit Trail   │    │ • ML Features   │
└─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- PostgreSQL (or use Docker)
- Git

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/iam-hiren/coventry-dw.git
   cd coventry-dw
   ```

2. **Set up environment:**
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit .env with your configuration
   # Set database credentials, AWS keys, etc.
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start infrastructure (Docker):**
   ```bash
   docker-compose up -d postgres redis localstack
   ```

5. **Run the pipeline:**
   ```bash
   python main.py --mode full
   ```

## 📋 Features

### ✅ Data Pipeline
- **Medallion Architecture**: Bronze → Silver → Gold layers
- **Schema Evolution**: Automatic detection and versioning
- **Data Quality**: Comprehensive validation with Pandera
- **Error Handling**: Retry logic and quarantine for bad data
- **Partitioning**: Year/month partitioning for performance

### ✅ Infrastructure as Code
- **Terraform**: AWS resources (S3, RDS, IAM, CloudWatch)
- **Docker**: Containerized pipeline with multi-stage builds
- **Local Development**: Docker Compose with all services

### ✅ CI/CD Pipeline
- **GitHub Actions**: Automated testing and deployment
- **Code Quality**: Black, Flake8, isort, MyPy, Bandit
- **Testing**: Unit, integration, and performance tests
- **Security**: Vulnerability scanning with Trivy

### ✅ Monitoring & Alerting
- **Structured Logging**: JSON logs with correlation IDs
- **Metrics**: Pipeline performance and data quality metrics
- **Alerts**: Email and Slack notifications
- **Health Checks**: Pipeline status monitoring

### ✅ Security & Compliance
- **Secrets Management**: Environment-based configuration
- **Encryption**: Data at rest and in transit
- **Audit Trail**: Complete lineage tracking
- **Access Control**: Role-based permissions

## 🎯 Usage

### Running the Pipeline

```bash
# Full pipeline execution
python main.py --mode full

# Individual stages
python main.py --mode ingestion
python main.py --mode transformation

# Check pipeline status
python main.py --mode status

# Schedule execution
python main.py --schedule daily

# Cleanup old data
python main.py --cleanup --days 30
```

### Using Docker

```bash
# Build and run pipeline
docker-compose up pipeline

# Run specific commands
docker-compose exec pipeline python main.py --mode full

# Access Jupyter for analysis
docker-compose up jupyter
# Navigate to http://localhost:8888 (token: coventry123)

# Database management
docker-compose up pgadmin
# Navigate to http://localhost:5050
```

### Configuration

The pipeline uses a hierarchical configuration system:

1. **Environment Variables** (`.env` file)
2. **YAML Configuration** (`config/pipeline_config.yaml`)
3. **Command Line Arguments**

Key configuration sections:
- **Data Sources**: Input file paths and formats
- **Storage**: Output paths and partitioning
- **Data Quality**: Validation rules and thresholds
- **Monitoring**: Alerting and logging settings

## 📊 Data Quality Framework

### Validation Layers

1. **Schema Validation**: Structure and data types
2. **Business Rules**: Domain-specific constraints
3. **Completeness**: Missing value detection
4. **Consistency**: Cross-field validation
5. **Freshness**: Data recency checks

### Quality Metrics

- **Coverage Threshold**: 95% (configurable)
- **Completeness Score**: Per-column analysis
- **Consistency Checks**: Duplicate detection
- **Freshness Alerts**: Data age monitoring

## 🏢 Financial Services Compliance

### Regulatory Features

- **Data Lineage**: Complete audit trail
- **Retention Policies**: Automated data lifecycle
- **Access Logging**: User activity tracking
- **Encryption**: AES-256 for sensitive data
- **Backup Strategy**: Point-in-time recovery

### Risk Management

- **Data Quarantine**: Isolate problematic data
- **Rollback Capability**: Version-based recovery
- **Monitoring**: Real-time anomaly detection
- **Alerting**: Immediate notification of issues

## 🧪 Testing

### Test Structure

```
tests/
├── unit/           # Unit tests for individual components
├── integration/    # End-to-end integration tests
├── performance/    # Load and performance tests
└── conftest.py     # Shared fixtures and configuration
```

### Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# With coverage
pytest --cov=src --cov-report=html

# Performance tests
pytest tests/performance/ -m slow
```

### Test Coverage

Target: **>80% code coverage**

Current coverage includes:
- Data ingestion and transformation
- Schema management and validation
- Data quality checks
- Pipeline orchestration
- Error handling and recovery

## 📈 Monitoring & Observability

### Metrics Collected

- **Pipeline Metrics**: Runtime, throughput, success rate
- **Data Metrics**: Row counts, quality scores, schema changes
- **System Metrics**: Memory usage, disk space, CPU
- **Business Metrics**: Transaction volumes, account activity

### Dashboards

- **Grafana**: Real-time monitoring dashboards
- **Pipeline Status**: Current and historical runs
- **Data Quality**: Quality trends and alerts
- **Performance**: Processing times and bottlenecks

### Alerting Rules

- **Pipeline Failures**: Immediate notification
- **Data Quality Issues**: Threshold-based alerts
- **Performance Degradation**: SLA monitoring
- **System Health**: Resource utilization

## 🚀 Deployment

### Environments

1. **Development**: Local Docker environment
2. **Staging**: AWS with reduced resources
3. **Production**: Full AWS deployment with HA

### Infrastructure

```bash
# Deploy infrastructure
cd infra/terraform
terraform init
terraform plan -var-file="environments/production.tfvars"
terraform apply
```

### CI/CD Pipeline

The GitHub Actions workflow includes:

1. **Code Quality**: Linting and formatting
2. **Testing**: Unit, integration, and security tests
3. **Build**: Docker image creation
4. **Deploy**: Automated deployment to staging
5. **Monitoring**: Post-deployment health checks

## 📁 Project Structure

```
coventry-dw/
├── src/                    # Source code
│   ├── ingestion/         # Data ingestion (Bronze)
│   ├── transformation/    # Data transformation (Silver/Gold)
│   ├── schema/           # Schema management
│   ├── data_quality/     # Data validation
│   ├── monitoring/       # Monitoring and alerting
│   ├── orchestrator/     # Pipeline orchestration
│   └── utils/            # Utilities and configuration
├── tests/                 # Test suite
├── config/               # Configuration files
├── data/                 # Sample data
├── schemas/              # Schema definitions
├── infra/                # Infrastructure as Code
│   ├── terraform/        # AWS resources
│   ├── docker/          # Container configurations
│   └── sql/             # Database scripts
├── .github/              # CI/CD workflows
├── docs/                 # Documentation
├── requirements.txt      # Python dependencies
├── docker-compose.yml    # Local development
├── Dockerfile           # Container definition
└── main.py              # Main entry point
```

## 🤝 Contributing

### Development Setup

1. **Fork and clone** the repository
2. **Create virtual environment**: `python -m venv venv`
3. **Install dependencies**: `pip install -r requirements.txt`
4. **Install pre-commit hooks**: `pre-commit install`
5. **Run tests**: `pytest`

### Code Standards

- **Python**: PEP 8 with Black formatting
- **Line Length**: 88 characters
- **Type Hints**: Required for public functions
- **Documentation**: Docstrings for all modules and classes
- **Testing**: Minimum 80% coverage for new code

### Pull Request Process

1. Create feature branch from `develop`
2. Implement changes with tests
3. Run full test suite
4. Update documentation
5. Submit PR with detailed description

### Troubleshooting

Common issues and solutions:

1. **Database Connection**: Check credentials and network
2. **Permission Errors**: Verify file system permissions
3. **Memory Issues**: Adjust batch sizes in configuration
4. **Schema Conflicts**: Review schema evolution logs