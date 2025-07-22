"""Pytest configuration and fixtures for Coventry DW Pipeline tests."""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
import os
import json
from datetime import datetime

# Test data fixtures
@pytest.fixture
def sample_transactions_df():
    """Sample transactions DataFrame for testing."""
    return pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC001', 'ACC003'],
        'transaction_id': ['TXN001', 'TXN002', 'TXN003', 'TXN004'],
        'transaction_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18'],
        'amount': [1250.00, -45.99, -12.50, 950.00],
        'description': ['Salary Payment', 'Tesco Supermarket', 'Costa Coffee', 'Freelance Payment'],
        'transaction_type': ['CREDIT', 'DEBIT', 'DEBIT', 'CREDIT'],
        'balance_after': [3450.50, 3404.51, 3392.01, 1876.23]
    })

@pytest.fixture
def sample_accounts_data():
    """Sample accounts data for testing."""
    return [
        {
            "account_id": "ACC001",
            "customer_id": "CUST001",
            "account_type": "CURRENT",
            "account_name": "Main Current Account",
            "opening_date": "2020-03-15",
            "current_balance": 3159.53,
            "overdraft_limit": 1000.00,
            "interest_rate": 0.01,
            "status": "ACTIVE",
            "branch_code": "COV001",
            "sort_code": "12-34-56"
        },
        {
            "account_id": "ACC002",
            "customer_id": "CUST002",
            "account_type": "SAVINGS",
            "account_name": "High Interest Savings",
            "opening_date": "2021-01-10",
            "current_balance": 5234.78,
            "overdraft_limit": 0.00,
            "interest_rate": 0.025,
            "status": "ACTIVE",
            "branch_code": "COV001",
            "sort_code": "12-34-56"
        }
    ]

@pytest.fixture
def temp_directory():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def test_csv_file(sample_transactions_df, temp_directory):
    """Create a temporary CSV file with sample data."""
    csv_file = temp_directory / "test_transactions.csv"
    sample_transactions_df.to_csv(csv_file, index=False)
    return csv_file

@pytest.fixture
def test_json_file(sample_accounts_data, temp_directory):
    """Create a temporary JSON file with sample data."""
    json_file = temp_directory / "test_accounts.json"
    with open(json_file, 'w') as f:
        json.dump(sample_accounts_data, f, indent=2)
    return json_file

@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    with patch('src.utils.config.config') as mock_cfg:
        mock_cfg.get_storage_config.return_value = {
            'bronze_path': 'test_output/bronze',
            'silver_path': 'test_output/silver',
            'gold_path': 'test_output/gold',
            'quarantine_path': 'test_output/quarantine',
            'format': 'parquet'
        }
        mock_cfg.get_data_sources.return_value = [
            {
                'name': 'test_transactions',
                'type': 'csv',
                'path': 'test_data/transactions.csv'
            }
        ]
        mock_cfg.get_data_quality_config.return_value = {
            'enable_validation': True,
            'coverage_threshold': 0.95,
            'fail_on_error': False,
            'rules': [
                {
                    'name': 'amount_not_null',
                    'column': 'amount',
                    'check': 'not_null'
                }
            ]
        }
        mock_cfg.base_config.environment = 'test'
        mock_cfg.base_config.pipeline_name = 'test-pipeline'
        mock_cfg.base_config.log_level = 'INFO'
        yield mock_cfg

@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch('src.utils.logger.get_logger') as mock_get_logger:
        mock_logger_instance = Mock()
        mock_get_logger.return_value = mock_logger_instance
        yield mock_logger_instance

@pytest.fixture
def test_schema_data():
    """Sample schema data for testing."""
    return {
        "version": "1.0.0",
        "created_at": "2024-01-01T00:00:00Z",
        "fields": [
            {
                "name": "account_id",
                "dtype": "object",
                "nullable": False,
                "description": "Account identifier",
                "constraints": {"max_length": 10}
            },
            {
                "name": "amount",
                "dtype": "float64",
                "nullable": False,
                "description": "Transaction amount",
                "constraints": {"min_value": -10000, "max_value": 10000}
            }
        ],
        "metadata": {
            "auto_detected": True,
            "sample_rows": 100
        }
    }

@pytest.fixture
def mock_database():
    """Mock database connection for testing."""
    with patch('sqlalchemy.create_engine') as mock_engine:
        mock_conn = Mock()
        mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        yield mock_conn

@pytest.fixture
def test_environment_vars():
    """Set up test environment variables."""
    test_env = {
        'ENVIRONMENT': 'test',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test_db',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_pass',
        'LOG_LEVEL': 'DEBUG'
    }
    
    # Store original values
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value
    
    yield test_env
    
    # Restore original values
    for key, original_value in original_env.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value

@pytest.fixture
def sample_pipeline_results():
    """Sample pipeline execution results for testing."""
    return {
        "run_id": "test-run-123",
        "pipeline_name": "test-pipeline",
        "start_time": "2024-01-01T10:00:00Z",
        "end_time": "2024-01-01T10:05:00Z",
        "duration_seconds": 300,
        "status": "completed",
        "total_rows_processed": 1000,
        "stages": {
            "ingestion": {
                "status": "completed",
                "sources_processed": [
                    {
                        "source_name": "transactions",
                        "rows_ingested": 500,
                        "status": "success"
                    }
                ]
            },
            "transformation": {
                "status": "completed",
                "files_processed": [
                    {
                        "source_name": "transactions",
                        "original_rows": 500,
                        "final_rows": 480,
                        "status": "success"
                    }
                ]
            }
        }
    }

@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing."""
    with patch('boto3.client') as mock_boto3:
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        yield mock_client

@pytest.fixture
def sample_validation_results():
    """Sample data validation results for testing."""
    return {
        "source_name": "test_transactions",
        "validation_timestamp": "2024-01-01T10:00:00Z",
        "total_rows": 100,
        "passed": True,
        "overall_score": 0.95,
        "quarantined_rows": 0,
        "checks": {
            "schema_validation": {
                "passed": True,
                "score": 1.0,
                "check_type": "schema_validation"
            },
            "business_rules": {
                "passed": True,
                "score": 0.95,
                "check_type": "business_rules"
            },
            "completeness": {
                "passed": True,
                "score": 0.98,
                "check_type": "completeness"
            }
        }
    }

# Test utilities
class TestDataGenerator:
    """Utility class for generating test data."""
    
    @staticmethod
    def generate_transactions(num_rows: int = 100) -> pd.DataFrame:
        """Generate synthetic transaction data."""
        import random
        from datetime import datetime, timedelta
        
        accounts = ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005']
        descriptions = [
            'Salary Payment', 'Tesco Supermarket', 'Costa Coffee',
            'Shell Petrol Station', 'ATM Withdrawal', 'Amazon Purchase',
            'Netflix Subscription', 'Electricity Bill'
        ]
        
        data = []
        for i in range(num_rows):
            data.append({
                'account_id': random.choice(accounts),
                'transaction_id': f'TXN{i+1:06d}',
                'transaction_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                'amount': round(random.uniform(-500, 2000), 2),
                'description': random.choice(descriptions),
                'transaction_type': random.choice(['CREDIT', 'DEBIT']),
                'balance_after': round(random.uniform(0, 10000), 2)
            })
        
        return pd.DataFrame(data)
    
    @staticmethod
    def generate_accounts(num_accounts: int = 10) -> list:
        """Generate synthetic account data."""
        import random
        
        account_types = ['CURRENT', 'SAVINGS', 'PREMIUM']
        statuses = ['ACTIVE', 'INACTIVE', 'CLOSED']
        
        accounts = []
        for i in range(num_accounts):
            accounts.append({
                'account_id': f'ACC{i+1:03d}',
                'customer_id': f'CUST{i+1:03d}',
                'account_type': random.choice(account_types),
                'account_name': f'Account {i+1}',
                'opening_date': (datetime.now() - timedelta(days=random.randint(365, 1825))).strftime('%Y-%m-%d'),
                'current_balance': round(random.uniform(0, 50000), 2),
                'overdraft_limit': random.choice([0, 500, 1000, 2000, 5000]),
                'interest_rate': round(random.uniform(0.001, 0.05), 3),
                'status': random.choice(statuses),
                'branch_code': f'COV{random.randint(1, 5):03d}',
                'sort_code': f'12-34-{random.randint(50, 99)}'
            })
        
        return accounts

@pytest.fixture
def test_data_generator():
    """Test data generator fixture."""
    return TestDataGenerator()

# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "database: mark test as requiring database"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Add unit marker to tests in unit directory
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        
        # Add integration marker to tests in integration directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Add database marker to tests that use database fixtures
        if "mock_database" in item.fixturenames or "test_database" in item.fixturenames:
            item.add_marker(pytest.mark.database)
