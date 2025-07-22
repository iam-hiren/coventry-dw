"""Basic unit tests for the Coventry DW Pipeline core functionality."""

import pytest
import pandas as pd
import json
from pathlib import Path
import tempfile
import shutil
from datetime import datetime


class TestBasicFunctionality:
    """Test basic pipeline functionality without complex dependencies."""
    
    def test_sample_data_exists(self):
        """Test that sample data files exist and are readable."""
        # Test transactions CSV
        transactions_file = Path('data/transactions.csv')
        assert transactions_file.exists(), "Transactions CSV file should exist"
        
        df = pd.read_csv(transactions_file)
        assert len(df) > 0, "Transactions CSV should contain data"
        assert 'account_id' in df.columns, "Should have account_id column"
        assert 'amount' in df.columns, "Should have amount column"
        assert 'transaction_date' in df.columns, "Should have transaction_date column"
        
        # Test accounts JSON
        accounts_file = Path('data/accounts.json')
        assert accounts_file.exists(), "Accounts JSON file should exist"
        
        with open(accounts_file, 'r') as f:
            accounts = json.load(f)
        
        assert len(accounts) > 0, "Accounts JSON should contain data"
        assert 'account_id' in accounts[0], "Should have account_id field"
        assert 'account_type' in accounts[0], "Should have account_type field"
    
    def test_data_quality_basic(self):
        """Test basic data quality checks."""
        df = pd.read_csv('data/transactions.csv')
        
        # Check for required columns
        required_columns = ['account_id', 'transaction_id', 'amount', 'transaction_date']
        for col in required_columns:
            assert col in df.columns, f"Required column {col} should be present"
        
        # Check data types
        assert pd.api.types.is_numeric_dtype(df['amount']), "Amount should be numeric"
        
        # Check for null values in critical columns
        assert df['account_id'].notna().all(), "Account ID should not have null values"
        assert df['transaction_id'].notna().all(), "Transaction ID should not have null values"
        
        # Check date format
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        assert df['transaction_date'].notna().all(), "All dates should be parseable"
    
    def test_transaction_categorization_logic(self):
        """Test transaction categorization logic."""
        # Sample transaction descriptions
        test_descriptions = [
            "Tesco Supermarket",
            "Shell Petrol Station", 
            "Netflix Subscription",
            "ATM Withdrawal",
            "Salary Payment"
        ]
        
        # Expected categories (simplified logic)
        expected_categories = {
            "Tesco Supermarket": "Grocery",
            "Shell Petrol Station": "Fuel", 
            "Netflix Subscription": "Entertainment",
            "ATM Withdrawal": "ATM",
            "Salary Payment": "Transfer"
        }
        
        # Simple categorization function for testing
        def categorize_transaction(description):
            desc_lower = description.lower()
            if any(word in desc_lower for word in ['tesco', 'sainsbury', 'asda']):
                return 'Grocery'
            elif any(word in desc_lower for word in ['shell', 'bp', 'petrol']):
                return 'Fuel'
            elif any(word in desc_lower for word in ['netflix', 'spotify']):
                return 'Entertainment'
            elif 'atm' in desc_lower:
                return 'ATM'
            elif any(word in desc_lower for word in ['salary', 'payment']):
                return 'Transfer'
            else:
                return 'Other'
        
        # Test categorization
        for description, expected in expected_categories.items():
            result = categorize_transaction(description)
            assert result == expected, f"Expected {expected} for {description}, got {result}"
    
    def test_data_aggregation_logic(self):
        """Test data aggregation functionality."""
        df = pd.read_csv('data/transactions.csv')
        
        # Test monthly aggregation
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df['year_month'] = df['transaction_date'].dt.to_period('M')
        
        monthly_agg = df.groupby(['account_id', 'year_month']).agg({
            'amount': ['sum', 'count', 'mean']
        }).round(2)
        
        assert len(monthly_agg) > 0, "Should have monthly aggregations"
        
        # Test account-level aggregation
        account_agg = df.groupby('account_id').agg({
            'amount': ['sum', 'count', 'min', 'max']
        }).round(2)
        
        assert len(account_agg) > 0, "Should have account-level aggregations"
        
        # Verify aggregation makes sense
        total_transactions = len(df)
        sum_by_account = account_agg[('amount', 'count')].sum()
        assert sum_by_account == total_transactions, "Aggregation counts should match total"
    
    def test_file_operations(self):
        """Test file operations for pipeline."""
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Test directory creation
            bronze_path = temp_path / 'bronze'
            silver_path = temp_path / 'silver' 
            gold_path = temp_path / 'gold'
            
            for path in [bronze_path, silver_path, gold_path]:
                path.mkdir(parents=True, exist_ok=True)
                assert path.exists(), f"Directory {path} should be created"
            
            # Test file writing and reading
            test_data = {'test': 'data', 'timestamp': datetime.now().isoformat()}
            test_file = temp_path / 'test.json'
            
            with open(test_file, 'w') as f:
                json.dump(test_data, f)
            
            assert test_file.exists(), "Test file should be created"
            
            with open(test_file, 'r') as f:
                loaded_data = json.load(f)
            
            assert loaded_data['test'] == 'data', "Data should be preserved"
    
    def test_configuration_structure(self):
        """Test configuration file structure."""
        config_file = Path('config/pipeline_config.yaml')
        assert config_file.exists(), "Pipeline config file should exist"
        
        env_example = Path('.env.example')
        assert env_example.exists(), "Environment example file should exist"
        
        # Test that config contains expected sections
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        assert 'pipeline' in config, "Config should have pipeline section"
        assert 'environments' in config, "Config should have environments section"
        
        # Test environment structure
        envs = config['environments']
        assert 'development' in envs, "Should have development environment"
        
        dev_env = envs['development']
        assert 'data_sources' in dev_env, "Should have data_sources in development"
        assert 'storage' in dev_env, "Should have storage config in development"
    
    def test_schema_structure(self):
        """Test that we can create and validate basic schema structures."""
        # Sample schema structure
        schema_structure = {
            "version": "1.0.0",
            "created_at": datetime.now().isoformat(),
            "fields": [
                {
                    "name": "account_id",
                    "dtype": "object",
                    "nullable": False,
                    "description": "Account identifier"
                },
                {
                    "name": "amount", 
                    "dtype": "float64",
                    "nullable": False,
                    "description": "Transaction amount"
                }
            ]
        }
        
        # Validate schema structure
        assert 'version' in schema_structure, "Schema should have version"
        assert 'fields' in schema_structure, "Schema should have fields"
        assert len(schema_structure['fields']) > 0, "Schema should have field definitions"
        
        # Validate field structure
        for field in schema_structure['fields']:
            assert 'name' in field, "Field should have name"
            assert 'dtype' in field, "Field should have data type"
            assert 'nullable' in field, "Field should have nullable flag"
    
    def test_monitoring_data_structure(self):
        """Test monitoring data structures."""
        # Sample pipeline run record
        pipeline_run = {
            "run_id": "test-run-001",
            "pipeline_name": "test-pipeline",
            "status": "completed",
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "total_rows_processed": 1000,
                "duration_seconds": 45.5,
                "sources_processed": [
                    {"source_name": "transactions", "status": "success", "rows": 500},
                    {"source_name": "accounts", "status": "success", "rows": 500}
                ]
            }
        }
        
        # Validate structure
        assert 'run_id' in pipeline_run, "Should have run_id"
        assert 'status' in pipeline_run, "Should have status"
        assert 'metadata' in pipeline_run, "Should have metadata"
        
        metadata = pipeline_run['metadata']
        assert 'total_rows_processed' in metadata, "Should track rows processed"
        assert 'duration_seconds' in metadata, "Should track duration"
        assert 'sources_processed' in metadata, "Should track source processing"
        
        # Validate sources structure
        for source in metadata['sources_processed']:
            assert 'source_name' in source, "Source should have name"
            assert 'status' in source, "Source should have status"
            assert 'rows' in source, "Source should have row count"


class TestDataValidation:
    """Test data validation functionality."""
    
    def test_data_completeness(self):
        """Test data completeness validation."""
        df = pd.read_csv('data/transactions.csv')
        
        # Calculate completeness per column
        completeness = {}
        for column in df.columns:
            non_null_count = df[column].notna().sum()
            total_count = len(df)
            completeness[column] = non_null_count / total_count if total_count > 0 else 0
        
        # Critical columns should be 100% complete
        critical_columns = ['account_id', 'transaction_id', 'amount']
        for col in critical_columns:
            assert completeness[col] == 1.0, f"Critical column {col} should be 100% complete"
        
        # Overall completeness should be high
        overall_completeness = sum(completeness.values()) / len(completeness)
        assert overall_completeness >= 0.9, "Overall completeness should be >= 90%"
    
    def test_business_rules(self):
        """Test business rule validation."""
        df = pd.read_csv('data/transactions.csv')
        
        # Rule 1: Account IDs should follow pattern
        account_pattern = df['account_id'].str.match(r'^ACC\d{3}$')
        assert account_pattern.all(), "All account IDs should follow ACC### pattern"
        
        # Rule 2: Transaction IDs should be unique
        assert df['transaction_id'].nunique() == len(df), "Transaction IDs should be unique"
        
        # Rule 3: Amounts should be reasonable (not extremely large)
        max_amount = df['amount'].abs().max()
        assert max_amount < 100000, "Transaction amounts should be reasonable"
        
        # Rule 4: Dates should be within reasonable range
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        min_date = df['transaction_date'].min()
        max_date = df['transaction_date'].max()
        
        # Should be within last few years
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=365*5)  # 5 years ago
        assert min_date >= cutoff_date, "Dates should not be too old"
        assert max_date <= datetime.now(), "Dates should not be in future"


# Pytest configuration for this test file
def pytest_configure(config):
    """Configure pytest markers for this test file."""
    config.addinivalue_line("markers", "basic: basic functionality tests")
    config.addinivalue_line("markers", "validation: data validation tests")


# Mark all tests in TestBasicFunctionality as basic tests
pytestmark = pytest.mark.basic
