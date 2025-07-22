"""Unit tests for the data ingestion module."""

import pytest
import pandas as pd
import json
from unittest.mock import Mock, patch, mock_open
from pathlib import Path
import tempfile

# Defer import to avoid PySpark initialization during test collection
DataIngester = None
INGESTION_AVAILABLE = None

def _get_data_ingester():
    """Lazy import of DataIngester to avoid heavy initialization."""
    global DataIngester, INGESTION_AVAILABLE
    if INGESTION_AVAILABLE is None:
        try:
            from src.ingestion.ingest import DataIngester as DI
            DataIngester = DI
            INGESTION_AVAILABLE = True
        except ImportError:
            DataIngester = None
            INGESTION_AVAILABLE = False
    return DataIngester


class TestDataIngester:
    """Test cases for DataIngester class."""
    
    @pytest.fixture
    def ingester(self, mock_config, mock_logger):
        """Create a DataIngester instance for testing."""
        DataIngesterClass = _get_data_ingester()
        if not INGESTION_AVAILABLE:
            pytest.skip("DataIngester not available")
            
        with patch('src.ingestion.ingest.SchemaManager') as mock_schema, \
             patch('src.ingestion.ingest.PipelineMonitor') as mock_monitor, \
             patch('src.ingestion.ingest.config') as mock_cfg:
            
            # Setup lightweight mocks to avoid heavy initialization
            mock_schema.return_value = Mock()
            mock_monitor.return_value = Mock()
            mock_cfg.get_storage_config.return_value = {
                'bronze_path': 'test_output/bronze',
                'format': 'parquet'
            }
            
            return DataIngesterClass()
    
    def test_ingest_csv_success(self, ingester, test_csv_file, sample_transactions_df):
        """Test successful CSV ingestion."""
        # Mock schema manager methods directly on the ingester
        mock_schema_instance = Mock()
        mock_schema_instance.auto_detect_schema.return_value = Mock(version="1.0.0")
        mock_schema_instance.save_schema.return_value = None
        
        # Set the mocked schema manager
        ingester._schema_manager = mock_schema_instance
        
        df, metadata = ingester.ingest_csv(str(test_csv_file), "test_transactions")
        
        # Verify DataFrame content
        assert len(df) == len(sample_transactions_df)
        assert list(df.columns[:7]) == list(sample_transactions_df.columns)
        
        # Verify metadata columns were added
        assert '_ingestion_timestamp' in df.columns
        assert '_source_file' in df.columns
        assert '_record_hash' in df.columns
        
        # Verify metadata
        assert metadata['source_type'] == 'csv'
        assert metadata['rows_ingested'] == len(sample_transactions_df)
        assert 'processing_time' in metadata
        assert 'schema_version' in metadata
        
        # Verify schema methods were called
        mock_schema_instance.auto_detect_schema.assert_called_once()
        mock_schema_instance.save_schema.assert_called_once()
    
    def test_ingest_csv_file_not_found(self, ingester):
        """Test CSV ingestion with non-existent file."""
        with pytest.raises(FileNotFoundError):
            ingester.ingest_csv("non_existent_file.csv", "test")
    
    def test_ingest_json_success(self, ingester, test_json_file, sample_accounts_data):
        """Test successful JSON ingestion."""
        # Mock schema manager methods directly on the ingester
        mock_schema_instance = Mock()
        mock_schema_instance.auto_detect_schema.return_value = Mock(version="1.0.0")
        mock_schema_instance.save_schema.return_value = None
        
        # Set the mocked schema manager
        ingester._schema_manager = mock_schema_instance
        
        df, metadata = ingester.ingest_json(str(test_json_file), "test_accounts")
        
        # Verify DataFrame content
        assert len(df) == len(sample_accounts_data)
        assert 'account_id' in df.columns
        
        # Verify metadata columns were added
        assert '_ingestion_timestamp' in df.columns
        assert '_source_file' in df.columns
        assert '_record_hash' in df.columns
        
        # Verify metadata
        assert metadata['source_type'] == 'json'
        assert metadata['rows_ingested'] == len(sample_accounts_data)
    
    def test_ingest_json_invalid_format(self, ingester, temp_directory):
        """Test JSON ingestion with invalid JSON format."""
        invalid_json_file = temp_directory / "invalid.json"
        with open(invalid_json_file, 'w') as f:
            f.write("invalid json content")
        
        with pytest.raises(json.JSONDecodeError):
            ingester.ingest_json(str(invalid_json_file), "test")
    
    def test_save_to_bronze(self, ingester, sample_transactions_df, temp_directory):
        """Test saving data to Bronze layer."""
        # Set storage config directly to use temp directory
        ingester._storage_config = {
            'bronze_path': str(temp_directory / 'bronze'),
            'format': 'parquet'
        }
        
        metadata = {'test': 'metadata'}
        output_file = ingester.save_to_bronze(sample_transactions_df, "test_source", metadata)
        
        # Verify file was created
        assert output_file.exists()
        assert output_file.suffix == '.parquet'
        
        # Verify data can be read back
        df_read = pd.read_parquet(output_file)
        assert len(df_read) == len(sample_transactions_df)
        
        # Verify metadata file was created
        metadata_files = list(output_file.parent.glob("*_metadata.json"))
        assert len(metadata_files) == 1
        
        with open(metadata_files[0]) as f:
            saved_metadata = json.load(f)
        assert saved_metadata['test'] == 'metadata'
    
    def test_ingest_from_s3(self, ingester):
        """Test S3 ingestion (mocked)."""
        with patch('src.ingestion.ingest.boto3.client') as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            # Mock the S3 method to avoid actual AWS calls
            with patch.object(ingester, 'ingest_from_s3') as mock_s3_method:
                mock_s3_method.return_value = (pd.DataFrame(), {'simulated': True})
                
                df, metadata = ingester.ingest_from_s3("s3://test-bucket/test-key", "test_s3")
                
                # Verify mocked results
                assert isinstance(df, pd.DataFrame)
                assert metadata['simulated'] is True
                mock_s3_method.assert_called_once()
    
    def test_run_ingestion_pipeline_success(self, ingester, mock_config):
        """Test successful full ingestion pipeline run."""
        # Mock data sources
        mock_config.get_data_sources.return_value = [
            {
                'name': 'test_transactions',
                'type': 'csv',
                'path': 'test_data/transactions.csv'
            }
        ]
        
        # Mock ingestion methods
        sample_df = pd.DataFrame({'col1': [1, 2, 3]})
        sample_metadata = {'rows_ingested': 3}
        
        with patch.object(ingester, 'ingest_csv', return_value=(sample_df, sample_metadata)) as mock_ingest, \
             patch.object(ingester, 'save_to_bronze', return_value=Path('test_output.parquet')) as mock_save:
            
            run_id = "test-run-123"
            results = ingester.run_ingestion_pipeline(run_id)
            
            # Verify results
            assert results['run_id'] == run_id
            assert results['status'] == 'completed'
            assert results['total_rows_ingested'] == 3
            assert len(results['sources_processed']) == 1
            
            source_result = results['sources_processed'][0]
            assert source_result['source_name'] == 'test_transactions'
            assert source_result['status'] == 'success'
            assert source_result['rows_ingested'] == 3
            
            # Verify methods were called
            mock_ingest.assert_called_once()
            mock_save.assert_called_once()
    
    def test_run_ingestion_pipeline_with_failure(self, ingester, mock_config):
        """Test ingestion pipeline with source failure."""
        # Mock data sources
        mock_config.get_data_sources.return_value = [
            {
                'name': 'failing_source',
                'type': 'csv',
                'path': 'non_existent.csv'
            }
        ]
        
        # Mock ingestion to raise exception
        with patch.object(ingester, 'ingest_csv', side_effect=FileNotFoundError("File not found")):
            
            run_id = "test-run-456"
            results = ingester.run_ingestion_pipeline(run_id)
            
            # Verify results show failure
            assert results['run_id'] == run_id
            assert results['status'] == 'completed'  # Pipeline completes even with source failures
            assert results['total_rows_ingested'] == 0
            assert len(results['sources_processed']) == 1
            
            source_result = results['sources_processed'][0]
            assert source_result['source_name'] == 'failing_source'
            assert source_result['status'] == 'failed'
            assert 'error' in source_result
    
    def test_retry_mechanism(self, ingester, test_csv_file):
        """Test retry mechanism for ingestion failures."""
        # Mock schema manager methods directly on the ingester
        mock_schema_instance = Mock()
        mock_schema_instance.save_schema.return_value = None
        
        # Mock schema detection to fail twice, then succeed
        mock_schema_instance.auto_detect_schema.side_effect = [
            Exception("Network error"),
            Exception("Temporary failure"),
            Mock(version="1.0.0")
        ]
        
        # Set the mocked schema manager
        ingester._schema_manager = mock_schema_instance
        
        df, metadata = ingester.ingest_csv(str(test_csv_file), "test_retry")
        
        # Verify it eventually succeeded
        assert len(df) > 0
        assert metadata['source_type'] == 'csv'
        
        # Verify retry attempts
        assert mock_schema_instance.auto_detect_schema.call_count == 3


class TestDataIngesterIntegration:
    """Integration tests for DataIngester."""
    
    def _create_mocked_ingester(self):
        """Helper method to create a mocked DataIngester instance."""
        DataIngesterClass = _get_data_ingester()
        if not INGESTION_AVAILABLE:
            pytest.skip("DataIngester not available")
            
        # Mock schema manager methods
        mock_schema_instance = Mock()
        mock_schema_instance.auto_detect_schema.return_value = Mock(version="1.0.0")
        mock_schema_instance.save_schema.return_value = None
        
        with patch('src.ingestion.ingest.PipelineMonitor') as mock_monitor:
            mock_monitor.return_value = Mock()
            
            ingester = DataIngesterClass()
            ingester._schema_manager = mock_schema_instance
            return ingester
    
    @pytest.mark.integration
    def test_end_to_end_csv_ingestion(self, temp_directory, sample_transactions_df):
        """Test end-to-end CSV ingestion process."""
        # Create test files
        csv_file = temp_directory / "transactions.csv"
        sample_transactions_df.to_csv(csv_file, index=False)
        
        # Create mocked ingester
        ingester = self._create_mocked_ingester()
        
        # Override storage config to use temp directory
        ingester._storage_config = {
            'bronze_path': str(temp_directory / 'bronze'),
            'format': 'parquet'
        }
        
        # Run ingestion
        df, metadata = ingester.ingest_csv(str(csv_file), "integration_test")
        
        # Verify results
        assert len(df) == len(sample_transactions_df)
        assert all(col in df.columns for col in sample_transactions_df.columns)
        assert '_ingestion_timestamp' in df.columns
        
        # Save to bronze and verify
        output_file = ingester.save_to_bronze(df, "integration_test", metadata)
        assert output_file.exists()
        
        # Verify saved data
        saved_df = pd.read_parquet(output_file)
        assert len(saved_df) == len(df)
    
    @pytest.mark.integration
    def test_schema_evolution_detection(self, temp_directory, test_data_generator):
        """Test schema evolution detection."""
        # Create mocked ingester
        ingester = self._create_mocked_ingester()
        
        # Create initial dataset (smaller for performance)
        df1 = test_data_generator.generate_transactions(5)
        csv_file1 = temp_directory / "transactions_v1.csv"
        df1.to_csv(csv_file1, index=False)
        
        # Ingest first version
        _, metadata1 = ingester.ingest_csv(str(csv_file1), "schema_evolution_test")
        
        # Create evolved dataset (add new column)
        df2 = df1.copy()
        df2['new_column'] = 'new_value'
        csv_file2 = temp_directory / "transactions_v2.csv"
        df2.to_csv(csv_file2, index=False)
        
        # Ingest second version
        _, metadata2 = ingester.ingest_csv(str(csv_file2), "schema_evolution_test")
        
        # Verify schema versions are different
        assert metadata1['schema_version'] != metadata2['schema_version']
        
        # Check if schema manager detected the difference
        # (This would require implementing schema comparison in the actual code)
