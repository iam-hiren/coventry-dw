"""Data ingestion module for the Coventry DW pipeline (Bronze layer)."""

import pandas as pd
import json
import boto3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from moto import mock_s3

from ..utils import get_logger, config
from ..monitoring import PipelineMonitor

# Lazy import to avoid heavy PySpark initialization
SchemaManager = None

def _get_schema_manager():
    """Lazy import of SchemaManager to avoid PySpark during test collection."""
    global SchemaManager
    if SchemaManager is None:
        from ..schema import SchemaManager as SM
        SchemaManager = SM
    return SchemaManager

logger = get_logger(__name__)


class DataIngester:
    """Handles data ingestion from various sources to Bronze layer."""
    
    def __init__(self):
        self._schema_manager = None
        self._monitor = None
        self._storage_config = None
    
    @property
    def schema_manager(self):
        """Lazy initialization of schema manager."""
        if self._schema_manager is None:
            SchemaManagerClass = _get_schema_manager()
            self._schema_manager = SchemaManagerClass()
        return self._schema_manager
    
    @property
    def monitor(self):
        """Lazy initialization of pipeline monitor."""
        if self._monitor is None:
            self._monitor = PipelineMonitor()
        return self._monitor
    
    @property
    def storage_config(self):
        """Lazy initialization of storage config."""
        if self._storage_config is None:
            self._storage_config = config.get_storage_config()
        return self._storage_config
    
    @storage_config.setter
    def storage_config(self, value):
        """Allow setting storage config for testing."""
        self._storage_config = value
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def ingest_csv(self, file_path: str, source_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Ingest data from CSV file."""
        logger.info(f"Ingesting CSV data from: {file_path}", source_name=source_name)
        
        start_time = datetime.utcnow()
        
        try:
            # Read CSV with error handling
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Auto-detect and save schema
            schema_version = self.schema_manager.auto_detect_schema(df, source_name)
            self.schema_manager.save_schema(source_name, schema_version)
            
            # Add metadata columns
            df['_ingestion_timestamp'] = datetime.utcnow()
            df['_source_file'] = file_path
            df['_record_hash'] = df.apply(lambda row: hash(tuple(row.astype(str))), axis=1)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            metadata = {
                "source_type": "csv",
                "source_path": file_path,
                "rows_ingested": len(df),
                "columns": list(df.columns),
                "processing_time": processing_time,
                "schema_version": schema_version.version,
                "ingestion_timestamp": datetime.utcnow().isoformat()
            }
            
            logger.log_data_processing(
                stage="csv_ingestion",
                input_rows=len(df),
                output_rows=len(df),
                processing_time=processing_time,
                source_name=source_name
            )
            
            return df, metadata
            
        except Exception as e:
            logger.error(f"Failed to ingest CSV: {file_path}", error=str(e), source_name=source_name)
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def ingest_json(self, file_path: str, source_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Ingest data from JSON file."""
        logger.info(f"Ingesting JSON data from: {file_path}", source_name=source_name)
        
        start_time = datetime.utcnow()
        
        try:
            # Read JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError("JSON data must be a list or dictionary")
            
            # Auto-detect and save schema
            schema_version = self.schema_manager.auto_detect_schema(df, source_name)
            self.schema_manager.save_schema(source_name, schema_version)
            
            # Add metadata columns
            df['_ingestion_timestamp'] = datetime.utcnow()
            df['_source_file'] = file_path
            df['_record_hash'] = df.apply(lambda row: hash(tuple(row.astype(str))), axis=1)
            
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            metadata = {
                "source_type": "json",
                "source_path": file_path,
                "rows_ingested": len(df),
                "columns": list(df.columns),
                "processing_time": processing_time,
                "schema_version": schema_version.version,
                "ingestion_timestamp": datetime.utcnow().isoformat()
            }
            
            logger.log_data_processing(
                stage="json_ingestion",
                input_rows=len(df),
                output_rows=len(df),
                processing_time=processing_time,
                source_name=source_name
            )
            
            return df, metadata
            
        except Exception as e:
            logger.error(f"Failed to ingest JSON: {file_path}", error=str(e), source_name=source_name)
            raise
    
    @mock_s3
    def ingest_from_s3(self, s3_path: str, source_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Ingest data from S3 (mocked for development)."""
        logger.info(f"Ingesting data from S3: {s3_path}", source_name=source_name)
        
        # Parse S3 path
        parts = s3_path.replace('s3://', '').split('/', 1)
        bucket_name = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        # Create mock S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.base_config.aws.access_key_id,
            aws_secret_access_key=config.base_config.aws.secret_access_key,
            region_name=config.base_config.aws.region
        )
        
        # For development, we'll simulate S3 by reading local files
        # In production, this would use actual S3 operations
        logger.info(f"Simulating S3 ingestion for: {s3_path}")
        
        # This is a placeholder - in real implementation, you'd:
        # 1. List objects in S3 bucket/prefix
        # 2. Download and process each file
        # 3. Handle different file formats
        
        return pd.DataFrame(), {"simulated": True}
    
    def save_to_bronze(self, df: pd.DataFrame, source_name: str, metadata: Dict[str, Any]) -> Path:
        """Save ingested data to Bronze layer."""
        logger.info(f"Saving data to Bronze layer: {source_name}")
        
        # Create bronze directory structure
        bronze_path = Path(self.storage_config.get('bronze_path', 'output/bronze'))
        
        # Add partitioning by year/month
        current_date = datetime.utcnow()
        partition_path = bronze_path / f"year={current_date.year}" / f"month={current_date.month:02d}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Save as Parquet
        output_file = partition_path / f"{source_name}_{current_date.strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, index=False)
        
        # Save metadata
        metadata_file = partition_path / f"{source_name}_{current_date.strftime('%Y%m%d_%H%M%S')}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Data saved to Bronze: {output_file}", 
                   rows=len(df), file_size_mb=output_file.stat().st_size / 1024 / 1024)
        
        return output_file
    
    def run_ingestion_pipeline(self, run_id: str) -> Dict[str, Any]:
        """Run the complete ingestion pipeline."""
        logger.log_pipeline_start("ingestion", run_id)
        start_time = datetime.utcnow()
        
        results = {
            "run_id": run_id,
            "start_time": start_time.isoformat(),
            "sources_processed": [],
            "total_rows_ingested": 0,
            "status": "running"
        }
        
        try:
            # Get data sources from config
            data_sources = config.get_data_sources()
            
            for source_config in data_sources:
                source_name = source_config['name']
                source_type = source_config['type']
                source_path = source_config['path']
                
                logger.info(f"Processing source: {source_name}", source_type=source_type)
                
                try:
                    # Ingest based on source type
                    if source_type == 'csv':
                        df, metadata = self.ingest_csv(source_path, source_name)
                    elif source_type == 'json':
                        df, metadata = self.ingest_json(source_path, source_name)
                    elif source_type == 's3':
                        df, metadata = self.ingest_from_s3(source_path, source_name)
                    else:
                        raise ValueError(f"Unsupported source type: {source_type}")
                    
                    # Save to Bronze layer
                    output_file = self.save_to_bronze(df, source_name, metadata)
                    
                    # Update results
                    results["sources_processed"].append({
                        "source_name": source_name,
                        "source_type": source_type,
                        "rows_ingested": len(df),
                        "output_file": str(output_file),
                        "status": "success"
                    })
                    results["total_rows_ingested"] += len(df)
                    
                except Exception as e:
                    logger.error(f"Failed to process source: {source_name}", error=str(e))
                    results["sources_processed"].append({
                        "source_name": source_name,
                        "source_type": source_type,
                        "status": "failed",
                        "error": str(e)
                    })
            
            # Calculate final results
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "status": "completed"
            })
            
            logger.log_pipeline_end("ingestion", run_id, "completed", duration)
            
            return results
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "status": "failed",
                "error": str(e)
            })
            
            logger.error("Ingestion pipeline failed", error=str(e), run_id=run_id)
            logger.log_pipeline_end("ingestion", run_id, "failed", duration)
            
            return results


def main():
    """Main entry point for ingestion pipeline."""
    import uuid
    
    ingester = DataIngester()
    run_id = str(uuid.uuid4())
    
    results = ingester.run_ingestion_pipeline(run_id)
    
    print(f"Ingestion pipeline completed with status: {results['status']}")
    print(f"Total rows ingested: {results['total_rows_ingested']}")
    
    return results


if __name__ == "__main__":
    main()
