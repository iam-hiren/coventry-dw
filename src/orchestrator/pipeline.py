"""Main pipeline orchestrator for the Coventry DW pipeline."""

import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import json
import pandas as pd

from ..utils import get_logger, config
from ..ingestion import DataIngester
from ..transformation import DataTransformer
from ..monitoring import PipelineMonitor
from ..data_quality import DataQualityValidator

logger = get_logger(__name__)


class PipelineOrchestrator:
    """Orchestrates the complete data pipeline execution."""
    
    def __init__(self):
        self.ingester = DataIngester()
        self.transformer = DataTransformer()
        self.monitor = PipelineMonitor()
        self.data_quality = DataQualityValidator()
        
    def run_full_pipeline(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Run the complete end-to-end pipeline."""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        logger.log_pipeline_start("full_pipeline", run_id)
        start_time = datetime.utcnow()
        
        pipeline_results = {
            "run_id": run_id,
            "pipeline_name": "coventry-dw-full-pipeline",
            "start_time": start_time.isoformat(),
            "stages": {},
            "status": "running",
            "total_rows_processed": 0
        }
        
        try:
            # Stage 1: Data Ingestion (Bronze Layer)
            logger.info("Starting ingestion stage", run_id=run_id)
            ingestion_results = self.ingester.run_ingestion_pipeline(f"{run_id}_ingestion")
            pipeline_results["stages"]["ingestion"] = ingestion_results
            
            if ingestion_results["status"] != "completed":
                raise Exception(f"Ingestion stage failed: {ingestion_results.get('error', 'Unknown error')}")
            
            # Get Bronze files for transformation
            bronze_files = []
            for source in ingestion_results["sources_processed"]:
                if source["status"] == "success":
                    bronze_files.append(Path(source["output_file"]))
            
            if not bronze_files:
                raise Exception("No Bronze files available for transformation")
            
            # Stage 2: Data Transformation (Silver/Gold Layers)
            logger.info("Starting transformation stage", run_id=run_id)
            transformation_results = self.transformer.run_transformation_pipeline(
                f"{run_id}_transformation", bronze_files
            )
            pipeline_results["stages"]["transformation"] = transformation_results
            
            if transformation_results["status"] != "completed":
                logger.warning("Transformation stage completed with issues", run_id=run_id)
            
            # Calculate total rows processed
            pipeline_results["total_rows_processed"] = (
                ingestion_results.get("total_rows_ingested", 0) + 
                transformation_results.get("total_rows_processed", 0)
            )
            
            # Stage 3: Data Quality Assessment
            logger.info("Running data quality assessment", run_id=run_id)
            quality_results = self._run_quality_assessment(bronze_files)
            pipeline_results["stages"]["data_quality"] = quality_results
            
            # Determine final status
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            # Check if any critical issues occurred
            has_failures = (
                ingestion_results["status"] == "failed" or
                transformation_results["status"] == "failed" or
                any(not result["passed"] for result in quality_results.values())
            )
            
            final_status = "failed" if has_failures else "completed"
            
            pipeline_results.update({
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "status": final_status
            })
            
            # Record pipeline run for monitoring
            self.monitor.record_pipeline_run(
                run_id=run_id,
                pipeline_name="coventry-dw-full-pipeline",
                status=final_status,
                metadata=pipeline_results
            )
            
            logger.log_pipeline_end("full_pipeline", run_id, final_status, duration)
            
            return pipeline_results
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            pipeline_results.update({
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "status": "failed",
                "error": str(e)
            })
            
            # Record failed pipeline run
            self.monitor.record_pipeline_run(
                run_id=run_id,
                pipeline_name="coventry-dw-full-pipeline",
                status="failed",
                metadata=pipeline_results
            )
            
            logger.error("Full pipeline execution failed", error=str(e), run_id=run_id)
            logger.log_pipeline_end("full_pipeline", run_id, "failed", duration)
            
            return pipeline_results
    
    def _run_quality_assessment(self, bronze_files: List[Path]) -> Dict[str, Any]:
        """Run data quality assessment on Bronze files."""
        quality_results = {}
        
        for bronze_file in bronze_files:
            try:
                source_name = bronze_file.stem.split('_')[0]
                df = pd.read_parquet(bronze_file)
                
                is_valid, validation_result = self.data_quality.validate_data(df, source_name)
                quality_results[source_name] = validation_result
                
            except Exception as e:
                logger.error(f"Quality assessment failed for {bronze_file}", error=str(e))
                quality_results[bronze_file.stem] = {
                    "passed": False,
                    "error": str(e)
                }
        
        return quality_results
    
    def run_ingestion_only(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Run only the ingestion stage."""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        logger.info("Running ingestion-only pipeline", run_id=run_id)
        return self.ingester.run_ingestion_pipeline(run_id)
    
    def run_transformation_only(self, run_id: Optional[str] = None, 
                              bronze_path: Optional[str] = None) -> Dict[str, Any]:
        """Run only the transformation stage."""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        # Find Bronze files
        if bronze_path:
            bronze_dir = Path(bronze_path)
        else:
            bronze_dir = Path(config.get_storage_config().get('bronze_path', 'output/bronze'))
        
        bronze_files = list(bronze_dir.rglob("*.parquet")) if bronze_dir.exists() else []
        
        if not bronze_files:
            return {
                "run_id": run_id,
                "status": "failed",
                "error": "No Bronze files found to process"
            }
        
        logger.info("Running transformation-only pipeline", run_id=run_id, files_count=len(bronze_files))
        return self.transformer.run_transformation_pipeline(run_id, bronze_files)
    
    def schedule_pipeline(self, schedule_type: str = "daily") -> Dict[str, Any]:
        """Schedule pipeline execution (simulation)."""
        logger.info(f"Scheduling pipeline execution: {schedule_type}")
        
        # In a real implementation, this would integrate with:
        # - Apache Airflow DAGs
        # - Prefect flows
        # - AWS EventBridge
        # - Azure Logic Apps
        # - Google Cloud Scheduler
        
        schedule_config = {
            "daily": {"cron": "0 2 * * *", "description": "Daily at 2 AM"},
            "hourly": {"cron": "0 * * * *", "description": "Every hour"},
            "weekly": {"cron": "0 2 * * 1", "description": "Weekly on Monday at 2 AM"}
        }
        
        if schedule_type not in schedule_config:
            return {
                "status": "error",
                "message": f"Invalid schedule type: {schedule_type}"
            }
        
        # Simulate scheduling
        schedule_info = schedule_config[schedule_type]
        
        return {
            "status": "scheduled",
            "schedule_type": schedule_type,
            "cron_expression": schedule_info["cron"],
            "description": schedule_info["description"],
            "next_run": "Simulated - would calculate next run time",
            "message": f"Pipeline scheduled for {schedule_info['description']}"
        }
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and health."""
        return self.monitor.get_pipeline_health()
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> Dict[str, Any]:
        """Clean up old pipeline data and logs."""
        logger.info(f"Starting cleanup of data older than {days_to_keep} days")
        
        cleanup_results = {
            "days_to_keep": days_to_keep,
            "cleanup_timestamp": datetime.utcnow().isoformat(),
            "files_removed": [],
            "space_freed_mb": 0,
            "errors": []
        }
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        # Cleanup paths
        cleanup_paths = [
            Path(config.get_storage_config().get('bronze_path', 'output/bronze')),
            Path(config.get_storage_config().get('silver_path', 'output/silver')),
            Path(config.get_storage_config().get('quarantine_path', 'output/quarantine')),
            Path('logs')
        ]
        
        for cleanup_path in cleanup_paths:
            if not cleanup_path.exists():
                continue
            
            try:
                for file_path in cleanup_path.rglob("*"):
                    if file_path.is_file():
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        
                        if file_mtime < cutoff_date:
                            file_size_mb = file_path.stat().st_size / 1024 / 1024
                            file_path.unlink()
                            
                            cleanup_results["files_removed"].append(str(file_path))
                            cleanup_results["space_freed_mb"] += file_size_mb
                            
            except Exception as e:
                cleanup_results["errors"].append(f"Error cleaning {cleanup_path}: {str(e)}")
                logger.error(f"Cleanup error for {cleanup_path}", error=str(e))
        
        logger.info(f"Cleanup completed", 
                   files_removed=len(cleanup_results["files_removed"]),
                   space_freed_mb=cleanup_results["space_freed_mb"])
        
        return cleanup_results


def main():
    """Main entry point for pipeline orchestration."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Coventry DW Pipeline Orchestrator")
    parser.add_argument("--mode", choices=["full", "ingestion", "transformation", "status", "cleanup"],
                       default="full", help="Pipeline execution mode")
    parser.add_argument("--run-id", help="Custom run ID")
    parser.add_argument("--bronze-path", help="Path to Bronze files for transformation-only mode")
    parser.add_argument("--schedule", choices=["daily", "hourly", "weekly"], 
                       help="Schedule pipeline execution")
    parser.add_argument("--cleanup-days", type=int, default=30,
                       help="Days of data to keep during cleanup")
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    if args.schedule:
        result = orchestrator.schedule_pipeline(args.schedule)
        print(f"Scheduling result: {result}")
        return
    
    if args.mode == "full":
        print("Running full pipeline...")
        result = orchestrator.run_full_pipeline(args.run_id)
    elif args.mode == "ingestion":
        print("Running ingestion only...")
        result = orchestrator.run_ingestion_only(args.run_id)
    elif args.mode == "transformation":
        print("Running transformation only...")
        result = orchestrator.run_transformation_only(args.run_id, args.bronze_path)
    elif args.mode == "status":
        print("Getting pipeline status...")
        result = orchestrator.get_pipeline_status()
    elif args.mode == "cleanup":
        print(f"Cleaning up data older than {args.cleanup_days} days...")
        result = orchestrator.cleanup_old_data(args.cleanup_days)
    
    print(f"\nPipeline execution completed:")
    print(f"Status: {result.get('status', 'unknown')}")
    if 'duration_seconds' in result:
        print(f"Duration: {result['duration_seconds']:.2f} seconds")
    if 'total_rows_processed' in result:
        print(f"Rows processed: {result['total_rows_processed']:,}")
    
    return result


if __name__ == "__main__":
    main()
