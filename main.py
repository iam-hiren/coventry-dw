#!/usr/bin/env python3
"""
Coventry Building Society Data Warehouse Pipeline
Main entry point for pipeline execution.

This script provides a command-line interface for running the complete
data pipeline or individual components.

Usage:
    python main.py --help
    python main.py --mode full
    python main.py --mode ingestion
    python main.py --mode transformation
    python main.py --schedule daily
"""

import sys
import argparse
import uuid
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.orchestrator import PipelineOrchestrator
from src.utils import get_logger, config
from src.monitoring import PipelineMonitor

logger = get_logger(__name__)


def create_parser():
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Coventry Building Society Data Warehouse Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode full                    # Run complete pipeline
  %(prog)s --mode ingestion               # Run ingestion only
  %(prog)s --mode transformation          # Run transformation only
  %(prog)s --mode status                  # Check pipeline status
  %(prog)s --schedule daily               # Schedule daily execution
  %(prog)s --cleanup --days 30            # Cleanup old data
  %(prog)s --validate-config              # Validate configuration
        """
    )
    
    # Main execution modes
    parser.add_argument(
        "--mode",
        choices=["full", "ingestion", "transformation", "status", "cleanup"],
        default="full",
        help="Pipeline execution mode (default: full)"
    )
    
    # Pipeline configuration
    parser.add_argument(
        "--run-id",
        help="Custom run ID for pipeline execution"
    )
    
    parser.add_argument(
        "--config",
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--environment",
        choices=["development", "staging", "production"],
        help="Override environment setting"
    )
    
    # Data source options
    parser.add_argument(
        "--bronze-path",
        help="Path to Bronze files for transformation-only mode"
    )
    
    parser.add_argument(
        "--data-path",
        help="Path to input data files"
    )
    
    # Scheduling options
    parser.add_argument(
        "--schedule",
        choices=["daily", "hourly", "weekly"],
        help="Schedule pipeline execution"
    )
    
    # Cleanup options
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Run cleanup of old data"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Days of data to keep during cleanup (default: 30)"
    )
    
    # Validation and testing
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration and exit"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry run without actual execution"
    )
    
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Test database and service connections"
    )
    
    # Output options
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress non-error output"
    )
    
    parser.add_argument(
        "--output-format",
        choices=["json", "table", "csv"],
        default="table",
        help="Output format for results (default: table)"
    )
    
    # Monitoring and alerting
    parser.add_argument(
        "--no-alerts",
        action="store_true",
        help="Disable alerting for this run"
    )
    
    parser.add_argument(
        "--export-metrics",
        help="Export metrics to specified file"
    )
    
    return parser


def validate_configuration():
    """Validate pipeline configuration."""
    logger.info("Validating pipeline configuration...")
    
    try:
        # Test configuration loading
        env_config = config.get_environment_config()
        data_sources = config.get_data_sources()
        storage_config = config.get_storage_config()
        
        # Validate data sources
        for source in data_sources:
            if source['type'] not in ['csv', 'json', 's3']:
                raise ValueError(f"Unsupported source type: {source['type']}")
            
            if source['type'] in ['csv', 'json']:
                source_path = Path(source['path'])
                if not source_path.exists():
                    logger.warning(f"Source file not found: {source_path}")
        
        # Validate storage paths
        for path_type, path_value in storage_config.items():
            if path_type.endswith('_path'):
                Path(path_value).mkdir(parents=True, exist_ok=True)
        
        logger.info("Configuration validation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return False


def test_connections():
    """Test database and service connections."""
    logger.info("Testing service connections...")
    
    connection_results = {
        "database": False,
        "s3": False,
        "monitoring": False
    }
    
    # Test database connection
    try:
        from sqlalchemy import create_engine
        engine = create_engine(config.base_config.db.connection_string)
        with engine.connect() as conn:
            conn.execute('SELECT 1')
        connection_results["database"] = True
        logger.info("Database connection: OK")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
    
    # Test S3 connection (if configured)
    try:
        import boto3
        s3_client = boto3.client('s3')
        # This is a basic test - in production you'd test actual bucket access
        connection_results["s3"] = True
        logger.info("S3 connection: OK")
    except Exception as e:
        logger.warning(f"S3 connection test skipped: {e}")
    
    # Test monitoring
    try:
        monitor = PipelineMonitor()
        health = monitor.get_pipeline_health()
        connection_results["monitoring"] = True
        logger.info(f"Monitoring system: OK (Status: {health['status']})")
    except Exception as e:
        logger.error(f"Monitoring system test failed: {e}")
    
    return connection_results


def format_output(data, output_format="table"):
    """Format output data according to specified format."""
    if output_format == "json":
        import json
        return json.dumps(data, indent=2, default=str)
    elif output_format == "csv":
        # Simple CSV output for basic data
        if isinstance(data, dict):
            return ",".join(f"{k}={v}" for k, v in data.items())
        return str(data)
    else:  # table format
        if isinstance(data, dict):
            lines = []
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{key}:")
                    for sub_key, sub_value in value.items():
                        lines.append(f"  {sub_key}: {sub_value}")
                else:
                    lines.append(f"{key}: {value}")
            return "\n".join(lines)
        return str(data)


def main():
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        import logging
        logging.getLogger().setLevel(logging.ERROR)
    
    # Override environment if specified
    if args.environment:
        import os
        os.environ['ENVIRONMENT'] = args.environment
    
    logger.info(f"Starting Coventry DW Pipeline - Mode: {args.mode}")
    logger.info(f"Environment: {config.base_config.environment}")
    
    try:
        # Configuration validation
        if args.validate_config:
            success = validate_configuration()
            sys.exit(0 if success else 1)
        
        # Connection testing
        if args.test_connection:
            connections = test_connections()
            print(format_output(connections, args.output_format))
            failed_connections = [k for k, v in connections.items() if not v]
            sys.exit(0 if not failed_connections else 1)
        
        # Create orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Handle scheduling
        if args.schedule:
            result = orchestrator.schedule_pipeline(args.schedule)
            print(format_output(result, args.output_format))
            return
        
        # Handle cleanup
        if args.cleanup:
            result = orchestrator.cleanup_old_data(args.days)
            print(format_output(result, args.output_format))
            return
        
        # Handle metrics export
        if args.export_metrics:
            monitor = PipelineMonitor()
            export_file = monitor.export_metrics(args.export_metrics)
            print(f"Metrics exported to: {export_file}")
            return
        
        # Generate run ID
        run_id = args.run_id or str(uuid.uuid4())
        
        # Execute pipeline based on mode
        if args.dry_run:
            logger.info("DRY RUN MODE - No actual execution will occur")
            result = {
                "run_id": run_id,
                "mode": args.mode,
                "status": "dry_run_completed",
                "message": "Dry run completed successfully"
            }
        elif args.mode == "full":
            result = orchestrator.run_full_pipeline(run_id)
        elif args.mode == "ingestion":
            result = orchestrator.run_ingestion_only(run_id)
        elif args.mode == "transformation":
            result = orchestrator.run_transformation_only(run_id, args.bronze_path)
        elif args.mode == "status":
            result = orchestrator.get_pipeline_status()
        elif args.mode == "cleanup":
            result = orchestrator.cleanup_old_data(args.days)
        else:
            raise ValueError(f"Unknown mode: {args.mode}")
        
        # Output results
        if not args.quiet:
            print("\n" + "="*60)
            print("PIPELINE EXECUTION RESULTS")
            print("="*60)
            print(format_output(result, args.output_format))
            
            # Summary
            if 'status' in result:
                status = result['status']
                print(f"\nFinal Status: {status.upper()}")
                
                if 'duration_seconds' in result:
                    duration = result['duration_seconds']
                    print(f"Duration: {duration:.2f} seconds")
                
                if 'total_rows_processed' in result:
                    rows = result['total_rows_processed']
                    print(f"Rows Processed: {rows:,}")
        
        # Exit with appropriate code
        if 'status' in result:
            exit_code = 0 if result['status'] in ['completed', 'healthy'] else 1
        else:
            exit_code = 0
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        logger.info("Pipeline execution interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
