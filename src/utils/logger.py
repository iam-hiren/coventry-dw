"""Structured logging utilities for the Coventry DW pipeline."""

import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import structlog
from .config import config


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "pipeline_name": config.base_config.pipeline_name,
            "environment": config.base_config.environment
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        return json.dumps(log_entry, default=str)


class PipelineLogger:
    """Enhanced logger for pipeline operations."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logger with JSON formatting."""
        logger = logging.getLogger(self.name)
        logger.setLevel(getattr(logging, config.base_config.log_level.upper()))
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JSONFormatter())
        logger.addHandler(console_handler)
        
        # File handler
        log_config = config.get_logging_config()
        for handler_config in log_config.get('handlers', []):
            if handler_config.get('type') == 'file':
                self._add_file_handler(logger, handler_config)
        
        return logger
    
    def _add_file_handler(self, logger: logging.Logger, handler_config: Dict[str, Any]):
        """Add file handler to logger."""
        log_file = Path(handler_config['filename'])
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=handler_config.get('max_bytes', 10485760),
            backupCount=handler_config.get('backup_count', 5)
        )
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)
    
    def info(self, message: str, **kwargs):
        """Log info message with extra fields."""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.info(message, extra=extra)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with extra fields."""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.warning(message, extra=extra)
    
    def error(self, message: str, **kwargs):
        """Log error message with extra fields."""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.error(message, extra=extra)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with extra fields."""
        extra = {'extra_fields': kwargs} if kwargs else {}
        self.logger.debug(message, extra=extra)
    
    def log_pipeline_start(self, pipeline_name: str, run_id: str):
        """Log pipeline start."""
        self.info(
            "Pipeline started",
            pipeline_name=pipeline_name,
            run_id=run_id,
            event_type="pipeline_start"
        )
    
    def log_pipeline_end(self, pipeline_name: str, run_id: str, status: str, duration: float):
        """Log pipeline completion."""
        self.info(
            "Pipeline completed",
            pipeline_name=pipeline_name,
            run_id=run_id,
            status=status,
            duration_seconds=duration,
            event_type="pipeline_end"
        )
    
    def log_data_processing(self, stage: str, input_rows: int, output_rows: int, 
                          processing_time: float, **kwargs):
        """Log data processing metrics."""
        self.info(
            f"Data processing completed: {stage}",
            stage=stage,
            input_rows=input_rows,
            output_rows=output_rows,
            processing_time_seconds=processing_time,
            event_type="data_processing",
            **kwargs
        )
    
    def log_data_quality_check(self, check_name: str, passed: bool, 
                             details: Optional[Dict[str, Any]] = None):
        """Log data quality check results."""
        self.info(
            f"Data quality check: {check_name}",
            check_name=check_name,
            passed=passed,
            details=details or {},
            event_type="data_quality_check"
        )
    
    def log_schema_validation(self, schema_name: str, validation_result: Dict[str, Any]):
        """Log schema validation results."""
        self.info(
            f"Schema validation: {schema_name}",
            schema_name=schema_name,
            validation_result=validation_result,
            event_type="schema_validation"
        )


def get_logger(name: str) -> PipelineLogger:
    """Get a pipeline logger instance."""
    return PipelineLogger(name)
