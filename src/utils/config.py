"""Configuration management for the Coventry DW pipeline."""

import os
import yaml
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DatabaseConfig(BaseSettings):
    """Database configuration."""
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    name: str = Field(default="coventry_dw", env="DB_NAME")
    user: str = Field(default="postgres", env="DB_USER")
    password: str = Field(default="", env="DB_PASSWORD")
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class AWSConfig(BaseSettings):
    """AWS configuration."""
    access_key_id: str = Field(default="", env="AWS_ACCESS_KEY_ID")
    secret_access_key: str = Field(default="", env="AWS_SECRET_ACCESS_KEY")
    region: str = Field(default="eu-west-2", env="AWS_DEFAULT_REGION")
    s3_bucket: str = Field(default="coventry-data-lake", env="S3_BUCKET_NAME")


class StorageConfig(BaseSettings):
    """Storage configuration."""
    data_root_path: str = Field(default="data", env="DATA_ROOT_PATH")
    output_root_path: str = Field(default="output", env="OUTPUT_ROOT_PATH")
    bronze_path: str = Field(default="output/bronze", env="BRONZE_PATH")
    silver_path: str = Field(default="output/silver", env="SILVER_PATH")
    gold_path: str = Field(default="output/gold", env="GOLD_PATH")
    quarantine_path: str = Field(default="output/quarantine", env="QUARANTINE_PATH")
    schema_path: str = Field(default="schemas", env="SCHEMA_PATH")
    logs_path: str = Field(default="logs", env="LOGS_PATH")
    storage_format: str = Field(default="parquet", env="STORAGE_FORMAT")
    partition_columns: str = Field(default="year,month", env="PARTITION_COLUMNS")
    compression: str = Field(default="snappy", env="COMPRESSION")
    
    @property
    def partition_cols_list(self) -> list:
        """Get partition columns as a list."""
        return [col.strip() for col in self.partition_columns.split(",") if col.strip()]


class DataQualityConfig(BaseSettings):
    """Data quality configuration."""
    threshold: float = Field(default=0.95, env="DATA_QUALITY_THRESHOLD")
    schema_validation_strict: bool = Field(default=True, env="SCHEMA_VALIDATION_STRICT")
    fail_on_error: bool = Field(default=False, env="FAIL_ON_DATA_QUALITY_ERROR")
    coverage_threshold: float = Field(default=0.95, env="COVERAGE_THRESHOLD")


class RetryConfig(BaseSettings):
    """Retry configuration."""
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay: int = Field(default=5, env="RETRY_DELAY")
    backoff_factor: float = Field(default=2.0, env="RETRY_BACKOFF_FACTOR")


class PerformanceConfig(BaseSettings):
    """Performance configuration."""
    max_workers: int = Field(default=4, env="MAX_WORKERS")
    chunk_size: int = Field(default=10000, env="CHUNK_SIZE")
    memory_limit_mb: int = Field(default=2048, env="MEMORY_LIMIT_MB")


class ComplianceConfig(BaseSettings):
    """Financial services compliance configuration."""
    enabled_levels: str = Field(default="FCA_RULES,GDPR,SOX", env="COMPLIANCE_LEVELS")
    audit_retention_days: int = Field(default=2555, env="AUDIT_RETENTION_DAYS")  # 7 years
    data_retention_days: int = Field(default=2555, env="DATA_RETENTION_DAYS")
    encryption_enabled: bool = Field(default=True, env="ENCRYPTION_ENABLED")
    pii_detection_enabled: bool = Field(default=True, env="PII_DETECTION_ENABLED")
    suspicious_transaction_threshold: float = Field(default=10000.0, env="SUSPICIOUS_THRESHOLD")
    max_transaction_amount: float = Field(default=1000000.0, env="MAX_TRANSACTION_AMOUNT")
    
    @property
    def enabled_levels_list(self) -> List[str]:
        return [level.strip() for level in self.enabled_levels.split(',')]


class MonitoringConfig(BaseSettings):
    """Monitoring and alerting configuration."""
    enable_metrics: bool = Field(default=True, env="ENABLE_METRICS")
    metrics_retention_days: int = Field(default=90, env="METRICS_RETENTION_DAYS")
    alert_email: str = Field(default="", env="ALERT_EMAIL")
    alert_slack_webhook: str = Field(default="", env="ALERT_SLACK_WEBHOOK")
    performance_threshold_seconds: int = Field(default=300, env="PERFORMANCE_THRESHOLD")
    data_quality_threshold: float = Field(default=0.95, env="DATA_QUALITY_THRESHOLD")
    suspicious_rate_threshold: float = Field(default=0.05, env="SUSPICIOUS_RATE_THRESHOLD")
    max_transaction_volume: int = Field(default=100000, env="MAX_TRANSACTION_VOLUME")


class SecurityConfig(BaseSettings):
    """Security configuration."""
    encryption_key: str = Field(default="", env="ENCRYPTION_KEY")
    jwt_secret: str = Field(default="", env="JWT_SECRET")
    api_key: str = Field(default="", env="API_KEY")
    enable_audit_logging: bool = Field(default=True, env="ENABLE_AUDIT_LOGGING")
    require_ssl: bool = Field(default=True, env="REQUIRE_SSL")
    session_timeout_minutes: int = Field(default=30, env="SESSION_TIMEOUT")
    max_login_attempts: int = Field(default=3, env="MAX_LOGIN_ATTEMPTS")


class BaseConfig:
    """Base configuration combining all config sections."""
    def __init__(self):
        self.database = DatabaseConfig()
        self.aws = AWSConfig()
        self.storage = StorageConfig()
        self.data_quality = DataQualityConfig()
        self.retry = RetryConfig()
        self.performance = PerformanceConfig()
        self.compliance = ComplianceConfig()
        self.monitoring = MonitoringConfig()
        self.security = SecurityConfig()


class ConfigManager:
    """Manages pipeline configuration from YAML files and environment variables."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/pipeline_config.yaml"
        self.base_config = BaseConfig()
        self._yaml_config = self._load_yaml_config()
    
    def _load_yaml_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_file = Path(self.config_path)
        if not config_file.exists():
            return {}
        
        with open(config_file, 'r') as f:
            content = f.read()
            # Expand environment variables
            content = self._expand_env_vars(content)
            return yaml.safe_load(content)
    
    def _expand_env_vars(self, content: str) -> str:
        """Expand environment variables in YAML content."""
        # Pattern to match ${VAR_NAME:-default_value} or ${VAR_NAME}
        pattern = r'\$\{([^}]+)\}'
        
        def replace_var(match):
            var_expr = match.group(1)
            if ':-' in var_expr:
                var_name, default_value = var_expr.split(':-', 1)
                return os.getenv(var_name.strip(), default_value.strip())
            else:
                return os.getenv(var_expr.strip(), '')
        
        return re.sub(pattern, replace_var, content)
    
    def get_environment_config(self) -> Dict[str, Any]:
        """Get configuration for current environment."""
        env = self.base_config.environment
        environments = self._yaml_config.get('environments', {})
        return environments.get(env, {})
    
    def get_data_sources(self) -> list:
        """Get data source configurations."""
        env_config = self.get_environment_config()
        return env_config.get('data_sources', [])
    
    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration."""
        # Merge YAML config with environment-based config
        env_config = self.get_environment_config()
        yaml_storage = env_config.get('storage', {})
        
        # Convert StorageConfig to dict and merge with YAML
        storage_dict = {
            'bronze_path': self.base_config.storage.bronze_path,
            'silver_path': self.base_config.storage.silver_path,
            'gold_path': self.base_config.storage.gold_path,
            'quarantine_path': self.base_config.storage.quarantine_path,
            'format': self.base_config.storage.storage_format,
            'partition_cols': self.base_config.storage.partition_cols_list,
            'compression': self.base_config.storage.compression,
        }
        
        # YAML config overrides environment config
        storage_dict.update(yaml_storage)
        return storage_dict
    
    def get_data_quality_config(self) -> Dict[str, Any]:
        """Get data quality configuration."""
        # Merge YAML config with environment-based config
        env_config = self.get_environment_config()
        yaml_dq = env_config.get('data_quality', {})
        
        # Convert DataQualityConfig to dict and merge with YAML
        dq_dict = {
            'enable_validation': True,  # Default from YAML
            'fail_on_error': self.base_config.data_quality.fail_on_error,
            'coverage_threshold': self.base_config.data_quality.coverage_threshold,
            'threshold': self.base_config.data_quality.threshold,
            'schema_validation_strict': self.base_config.data_quality.schema_validation_strict,
        }
        
        # YAML config overrides environment config
        dq_dict.update(yaml_dq)
        return dq_dict
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self._yaml_config.get('logging', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration."""
        return self._yaml_config.get('monitoring', {})
    
    def get_retry_config(self) -> Dict[str, Any]:
        """Get retry policy configuration."""
        # Merge YAML config with environment-based config
        yaml_retry = self._yaml_config.get('retry_policy', {})
        
        # Convert RetryConfig to dict and merge with YAML
        retry_dict = {
            'max_retries': self.base_config.retry.max_retries,
            'retry_delay': self.base_config.retry.retry_delay,
            'backoff_factor': self.base_config.retry.backoff_factor,
        }
        
        # YAML config overrides environment config
        retry_dict.update(yaml_retry)
        return retry_dict
    
    def get_compliance_config(self) -> Dict[str, Any]:
        """Get compliance configuration."""
        # Merge YAML config with environment-based config
        yaml_compliance = self._yaml_config.get('compliance', {})
        
        # Convert ComplianceConfig to dict and merge with YAML
        compliance_dict = {
            'enabled_levels': self.base_config.compliance.enabled_levels_list,
            'audit_retention_days': self.base_config.compliance.audit_retention_days,
            'data_retention_days': self.base_config.compliance.data_retention_days,
            'encryption_enabled': self.base_config.compliance.encryption_enabled,
            'pii_detection_enabled': self.base_config.compliance.pii_detection_enabled,
            'suspicious_transaction_threshold': self.base_config.compliance.suspicious_transaction_threshold,
            'max_transaction_amount': self.base_config.compliance.max_transaction_amount,
        }
        
        # Create compliance rules for different levels
        compliance_dict['FCA_RULES'] = {
            'allowed_classifications': ['PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'],
            'allowed_actions': ['READ', 'write', 'validate', 'transform', 'audit']
        }
        compliance_dict['GDPR'] = {
            'allowed_classifications': ['PUBLIC', 'INTERNAL'],
            'allowed_actions': ['read', 'validate', 'anonymize']
        }
        compliance_dict['SOX'] = {
            'allowed_classifications': ['PUBLIC', 'INTERNAL', 'CONFIDENTIAL'],
            'allowed_actions': ['read', 'validate', 'audit']
        }
        
        # YAML config overrides environment config
        compliance_dict.update(yaml_compliance)
        return compliance_dict
    
    def get_monitoring_thresholds(self) -> Dict[str, Any]:
        """Get monitoring thresholds configuration."""
        # Merge YAML config with environment-based config
        yaml_monitoring = self._yaml_config.get('monitoring', {})
        
        # Convert MonitoringConfig to dict and merge with YAML
        monitoring_dict = {
            'enable_metrics': self.base_config.monitoring.enable_metrics,
            'metrics_retention_days': self.base_config.monitoring.metrics_retention_days,
            'alert_email': self.base_config.monitoring.alert_email,
            'alert_slack_webhook': self.base_config.monitoring.alert_slack_webhook,
            'performance_threshold_seconds': self.base_config.monitoring.performance_threshold_seconds,
            'data_quality_threshold': self.base_config.monitoring.data_quality_threshold,
            'suspicious_rate_threshold': self.base_config.monitoring.suspicious_rate_threshold,
            'max_transaction_volume': self.base_config.monitoring.max_transaction_volume,
            
            # Additional thresholds
            'ingestion_max_duration': 300,  # 5 minutes
            'transformation_max_duration': 600,  # 10 minutes
            'validation_max_duration': 180,  # 3 minutes
            'min_data_quality_score': self.base_config.monitoring.data_quality_threshold,
            'max_suspicious_rate': self.base_config.monitoring.suspicious_rate_threshold,
        }
        
        # YAML config overrides environment config
        monitoring_dict.update(yaml_monitoring)
        return monitoring_dict
    
    def get_security_config(self) -> Dict[str, Any]:
        """Get security configuration."""
        # Merge YAML config with environment-based config
        yaml_security = self._yaml_config.get('security', {})
        
        # Convert SecurityConfig to dict and merge with YAML
        security_dict = {
            'encryption_key': self.base_config.security.encryption_key,
            'jwt_secret': self.base_config.security.jwt_secret,
            'api_key': self.base_config.security.api_key,
            'enable_audit_logging': self.base_config.security.enable_audit_logging,
            'require_ssl': self.base_config.security.require_ssl,
            'session_timeout_minutes': self.base_config.security.session_timeout_minutes,
            'max_login_attempts': self.base_config.security.max_login_attempts,
        }
        
        # YAML config overrides environment config
        security_dict.update(yaml_security)
        return security_dict
    
    def get_retention_policies(self) -> Dict[str, Any]:
        """Get data retention policies."""
        return {
            'transaction_data': self.base_config.compliance.data_retention_days,
            'customer_data': self.base_config.compliance.data_retention_days,
            'audit_logs': self.base_config.compliance.audit_retention_days,
            'metrics_data': self.base_config.monitoring.metrics_retention_days,
            'quarantine_data': 90,  # 3 months
            'temp_files': 7,  # 1 week
        }


# Global config instance
config = ConfigManager()
