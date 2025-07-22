"""Data quality validation module for the Coventry DW pipeline."""

import pandas as pd
import pandera as pa
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import json
from pathlib import Path

from ..utils import get_logger, config
from ..schema import SchemaManager

logger = get_logger(__name__)


class DataQualityValidator:
    """Comprehensive data quality validation using multiple approaches."""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
        self.quality_config = config.get_data_quality_config()
        self.quarantine_path = Path(config.get_storage_config().get('quarantine_path', 'output/quarantine'))
        self.quarantine_path.mkdir(parents=True, exist_ok=True)
        
    def validate_data(self, df: pd.DataFrame, source_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Run comprehensive data quality validation."""
        logger.info(f"Running data quality validation for: {source_name}")
        
        validation_results = {
            "source_name": source_name,
            "validation_timestamp": datetime.utcnow().isoformat(),
            "total_rows": len(df),
            "checks": {},
            "overall_score": 0.0,
            "passed": False,
            "quarantined_rows": 0
        }
        
        # Schema validation
        schema_valid, schema_result = self._validate_schema(df, source_name)
        validation_results["checks"]["schema_validation"] = schema_result
        
        # Business rule validation
        business_valid, business_result = self._validate_business_rules(df, source_name)
        validation_results["checks"]["business_rules"] = business_result
        
        # Data completeness validation
        completeness_valid, completeness_result = self._validate_completeness(df)
        validation_results["checks"]["completeness"] = completeness_result
        
        # Data consistency validation
        consistency_valid, consistency_result = self._validate_consistency(df)
        validation_results["checks"]["consistency"] = consistency_result
        
        # Data freshness validation
        freshness_valid, freshness_result = self._validate_freshness(df)
        validation_results["checks"]["freshness"] = freshness_result
        
        # Calculate overall score
        check_scores = [
            schema_result.get("score", 0),
            business_result.get("score", 0),
            completeness_result.get("score", 0),
            consistency_result.get("score", 0),
            freshness_result.get("score", 0)
        ]
        validation_results["overall_score"] = sum(check_scores) / len(check_scores)
        
        # Determine if validation passed
        threshold = self.quality_config.get("coverage_threshold", 0.95)
        validation_results["passed"] = validation_results["overall_score"] >= threshold
        
        # Quarantine bad data if needed
        if not validation_results["passed"]:
            quarantined_rows = self._quarantine_bad_data(df, validation_results, source_name)
            validation_results["quarantined_rows"] = quarantined_rows
        
        logger.log_data_quality_check(
            f"overall_validation_{source_name}",
            validation_results["passed"],
            {"score": validation_results["overall_score"], "threshold": threshold}
        )
        
        return validation_results["passed"], validation_results
    
    def _validate_schema(self, df: pd.DataFrame, source_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Validate data against schema definition."""
        try:
            is_valid, schema_result = self.schema_manager.validate_dataframe(df, source_name)
            
            result = {
                "passed": is_valid,
                "score": 1.0 if is_valid else 0.0,
                "details": schema_result,
                "check_type": "schema_validation"
            }
            
            return is_valid, result
            
        except Exception as e:
            logger.error(f"Schema validation error for {source_name}", error=str(e))
            return False, {
                "passed": False,
                "score": 0.0,
                "error": str(e),
                "check_type": "schema_validation"
            }
    
    def _validate_business_rules(self, df: pd.DataFrame, source_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Validate business-specific rules."""
        rules = self.quality_config.get("rules", [])
        rule_results = []
        total_score = 0.0
        
        for rule in rules:
            rule_name = rule["name"]
            column = rule["column"]
            check_type = rule["check"]
            
            if column not in df.columns:
                rule_results.append({
                    "rule": rule_name,
                    "passed": False,
                    "error": f"Column {column} not found"
                })
                continue
            
            try:
                if check_type == "not_null":
                    passed_rows = df[column].notna().sum()
                    total_rows = len(df)
                    score = passed_rows / total_rows if total_rows > 0 else 0
                    
                elif check_type == "greater_than":
                    value = rule["value"]
                    passed_rows = (df[column] > value).sum()
                    total_rows = len(df)
                    score = passed_rows / total_rows if total_rows > 0 else 0
                    
                elif check_type == "date_range":
                    min_date = pd.to_datetime(rule["min_date"])
                    max_date = pd.to_datetime(rule["max_date"])
                    date_col = pd.to_datetime(df[column], errors='coerce')
                    passed_rows = ((date_col >= min_date) & (date_col <= max_date)).sum()
                    total_rows = len(df)
                    score = passed_rows / total_rows if total_rows > 0 else 0
                    
                else:
                    score = 0.0
                    passed_rows = 0
                    total_rows = len(df)
                
                rule_results.append({
                    "rule": rule_name,
                    "passed": score >= 0.95,  # 95% threshold for individual rules
                    "score": score,
                    "passed_rows": passed_rows,
                    "total_rows": total_rows
                })
                
                total_score += score
                
            except Exception as e:
                logger.error(f"Business rule validation error: {rule_name}", error=str(e))
                rule_results.append({
                    "rule": rule_name,
                    "passed": False,
                    "error": str(e)
                })
        
        overall_score = total_score / len(rules) if rules else 1.0
        overall_passed = overall_score >= 0.95
        
        result = {
            "passed": overall_passed,
            "score": overall_score,
            "rule_results": rule_results,
            "check_type": "business_rules"
        }
        
        return overall_passed, result
    
    def _validate_completeness(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validate data completeness."""
        business_columns = [col for col in df.columns if not col.startswith('_')]
        
        completeness_stats = {}
        total_completeness = 0.0
        
        for column in business_columns:
            non_null_count = df[column].notna().sum()
            total_count = len(df)
            completeness = non_null_count / total_count if total_count > 0 else 0
            
            completeness_stats[column] = {
                "completeness": completeness,
                "non_null_count": non_null_count,
                "total_count": total_count
            }
            
            total_completeness += completeness
        
        overall_completeness = total_completeness / len(business_columns) if business_columns else 1.0
        passed = overall_completeness >= 0.9  # 90% completeness threshold
        
        result = {
            "passed": passed,
            "score": overall_completeness,
            "overall_completeness": overall_completeness,
            "column_completeness": completeness_stats,
            "check_type": "completeness"
        }
        
        return passed, result
    
    def _validate_consistency(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validate data consistency."""
        consistency_checks = []
        
        # Check for duplicate records (excluding metadata columns)
        business_columns = [col for col in df.columns if not col.startswith('_')]
        duplicate_count = df.duplicated(subset=business_columns).sum()
        duplicate_rate = duplicate_count / len(df) if len(df) > 0 else 0
        
        consistency_checks.append({
            "check": "duplicate_records",
            "duplicate_count": duplicate_count,
            "duplicate_rate": duplicate_rate,
            "passed": duplicate_rate < 0.01  # Less than 1% duplicates
        })
        
        # Check for data type consistency
        type_consistency = True
        type_issues = []
        
        for column in df.columns:
            if column.startswith('_'):
                continue
                
            # Check for mixed types in object columns
            if df[column].dtype == 'object':
                sample_types = df[column].dropna().apply(type).unique()
                if len(sample_types) > 1:
                    type_consistency = False
                    type_issues.append({
                        "column": column,
                        "types_found": [t.__name__ for t in sample_types]
                    })
        
        consistency_checks.append({
            "check": "type_consistency",
            "passed": type_consistency,
            "type_issues": type_issues
        })
        
        # Calculate overall consistency score
        passed_checks = sum(1 for check in consistency_checks if check["passed"])
        consistency_score = passed_checks / len(consistency_checks) if consistency_checks else 1.0
        
        result = {
            "passed": consistency_score >= 0.9,
            "score": consistency_score,
            "consistency_checks": consistency_checks,
            "check_type": "consistency"
        }
        
        return result["passed"], result
    
    def _validate_freshness(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """Validate data freshness."""
        freshness_checks = []
        
        # Check ingestion timestamp freshness
        if '_ingestion_timestamp' in df.columns:
            latest_ingestion = pd.to_datetime(df['_ingestion_timestamp']).max()
            hours_since_ingestion = (datetime.utcnow() - latest_ingestion).total_seconds() / 3600
            
            freshness_checks.append({
                "check": "ingestion_freshness",
                "hours_since_ingestion": hours_since_ingestion,
                "passed": hours_since_ingestion < 24  # Data should be less than 24 hours old
            })
        
        # Check business date freshness (if applicable)
        date_columns = [col for col in df.columns if 'date' in col.lower() and not col.startswith('_')]
        for date_col in date_columns:
            try:
                latest_business_date = pd.to_datetime(df[date_col]).max()
                days_since_business_date = (datetime.utcnow() - latest_business_date).days
                
                freshness_checks.append({
                    "check": f"{date_col}_freshness",
                    "days_since_latest": days_since_business_date,
                    "passed": days_since_business_date < 7  # Business data should be within a week
                })
            except Exception:
                # Skip if date parsing fails
                continue
        
        # Calculate overall freshness score
        if not freshness_checks:
            # If no freshness checks possible, assume fresh
            freshness_score = 1.0
            passed = True
        else:
            passed_checks = sum(1 for check in freshness_checks if check["passed"])
            freshness_score = passed_checks / len(freshness_checks)
            passed = freshness_score >= 0.8
        
        result = {
            "passed": passed,
            "score": freshness_score,
            "freshness_checks": freshness_checks,
            "check_type": "freshness"
        }
        
        return passed, result
    
    def _quarantine_bad_data(self, df: pd.DataFrame, validation_results: Dict[str, Any], 
                           source_name: str) -> int:
        """Quarantine data that fails quality checks."""
        logger.info(f"Quarantining bad data for: {source_name}")
        
        # Identify rows to quarantine based on validation results
        quarantine_mask = pd.Series([False] * len(df))
        
        # Add rows with schema violations
        schema_result = validation_results["checks"].get("schema_validation", {})
        if not schema_result.get("passed", True):
            # For simplicity, quarantine all rows if schema validation fails
            # In practice, you'd identify specific failing rows
            quarantine_mask = pd.Series([True] * len(df))
        
        # Add rows with business rule violations
        business_result = validation_results["checks"].get("business_rules", {})
        for rule_result in business_result.get("rule_results", []):
            if not rule_result.get("passed", True):
                rule_name = rule_result["rule"]
                # This is a simplified approach - in practice, you'd identify specific failing rows
                logger.warning(f"Business rule failed: {rule_name}")
        
        quarantined_df = df[quarantine_mask]
        
        if len(quarantined_df) > 0:
            # Save quarantined data
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            quarantine_file = self.quarantine_path / f"{source_name}_quarantine_{timestamp}.parquet"
            quarantined_df.to_parquet(quarantine_file, index=False)
            
            # Save quarantine metadata
            quarantine_metadata = {
                "source_name": source_name,
                "quarantine_timestamp": datetime.utcnow().isoformat(),
                "quarantined_rows": len(quarantined_df),
                "total_rows": len(df),
                "quarantine_reason": validation_results,
                "quarantine_file": str(quarantine_file)
            }
            
            metadata_file = self.quarantine_path / f"{source_name}_quarantine_{timestamp}_metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(quarantine_metadata, f, indent=2, default=str)
            
            logger.warning(f"Quarantined {len(quarantined_df)} rows to: {quarantine_file}")
        
        return len(quarantined_df)
    
    def generate_quality_report(self, validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive data quality report."""
        report = {
            "report_timestamp": datetime.utcnow().isoformat(),
            "source_name": validation_results["source_name"],
            "overall_assessment": {
                "passed": validation_results["passed"],
                "score": validation_results["overall_score"],
                "total_rows": validation_results["total_rows"],
                "quarantined_rows": validation_results["quarantined_rows"]
            },
            "detailed_results": validation_results["checks"],
            "recommendations": self._generate_recommendations(validation_results)
        }
        
        return report
    
    def _generate_recommendations(self, validation_results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        # Schema recommendations
        schema_result = validation_results["checks"].get("schema_validation", {})
        if not schema_result.get("passed", True):
            recommendations.append("Review and update schema definitions to match incoming data structure")
        
        # Completeness recommendations
        completeness_result = validation_results["checks"].get("completeness", {})
        if completeness_result.get("score", 1.0) < 0.9:
            recommendations.append("Investigate data sources for missing values and implement data collection improvements")
        
        # Consistency recommendations
        consistency_result = validation_results["checks"].get("consistency", {})
        if not consistency_result.get("passed", True):
            recommendations.append("Implement data standardization processes to ensure consistency")
        
        # Freshness recommendations
        freshness_result = validation_results["checks"].get("freshness", {})
        if not freshness_result.get("passed", True):
            recommendations.append("Review data ingestion schedules to ensure timely data delivery")
        
        # Business rules recommendations
        business_result = validation_results["checks"].get("business_rules", {})
        if not business_result.get("passed", True):
            recommendations.append("Review business rules and data validation logic")
        
        return recommendations
