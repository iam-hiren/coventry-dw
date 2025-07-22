"""
Financial Services Specific Monitoring and Metrics

This module provides specialized monitoring capabilities for financial
data pipelines including regulatory compliance metrics, risk indicators,
and operational performance tracking.
"""

import logging
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json

from ..compliance.audit_manager import AuditManager, ComplianceLevel
from ..utils.config import ConfigManager


class AlertSeverity(Enum):
    """Alert severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class MetricType(Enum):
    """Types of financial metrics"""
    OPERATIONAL = "OPERATIONAL"
    COMPLIANCE = "COMPLIANCE"
    RISK = "RISK"
    PERFORMANCE = "PERFORMANCE"
    DATA_QUALITY = "DATA_QUALITY"


@dataclass
class FinancialMetric:
    """Financial metric data structure"""
    metric_name: str
    metric_type: MetricType
    value: float
    threshold: float
    timestamp: datetime
    severity: AlertSeverity
    description: str
    metadata: Dict[str, Any]


@dataclass
class ComplianceAlert:
    """Compliance alert data structure"""
    alert_id: str
    compliance_level: ComplianceLevel
    severity: AlertSeverity
    message: str
    timestamp: datetime
    affected_records: int
    resolution_required: bool
    metadata: Dict[str, Any]


class FinancialMetricsCollector:
    """
    Comprehensive metrics collector for financial data pipelines
    """
    
    def __init__(self, config_manager: ConfigManager, audit_manager: Optional[AuditManager] = None):
        self.config = config_manager
        self.audit_manager = audit_manager
        self.logger = logging.getLogger(__name__)
        
        # Metrics storage
        self.metrics: List[FinancialMetric] = []
        self.alerts: List[ComplianceAlert] = []
        
        # Thresholds from configuration
        self.thresholds = self.config.get_monitoring_thresholds()
        
        # Performance tracking
        self.processing_times: Dict[str, List[float]] = {}
        self.error_counts: Dict[str, int] = {}
        
    def record_processing_time(self, operation: str, duration: float) -> None:
        """
        Record processing time for an operation
        
        Args:
            operation: Name of the operation
            duration: Duration in seconds
        """
        if operation not in self.processing_times:
            self.processing_times[operation] = []
        
        self.processing_times[operation].append(duration)
        
        # Keep only last 100 measurements
        if len(self.processing_times[operation]) > 100:
            self.processing_times[operation] = self.processing_times[operation][-100:]
        
        # Check if processing time exceeds threshold
        threshold = self.thresholds.get(f"{operation}_max_duration", 300)  # 5 minutes default
        
        if duration > threshold:
            self._create_performance_alert(
                operation=operation,
                duration=duration,
                threshold=threshold
            )
    
    def record_data_quality_metric(
        self,
        dataset: str,
        total_records: int,
        valid_records: int,
        invalid_records: int,
        quality_score: float
    ) -> None:
        """
        Record data quality metrics
        
        Args:
            dataset: Name of the dataset
            total_records: Total number of records processed
            valid_records: Number of valid records
            invalid_records: Number of invalid records
            quality_score: Quality score (0-1)
        """
        metric = FinancialMetric(
            metric_name=f"{dataset}_data_quality",
            metric_type=MetricType.DATA_QUALITY,
            value=quality_score,
            threshold=self.thresholds.get("min_data_quality_score", 0.95),
            timestamp=datetime.utcnow(),
            severity=self._calculate_severity(quality_score, 0.95),
            description=f"Data quality score for {dataset}",
            metadata={
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": invalid_records,
                "error_rate": invalid_records / total_records if total_records > 0 else 0
            }
        )
        
        self.metrics.append(metric)
        
        # Create alert if quality is below threshold
        if quality_score < self.thresholds.get("min_data_quality_score", 0.95):
            self._create_data_quality_alert(dataset, quality_score, total_records, invalid_records)
    
    def record_compliance_metric(
        self,
        compliance_level: ComplianceLevel,
        check_name: str,
        passed: bool,
        affected_records: int,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record compliance check results
        
        Args:
            compliance_level: Level of compliance being checked
            check_name: Name of the compliance check
            passed: Whether the check passed
            affected_records: Number of records affected
            details: Additional details about the check
        """
        metric = FinancialMetric(
            metric_name=f"compliance_{compliance_level.value}_{check_name}",
            metric_type=MetricType.COMPLIANCE,
            value=1.0 if passed else 0.0,
            threshold=1.0,
            timestamp=datetime.utcnow(),
            severity=AlertSeverity.LOW if passed else AlertSeverity.HIGH,
            description=f"Compliance check: {check_name} for {compliance_level.value}",
            metadata={
                "compliance_level": compliance_level.value,
                "check_name": check_name,
                "affected_records": affected_records,
                "details": details or {}
            }
        )
        
        self.metrics.append(metric)
        
        # Create compliance alert if check failed
        if not passed:
            self._create_compliance_alert(
                compliance_level=compliance_level,
                check_name=check_name,
                affected_records=affected_records,
                details=details
            )
    
    def record_risk_metric(
        self,
        risk_type: str,
        risk_score: float,
        threshold: float,
        affected_entities: int,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record risk assessment metrics
        
        Args:
            risk_type: Type of risk being measured
            risk_score: Risk score (0-1, where 1 is highest risk)
            threshold: Risk threshold
            affected_entities: Number of entities affected
            details: Additional risk details
        """
        metric = FinancialMetric(
            metric_name=f"risk_{risk_type}",
            metric_type=MetricType.RISK,
            value=risk_score,
            threshold=threshold,
            timestamp=datetime.utcnow(),
            severity=self._calculate_risk_severity(risk_score, threshold),
            description=f"Risk assessment: {risk_type}",
            metadata={
                "risk_type": risk_type,
                "affected_entities": affected_entities,
                "details": details or {}
            }
        )
        
        self.metrics.append(metric)
        
        # Create risk alert if score exceeds threshold
        if risk_score > threshold:
            self._create_risk_alert(risk_type, risk_score, threshold, affected_entities)
    
    def record_transaction_metrics(
        self,
        total_transactions: int,
        total_amount: float,
        suspicious_transactions: int,
        large_transactions: int,
        processing_duration: float
    ) -> None:
        """
        Record transaction processing metrics
        
        Args:
            total_transactions: Total number of transactions processed
            total_amount: Total monetary amount processed
            suspicious_transactions: Number of suspicious transactions
            large_transactions: Number of large transactions (>£10k)
            processing_duration: Time taken to process
        """
        # Transaction volume metric
        self.metrics.append(FinancialMetric(
            metric_name="transaction_volume",
            metric_type=MetricType.OPERATIONAL,
            value=total_transactions,
            threshold=self.thresholds.get("max_transaction_volume", 100000),
            timestamp=datetime.utcnow(),
            severity=AlertSeverity.LOW,
            description="Total transaction volume processed",
            metadata={
                "total_amount": total_amount,
                "average_amount": total_amount / total_transactions if total_transactions > 0 else 0
            }
        ))
        
        # Suspicious transaction rate
        suspicious_rate = suspicious_transactions / total_transactions if total_transactions > 0 else 0
        self.metrics.append(FinancialMetric(
            metric_name="suspicious_transaction_rate",
            metric_type=MetricType.RISK,
            value=suspicious_rate,
            threshold=self.thresholds.get("max_suspicious_rate", 0.05),  # 5% threshold
            timestamp=datetime.utcnow(),
            severity=self._calculate_severity(suspicious_rate, 0.05, reverse=True),
            description="Rate of suspicious transactions detected",
            metadata={
                "suspicious_count": suspicious_transactions,
                "total_transactions": total_transactions
            }
        ))
        
        # Processing performance
        self.record_processing_time("transaction_processing", processing_duration)
    
    def get_metrics_summary(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get metrics summary for the specified time period
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Dictionary containing metrics summary
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        recent_metrics = [m for m in self.metrics if m.timestamp >= cutoff_time]
        
        # Group metrics by type
        metrics_by_type = {}
        for metric in recent_metrics:
            metric_type = metric.metric_type.value
            if metric_type not in metrics_by_type:
                metrics_by_type[metric_type] = []
            metrics_by_type[metric_type].append(metric)
        
        # Calculate summaries
        summary = {
            "period_hours": hours,
            "total_metrics": len(recent_metrics),
            "metrics_by_type": {},
            "alerts": {
                "critical": len([a for a in self.alerts if a.severity == AlertSeverity.CRITICAL]),
                "high": len([a for a in self.alerts if a.severity == AlertSeverity.HIGH]),
                "medium": len([a for a in self.alerts if a.severity == AlertSeverity.MEDIUM]),
                "low": len([a for a in self.alerts if a.severity == AlertSeverity.LOW])
            },
            "performance": self._get_performance_summary()
        }
        
        for metric_type, metrics in metrics_by_type.items():
            summary["metrics_by_type"][metric_type] = {
                "count": len(metrics),
                "avg_value": sum(m.value for m in metrics) / len(metrics),
                "alerts": len([m for m in metrics if m.severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]])
            }
        
        return summary
    
    def check_sla_compliance(self) -> Dict[str, Any]:
        """
        Check SLA compliance across all operations
        
        Returns:
            Dictionary containing SLA compliance status
        """
        sla_results = {}
        
        for operation, times in self.processing_times.items():
            if not times:
                continue
            
            avg_time = sum(times) / len(times)
            max_time = max(times)
            sla_threshold = self.thresholds.get(f"{operation}_sla", 300)  # 5 minutes default
            
            compliance_rate = len([t for t in times if t <= sla_threshold]) / len(times)
            
            sla_results[operation] = {
                "average_time": avg_time,
                "max_time": max_time,
                "sla_threshold": sla_threshold,
                "compliance_rate": compliance_rate,
                "compliant": compliance_rate >= 0.95,  # 95% compliance required
                "measurements": len(times)
            }
        
        return sla_results
    
    def generate_compliance_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive compliance report
        
        Returns:
            Dictionary containing compliance report
        """
        compliance_metrics = [m for m in self.metrics if m.metric_type == MetricType.COMPLIANCE]
        compliance_alerts = [a for a in self.alerts if a.severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]]
        
        # Group by compliance level
        by_compliance_level = {}
        for metric in compliance_metrics:
            level = metric.metadata.get("compliance_level", "UNKNOWN")
            if level not in by_compliance_level:
                by_compliance_level[level] = {"passed": 0, "failed": 0, "checks": []}
            
            if metric.value == 1.0:
                by_compliance_level[level]["passed"] += 1
            else:
                by_compliance_level[level]["failed"] += 1
            
            by_compliance_level[level]["checks"].append({
                "check_name": metric.metadata.get("check_name"),
                "passed": metric.value == 1.0,
                "timestamp": metric.timestamp.isoformat(),
                "affected_records": metric.metadata.get("affected_records", 0)
            })
        
        return {
            "report_generated": datetime.utcnow().isoformat(),
            "total_compliance_checks": len(compliance_metrics),
            "total_compliance_alerts": len(compliance_alerts),
            "compliance_by_level": by_compliance_level,
            "recent_alerts": [
                {
                    "alert_id": alert.alert_id,
                    "compliance_level": alert.compliance_level.value,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "affected_records": alert.affected_records
                }
                for alert in compliance_alerts[-10:]  # Last 10 alerts
            ]
        }
    
    def _calculate_severity(self, value: float, threshold: float, reverse: bool = False) -> AlertSeverity:
        """Calculate alert severity based on value and threshold"""
        if reverse:
            # For metrics where higher values are worse (e.g., error rates)
            if value > threshold * 2:
                return AlertSeverity.CRITICAL
            elif value > threshold * 1.5:
                return AlertSeverity.HIGH
            elif value > threshold:
                return AlertSeverity.MEDIUM
            else:
                return AlertSeverity.LOW
        else:
            # For metrics where lower values are worse (e.g., quality scores)
            if value < threshold * 0.5:
                return AlertSeverity.CRITICAL
            elif value < threshold * 0.75:
                return AlertSeverity.HIGH
            elif value < threshold:
                return AlertSeverity.MEDIUM
            else:
                return AlertSeverity.LOW
    
    def _calculate_risk_severity(self, risk_score: float, threshold: float) -> AlertSeverity:
        """Calculate risk alert severity"""
        if risk_score > threshold * 2:
            return AlertSeverity.CRITICAL
        elif risk_score > threshold * 1.5:
            return AlertSeverity.HIGH
        elif risk_score > threshold:
            return AlertSeverity.MEDIUM
        else:
            return AlertSeverity.LOW
    
    def _create_performance_alert(self, operation: str, duration: float, threshold: float) -> None:
        """Create performance alert"""
        alert = ComplianceAlert(
            alert_id=f"PERF_{operation}_{int(time.time())}",
            compliance_level=ComplianceLevel.SOX,  # Performance impacts SOX compliance
            severity=AlertSeverity.HIGH if duration > threshold * 2 else AlertSeverity.MEDIUM,
            message=f"Operation '{operation}' exceeded performance threshold: {duration:.2f}s > {threshold}s",
            timestamp=datetime.utcnow(),
            affected_records=0,
            resolution_required=True,
            metadata={
                "operation": operation,
                "duration": duration,
                "threshold": threshold,
                "type": "PERFORMANCE"
            }
        )
        
        self.alerts.append(alert)
        self.logger.warning(f"Performance alert: {alert.message}")
    
    def _create_data_quality_alert(self, dataset: str, quality_score: float, total_records: int, invalid_records: int) -> None:
        """Create data quality alert"""
        alert = ComplianceAlert(
            alert_id=f"DQ_{dataset}_{int(time.time())}",
            compliance_level=ComplianceLevel.FCA_RULES,
            severity=AlertSeverity.HIGH if quality_score < 0.8 else AlertSeverity.MEDIUM,
            message=f"Data quality below threshold for {dataset}: {quality_score:.2%}",
            timestamp=datetime.utcnow(),
            affected_records=invalid_records,
            resolution_required=True,
            metadata={
                "dataset": dataset,
                "quality_score": quality_score,
                "total_records": total_records,
                "invalid_records": invalid_records,
                "type": "DATA_QUALITY"
            }
        )
        
        self.alerts.append(alert)
        self.logger.error(f"Data quality alert: {alert.message}")
    
    def _create_compliance_alert(
        self,
        compliance_level: ComplianceLevel,
        check_name: str,
        affected_records: int,
        details: Optional[Dict[str, Any]]
    ) -> None:
        """Create compliance alert"""
        alert = ComplianceAlert(
            alert_id=f"COMP_{compliance_level.value}_{check_name}_{int(time.time())}",
            compliance_level=compliance_level,
            severity=AlertSeverity.CRITICAL,  # Compliance failures are always critical
            message=f"Compliance check failed: {check_name} ({compliance_level.value})",
            timestamp=datetime.utcnow(),
            affected_records=affected_records,
            resolution_required=True,
            metadata={
                "check_name": check_name,
                "compliance_level": compliance_level.value,
                "details": details or {},
                "type": "COMPLIANCE"
            }
        )
        
        self.alerts.append(alert)
        self.logger.critical(f"Compliance alert: {alert.message}")
    
    def _create_risk_alert(self, risk_type: str, risk_score: float, threshold: float, affected_entities: int) -> None:
        """Create risk alert"""
        alert = ComplianceAlert(
            alert_id=f"RISK_{risk_type}_{int(time.time())}",
            compliance_level=ComplianceLevel.FCA_RULES,
            severity=self._calculate_risk_severity(risk_score, threshold),
            message=f"Risk threshold exceeded for {risk_type}: {risk_score:.2%} > {threshold:.2%}",
            timestamp=datetime.utcnow(),
            affected_records=affected_entities,
            resolution_required=True,
            metadata={
                "risk_type": risk_type,
                "risk_score": risk_score,
                "threshold": threshold,
                "affected_entities": affected_entities,
                "type": "RISK"
            }
        )
        
        self.alerts.append(alert)
        self.logger.warning(f"Risk alert: {alert.message}")
    
    def _get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        summary = {}
        
        for operation, times in self.processing_times.items():
            if times:
                summary[operation] = {
                    "avg_time": sum(times) / len(times),
                    "min_time": min(times),
                    "max_time": max(times),
                    "measurements": len(times)
                }
        
        return summary
    
    def export_metrics(self, output_path: str) -> None:
        """
        Export all metrics to file
        
        Args:
            output_path: Path to export file
        """
        export_data = {
            "export_timestamp": datetime.utcnow().isoformat(),
            "metrics": [
                {
                    "metric_name": m.metric_name,
                    "metric_type": m.metric_type.value,
                    "value": m.value,
                    "threshold": m.threshold,
                    "timestamp": m.timestamp.isoformat(),
                    "severity": m.severity.value,
                    "description": m.description,
                    "metadata": m.metadata
                }
                for m in self.metrics
            ],
            "alerts": [
                {
                    "alert_id": a.alert_id,
                    "compliance_level": a.compliance_level.value,
                    "severity": a.severity.value,
                    "message": a.message,
                    "timestamp": a.timestamp.isoformat(),
                    "affected_records": a.affected_records,
                    "resolution_required": a.resolution_required,
                    "metadata": a.metadata
                }
                for a in self.alerts
            ]
        }
        
        with open(output_path, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        self.logger.info(f"Metrics exported to {output_path}")
