"""
Financial Services Audit and Compliance Manager

This module provides comprehensive audit trail and compliance features
specifically designed for financial services data pipelines.

Key Features:
- Complete data lineage tracking
- Regulatory compliance monitoring
- Audit trail generation
- Data retention management
- Access control logging
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import uuid

from ..utils.config import ConfigManager


class ComplianceLevel(Enum):
    """Compliance levels for financial data processing"""
    PCI_DSS = "PCI_DSS"
    GDPR = "GDPR"
    SOX = "SOX"
    BASEL_III = "BASEL_III"
    FCA_RULES = "FCA_RULES"
    MIFID_II = "MIFID_II"


class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"
    TOP_SECRET = "TOP_SECRET"


@dataclass
class AuditEvent:
    """Audit event record"""
    event_id: str
    timestamp: datetime
    user_id: str
    action: str
    resource: str
    data_classification: DataClassification
    compliance_level: ComplianceLevel
    source_system: str
    destination_system: Optional[str]
    record_count: int
    data_hash: str
    success: bool
    error_message: Optional[str]
    metadata: Dict[str, Any]


@dataclass
class DataLineage:
    """Data lineage tracking"""
    lineage_id: str
    source_file: str
    source_hash: str
    transformations: List[str]
    destination_table: str
    processing_time: datetime
    data_quality_score: float
    compliance_checks: List[str]
    retention_date: datetime


class AuditManager:
    """
    Comprehensive audit and compliance manager for financial data pipelines
    """
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.logger = logging.getLogger(__name__)
        self.audit_events: List[AuditEvent] = []
        self.lineage_records: List[DataLineage] = []
        
        # Initialize compliance settings
        self.compliance_settings = self.config.get_compliance_config()
        self.retention_policies = self.config.get_retention_policies()
        
    def log_data_access(
        self,
        user_id: str,
        resource: str,
        action: str,
        data_classification: DataClassification,
        compliance_level: ComplianceLevel,
        record_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Log data access event for audit trail
        
        Args:
            user_id: User performing the action
            resource: Resource being accessed
            action: Action being performed
            data_classification: Classification of data
            compliance_level: Required compliance level
            record_count: Number of records affected
            metadata: Additional metadata
            
        Returns:
            Event ID for tracking
        """
        event_id = str(uuid.uuid4())
        
        # Create data hash for integrity verification
        data_hash = self._create_data_hash(resource, action, record_count)
        
        audit_event = AuditEvent(
            event_id=event_id,
            timestamp=datetime.utcnow(),
            user_id=user_id,
            action=action,
            resource=resource,
            data_classification=data_classification,
            compliance_level=compliance_level,
            source_system="coventry-dw-pipeline",
            destination_system=None,
            record_count=record_count,
            data_hash=data_hash,
            success=True,
            error_message=None,
            metadata=metadata or {}
        )
        
        self.audit_events.append(audit_event)
        
        # Log to structured logging
        self.logger.info(
            "Audit event logged",
            extra={
                "event_id": event_id,
                "user_id": user_id,
                "action": action,
                "resource": resource,
                "data_classification": data_classification.value,
                "compliance_level": compliance_level.value,
                "record_count": record_count
            }
        )
        
        return event_id
    
    def log_data_transformation(
        self,
        source_file: str,
        destination_table: str,
        transformations: List[str],
        data_quality_score: float,
        compliance_checks: List[str],
        retention_days: int = 2555  # 7 years default for financial data
    ) -> str:
        """
        Log data transformation for lineage tracking
        
        Args:
            source_file: Source file path
            destination_table: Destination table name
            transformations: List of transformations applied
            data_quality_score: Quality score (0-1)
            compliance_checks: List of compliance checks passed
            retention_days: Data retention period in days
            
        Returns:
            Lineage ID for tracking
        """
        lineage_id = str(uuid.uuid4())
        
        # Calculate source file hash
        source_hash = self._calculate_file_hash(source_file)
        
        # Calculate retention date
        retention_date = datetime.utcnow() + timedelta(days=retention_days)
        
        lineage_record = DataLineage(
            lineage_id=lineage_id,
            source_file=source_file,
            source_hash=source_hash,
            transformations=transformations,
            destination_table=destination_table,
            processing_time=datetime.utcnow(),
            data_quality_score=data_quality_score,
            compliance_checks=compliance_checks,
            retention_date=retention_date
        )
        
        self.lineage_records.append(lineage_record)
        
        self.logger.info(
            "Data lineage recorded",
            extra={
                "lineage_id": lineage_id,
                "source_file": source_file,
                "destination_table": destination_table,
                "data_quality_score": data_quality_score,
                "retention_date": retention_date.isoformat()
            }
        )
        
        return lineage_id
    
    def check_compliance(
        self,
        data_classification: DataClassification,
        compliance_level: ComplianceLevel,
        action: str
    ) -> bool:
        """
        Check if action is compliant with regulations
        
        Args:
            data_classification: Classification of data
            compliance_level: Required compliance level
            action: Action to be performed
            
        Returns:
            True if compliant, False otherwise
        """
        compliance_rules = self.compliance_settings.get(compliance_level.value, {})
        
        # Check data classification requirements
        allowed_classifications = compliance_rules.get("allowed_classifications", [])
        if data_classification.value not in allowed_classifications:
            self.logger.warning(
                f"Compliance violation: {data_classification.value} not allowed for {compliance_level.value}"
            )
            return False
        
        # Check action permissions
        allowed_actions = compliance_rules.get("allowed_actions", [])
        if action not in allowed_actions:
            self.logger.warning(
                f"Compliance violation: {action} not allowed for {compliance_level.value}"
            )
            return False
        
        return True
    
    def generate_audit_report(
        self,
        start_date: datetime,
        end_date: datetime,
        compliance_level: Optional[ComplianceLevel] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive audit report
        
        Args:
            start_date: Report start date
            end_date: Report end date
            compliance_level: Filter by compliance level
            
        Returns:
            Audit report dictionary
        """
        # Filter events by date range
        filtered_events = [
            event for event in self.audit_events
            if start_date <= event.timestamp <= end_date
        ]
        
        # Filter by compliance level if specified
        if compliance_level:
            filtered_events = [
                event for event in filtered_events
                if event.compliance_level == compliance_level
            ]
        
        # Generate report statistics
        total_events = len(filtered_events)
        successful_events = len([e for e in filtered_events if e.success])
        failed_events = total_events - successful_events
        
        # Group by action type
        action_summary = {}
        for event in filtered_events:
            action_summary[event.action] = action_summary.get(event.action, 0) + 1
        
        # Group by data classification
        classification_summary = {}
        for event in filtered_events:
            cls = event.data_classification.value
            classification_summary[cls] = classification_summary.get(cls, 0) + 1
        
        report = {
            "report_id": str(uuid.uuid4()),
            "generated_at": datetime.utcnow().isoformat(),
            "period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "summary": {
                "total_events": total_events,
                "successful_events": successful_events,
                "failed_events": failed_events,
                "success_rate": successful_events / total_events if total_events > 0 else 0
            },
            "action_summary": action_summary,
            "classification_summary": classification_summary,
            "compliance_level": compliance_level.value if compliance_level else "ALL",
            "events": [asdict(event) for event in filtered_events]
        }
        
        return report
    
    def check_retention_compliance(self) -> List[Dict[str, Any]]:
        """
        Check data retention compliance and identify records for deletion
        
        Returns:
            List of records that should be deleted
        """
        current_time = datetime.utcnow()
        expired_records = []
        
        for lineage in self.lineage_records:
            if current_time > lineage.retention_date:
                expired_records.append({
                    "lineage_id": lineage.lineage_id,
                    "destination_table": lineage.destination_table,
                    "retention_date": lineage.retention_date.isoformat(),
                    "days_overdue": (current_time - lineage.retention_date).days
                })
        
        if expired_records:
            self.logger.warning(
                f"Found {len(expired_records)} records past retention date",
                extra={"expired_count": len(expired_records)}
            )
        
        return expired_records
    
    def _create_data_hash(self, resource: str, action: str, record_count: int) -> str:
        """Create hash for data integrity verification"""
        data_string = f"{resource}:{action}:{record_count}:{datetime.utcnow().isoformat()}"
        return hashlib.sha256(data_string.encode()).hexdigest()
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate hash of source file for lineage tracking"""
        try:
            with open(file_path, 'rb') as f:
                file_hash = hashlib.sha256()
                for chunk in iter(lambda: f.read(4096), b""):
                    file_hash.update(chunk)
                return file_hash.hexdigest()
        except FileNotFoundError:
            # For demo purposes, return a placeholder hash
            return hashlib.sha256(file_path.encode()).hexdigest()
    
    def export_audit_trail(self, output_path: str) -> None:
        """
        Export complete audit trail to file
        
        Args:
            output_path: Path to export file
        """
        audit_data = {
            "export_timestamp": datetime.utcnow().isoformat(),
            "total_events": len(self.audit_events),
            "total_lineage_records": len(self.lineage_records),
            "audit_events": [asdict(event) for event in self.audit_events],
            "lineage_records": [asdict(record) for record in self.lineage_records]
        }
        
        with open(output_path, 'w') as f:
            json.dump(audit_data, f, indent=2, default=str)
        
        self.logger.info(f"Audit trail exported to {output_path}")
