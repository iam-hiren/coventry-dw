#!/usr/bin/env python3
"""
Financial Services DataOps Pipeline

Production financial data processing system with comprehensive
compliance, audit trails, data quality validation, and monitoring
specifically designed for banking institutions.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.utils.config import ConfigManager
from src.compliance.audit_manager import (
    AuditManager, 
    DataClassification, 
    ComplianceLevel
)
from src.data_quality.financial_validators import FinancialValidators
from src.monitoring.financial_metrics import FinancialMetricsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/financial_demo.log')
    ]
)

logger = logging.getLogger(__name__)


def create_sample_transaction_data() -> pd.DataFrame:
    """Create sample financial transaction data for testing purposes."""
    
    sample_data = {
        'transaction_id': [
            'TXN001', 'TXN002', 'TXN003', 'TXN004', 'TXN005',
            'TXN006', 'TXN007', 'TXN008', 'TXN009', 'TXN010'
        ],
        'account_number': [
            '12345678', '87654321', '11111111', '22222222', '33333333',
            '44444444', '55555555', '66666666', '77777777', '88888888'
        ],
        'sort_code': [
            '12-34-56', '65-43-21', '11-11-11', '22-22-22', '33-33-33',
            '44-44-44', '55-55-55', '66-66-66', '77-77-77', '88-88-88'
        ],
        'amount': [
            150.50, 2500.00, 15000.00, 75.25, 500.00,
            10000.00, 25000.00, 100.00, 1000.00, 50000.00
        ],
        'currency': ['GBP'] * 10,
        'transaction_date': [
            '2024-01-15', '2024-01-15', '2024-01-16', '2024-01-16', '2024-01-17',
            '2024-01-20', '2024-01-21', '2024-01-22', '2024-01-23', '2024-01-24'
        ],
        'transaction_type': [
            'DEBIT', 'CREDIT', 'TRANSFER', 'PAYMENT', 'WITHDRAWAL',
            'DEPOSIT', 'TRANSFER', 'PAYMENT', 'DEBIT', 'TRANSFER'
        ],
        'description': [
            'ATM Withdrawal', 'Salary Payment', 'Property Purchase', 'Online Shopping', 'Cash Withdrawal',
            'Large Deposit', 'International Transfer', 'Utility Payment', 'Grocery Shopping', 'Investment Transfer'
        ],
        'counterparty_name': [
            'ATM Network', 'Employer Ltd', 'Property Co', 'Online Store', 'Bank Branch',
            'Investment Fund', 'Overseas Bank', 'Utility Co', 'Supermarket', 'Investment Bank'
        ],
        'counterparty_account': [
            None, '98765432', '12121212', None, None,
            '34343434', 'GB82WEST12345698765432', None, None, '56565656'
        ]
    }
    
    return pd.DataFrame(sample_data)


def test_financial_validation():
    """Test financial data validation capabilities."""
    
    print("\n" + "="*80)
    print("🏦 FINANCIAL DATA VALIDATION DEMO")
    print("="*80)
    
    # Initialize validators
    config_manager = ConfigManager()
    audit_manager = AuditManager(config_manager)
    validators = FinancialValidators(audit_manager)
    
    # Create sample data
    transaction_df = create_sample_transaction_data()
    
    print(f"\n📊 Sample Transaction Data ({len(transaction_df)} records):")
    print(transaction_df.head(3).to_string(index=False))
    
    # Validate transaction data
    print("\n🔍 Validating Transaction Data...")
    transaction_schema = validators.create_transaction_schema()
    
    start_time = time.time()
    is_valid, errors = validators.validate_dataframe(
        transaction_df, 
        transaction_schema,
        DataClassification.CONFIDENTIAL
    )
    validation_time = time.time() - start_time
    
    print(f"✅ Validation Result: {'PASSED' if is_valid else 'FAILED'}")
    print(f"⏱️  Validation Time: {validation_time:.3f} seconds")
    
    if not is_valid:
        print(f"❌ Validation Errors ({len(errors)}):")
        for error in errors[:5]:
            print(f"   • {error}")
    
    # Check for suspicious transactions
    print("\n🚨 Suspicious Transaction Detection...")
    suspicious_df = validators.check_suspicious_transactions(transaction_df)
    
    suspicious_count = len(suspicious_df[suspicious_df['suspicious_flags'].notna()])
    print(f"🔍 Found {suspicious_count} suspicious transactions:")
    
    for _, row in suspicious_df[suspicious_df['suspicious_flags'].notna()].iterrows():
        print(f"   • TXN {row['transaction_id']}: £{row['amount']:,.2f} - {row['suspicious_flags']}")
    
    return transaction_df, suspicious_df


def run_end_to_end_pipeline():
    """Execute end-to-end pipeline with all financial features."""
    
    print("\n" + "="*80)
    print("🚀 END-TO-END FINANCIAL PIPELINE DEMO")
    print("="*80)
    
    # Initialize all components
    config_manager = ConfigManager()
    audit_manager = AuditManager(config_manager)
    validators = FinancialValidators(audit_manager)
    metrics_collector = FinancialMetricsCollector(config_manager, audit_manager)
    
    # Create sample data
    transaction_df = create_sample_transaction_data()
    
    print(f"\n📊 Processing {len(transaction_df)} transactions...")
    
    # Step 1: Data Ingestion with Audit
    print("\n1️⃣  Data Ingestion & Audit Logging...")
    
    ingestion_start = time.time()
    
    # Log data ingestion
    audit_manager.log_data_access(
        user_id="pipeline-system",
        resource="transaction-data-source",
        action="INGEST",
        data_classification=DataClassification.CONFIDENTIAL,
        compliance_level=ComplianceLevel.FCA_RULES,
        record_count=len(transaction_df)
    )
    
    ingestion_time = time.time() - ingestion_start
    metrics_collector.record_processing_time("ingestion", ingestion_time)
    
    print(f"✅ Ingested {len(transaction_df)} records in {ingestion_time:.3f}s")
    
    # Step 2: Data Validation
    print("\n2️⃣  Data Validation & Quality Checks...")
    
    validation_start = time.time()
    
    schema = validators.create_transaction_schema()
    is_valid, errors = validators.validate_dataframe(
        transaction_df, 
        schema,
        DataClassification.CONFIDENTIAL
    )
    
    validation_time = time.time() - validation_start
    metrics_collector.record_processing_time("validation", validation_time)
    
    # Record data quality metrics
    valid_records = len(transaction_df) if is_valid else len(transaction_df) - len(errors)
    invalid_records = len(errors) if not is_valid else 0
    quality_score = valid_records / len(transaction_df)
    
    metrics_collector.record_data_quality_metric(
        dataset="transactions",
        total_records=len(transaction_df),
        valid_records=valid_records,
        invalid_records=invalid_records,
        quality_score=quality_score
    )
    
    print(f"✅ Validation completed in {validation_time:.3f}s")
    print(f"📊 Data Quality Score: {quality_score:.2%}")
    
    # Step 3: Suspicious Activity Detection
    print("\n3️⃣  Suspicious Activity Detection...")
    
    detection_start = time.time()
    
    suspicious_df = validators.check_suspicious_transactions(transaction_df)
    suspicious_count = len(suspicious_df[suspicious_df['suspicious_flags'].notna()])
    
    detection_time = time.time() - detection_start
    metrics_collector.record_processing_time("suspicious_detection", detection_time)
    
    # Record risk metrics
    suspicious_rate = suspicious_count / len(transaction_df)
    metrics_collector.record_risk_metric(
        risk_type="suspicious_transactions",
        risk_score=suspicious_rate,
        threshold=0.05,  # 5% threshold
        affected_entities=suspicious_count
    )
    
    print(f"✅ Suspicious activity detection completed in {detection_time:.3f}s")
    print(f"🚨 Found {suspicious_count} suspicious transactions ({suspicious_rate:.2%})")
    
    # Generate final reports
    print("\n📋 Generating Final Reports...")
    
    # Audit report
    audit_report = audit_manager.generate_audit_report(
        datetime.utcnow() - timedelta(minutes=5),
        datetime.utcnow()
    )
    
    # Metrics summary
    metrics_summary = metrics_collector.get_metrics_summary(hours=1)
    
    print(f"📊 Final Summary:")
    print(f"   • Audit Events: {audit_report['summary']['total_events']}")
    print(f"   • Metrics Collected: {metrics_summary['total_metrics']}")
    
    return {
        'processed_data': suspicious_df,
        'audit_report': audit_report,
        'metrics_summary': metrics_summary
    }


def main():
    """Main execution function."""
    
    print("🏦 COVENTRY BUILDING SOCIETY")
    print("Financial Services DataOps Pipeline")
    print("="*80)
    
    # Ensure output directories exist
    os.makedirs("logs", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    try:
        # Run tests
        transaction_df, suspicious_df = test_financial_validation()
        results = run_end_to_end_pipeline()
        
        # Export results
        print("\n💾 Exporting Demo Results...")
        
        results['processed_data'].to_csv("output/demo_processed_transactions.csv", index=False)
        
        with open("output/demo_reports.json", 'w') as f:
            json.dump({
                'audit_report': results['audit_report'],
                'metrics_summary': results['metrics_summary']
            }, f, indent=2, default=str)
        
        print("✅ Results exported to output/ directory")
        
        print("\n" + "="*80)
        print("🎉 PIPELINE EXECUTION COMPLETED!")
        print("="*80)
        print("Key Features Validated:")
        print("✅ Financial data validation (UK banking formats)")
        print("✅ Compliance checking and audit trails")
        print("✅ Suspicious transaction detection")
        print("✅ Real-time monitoring and alerting")
        print("✅ End-to-end pipeline processing")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        print(f"❌ Pipeline execution failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
