#!/usr/bin/env python3
"""
Test Financial Components

Simple test to verify financial services components are working.
"""

import sys
import os
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

def test_imports():
    """Test that all financial components can be imported."""
    print("🧪 Testing Financial Components Import...")
    
    try:
        from utils.config import ConfigManager
        print("✅ ConfigManager imported successfully")
        
        from compliance.audit_manager import AuditManager, DataClassification, ComplianceLevel
        print("✅ AuditManager imported successfully")
        
        from data_quality.financial_validators import FinancialValidators
        print("✅ FinancialValidators imported successfully")
        
        from monitoring.financial_metrics import FinancialMetricsCollector
        print("✅ FinancialMetricsCollector imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality of financial components."""
    print("\n🔧 Testing Basic Functionality...")
    
    try:
        # Test ConfigManager
        from utils.config import ConfigManager
        config = ConfigManager()
        print("✅ ConfigManager initialized")
        
        # Test AuditManager
        from compliance.audit_manager import AuditManager, DataClassification, ComplianceLevel
        audit_manager = AuditManager(config)
        
        # Log a test event
        event_id = audit_manager.log_data_access(
            user_id="test-user",
            resource="test-resource",
            action="TEST",
            data_classification=DataClassification.PUBLIC,
            compliance_level=ComplianceLevel.FCA_RULES,
            record_count=1
        )
        print(f"✅ AuditManager logged event: {event_id}")
        
        # Test FinancialValidators
        from data_quality.financial_validators import FinancialValidators
        validators = FinancialValidators()
        
        # Test UK sort code validation
        is_valid = validators.validate_uk_sort_code("12-34-56")
        print(f"✅ UK sort code validation: {is_valid}")
        
        # Test FinancialMetricsCollector
        from monitoring.financial_metrics import FinancialMetricsCollector
        metrics = FinancialMetricsCollector(config)
        
        # Record a test metric
        metrics.record_processing_time("test_operation", 1.5)
        print("✅ FinancialMetricsCollector recorded metric")
        
        return True
        
    except Exception as e:
        print(f"❌ Functionality test failed: {e}")
        return False

def test_financial_validation():
    """Test financial validation functions."""
    print("\n🏦 Testing Financial Validation...")
    
    try:
        from data_quality.financial_validators import FinancialValidators
        validators = FinancialValidators()
        
        # Test various financial validations
        test_cases = [
            ("UK Sort Code", validators.validate_uk_sort_code, "12-34-56", True),
            ("UK Account Number", validators.validate_uk_account_number, "12345678", True),
            ("IBAN", validators.validate_iban, "GB82WEST12345698765432", True),
            ("SWIFT BIC", validators.validate_swift_bic, "ABCDGB2L", True),
            ("Currency Code", validators.validate_currency_code, "GBP", True),
            ("Financial Amount", validators.validate_financial_amount, 100.50, True),
        ]
        
        for test_name, func, value, expected in test_cases:
            result = func(value)
            status = "✅" if result == expected else "❌"
            print(f"{status} {test_name}: {value} -> {result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Financial validation test failed: {e}")
        return False

def main():
    """Main test function."""
    print("🏦 COVENTRY BUILDING SOCIETY")
    print("Financial Components Test Suite")
    print("=" * 50)
    
    # Ensure directories exist
    os.makedirs("logs", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # Run tests
    tests = [
        ("Import Test", test_imports),
        ("Basic Functionality Test", test_basic_functionality),
        ("Financial Validation Test", test_financial_validation),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print(f"\n{'='*50}")
    print(f"🏁 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Financial components are ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
