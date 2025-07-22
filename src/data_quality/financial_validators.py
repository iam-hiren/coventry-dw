"""
Financial Services Specific Data Quality Validators

This module provides specialized data quality validation rules
for financial services data, including regulatory compliance checks.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

from ..compliance.audit_manager import AuditManager, DataClassification, ComplianceLevel


class FinancialValidators:
    """
    Financial services specific data validators
    """
    
    def __init__(self, audit_manager: Optional[AuditManager] = None):
        self.audit_manager = audit_manager
        self.logger = logging.getLogger(__name__)
        
        # UK financial validation patterns
        self.uk_sort_code_pattern = re.compile(r'^\d{2}-\d{2}-\d{2}$')
        self.uk_account_number_pattern = re.compile(r'^\d{8}$')
        self.iban_pattern = re.compile(r'^[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}$')
        self.swift_bic_pattern = re.compile(r'^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$')
        
        # Financial amount validation
        self.max_transaction_amount = Decimal('1000000.00')  # £1M limit
        self.min_transaction_amount = Decimal('0.01')  # 1p minimum
    
    @staticmethod
    def validate_uk_sort_code(sort_code: str) -> bool:
        """
        Validate UK sort code format (XX-XX-XX)
        
        Args:
            sort_code: Sort code to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(sort_code, str):
            return False
        return bool(re.match(r'^\d{2}-\d{2}-\d{2}$', sort_code))
    
    @staticmethod
    def validate_uk_account_number(account_number: str) -> bool:
        """
        Validate UK account number (8 digits)
        
        Args:
            account_number: Account number to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(account_number, str):
            return False
        return bool(re.match(r'^\d{8}$', account_number))
    
    @staticmethod
    def validate_iban(iban: str) -> bool:
        """
        Validate International Bank Account Number (IBAN)
        
        Args:
            iban: IBAN to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(iban, str):
            return False
        
        # Remove spaces and convert to uppercase
        iban = iban.replace(' ', '').upper()
        
        # Check format
        if not re.match(r'^[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}$', iban):
            return False
        
        # IBAN checksum validation (simplified)
        # Move first 4 characters to end
        rearranged = iban[4:] + iban[:4]
        
        # Replace letters with numbers (A=10, B=11, ..., Z=35)
        numeric_string = ''
        for char in rearranged:
            if char.isalpha():
                numeric_string += str(ord(char) - ord('A') + 10)
            else:
                numeric_string += char
        
        # Check if mod 97 equals 1
        try:
            return int(numeric_string) % 97 == 1
        except ValueError:
            return False
    
    @staticmethod
    def validate_swift_bic(swift_bic: str) -> bool:
        """
        Validate SWIFT/BIC code
        
        Args:
            swift_bic: SWIFT/BIC code to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not isinstance(swift_bic, str):
            return False
        return bool(re.match(r'^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$', swift_bic.upper()))
    
    @staticmethod
    def validate_financial_amount(amount: Any, min_amount: float = 0.01, max_amount: float = 1000000.00) -> bool:
        """
        Validate financial amount
        
        Args:
            amount: Amount to validate
            min_amount: Minimum allowed amount
            max_amount: Maximum allowed amount
            
        Returns:
            True if valid, False otherwise
        """
        try:
            decimal_amount = Decimal(str(amount))
            return Decimal(str(min_amount)) <= decimal_amount <= Decimal(str(max_amount))
        except (InvalidOperation, ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_currency_code(currency_code: str) -> bool:
        """
        Validate ISO 4217 currency code
        
        Args:
            currency_code: Currency code to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Common currency codes for UK financial services
        valid_currencies = {
            'GBP', 'USD', 'EUR', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD',
            'SEK', 'NOK', 'DKK', 'PLN', 'CZK', 'HUF', 'SGD', 'HKD'
        }
        
        if not isinstance(currency_code, str):
            return False
        
        return currency_code.upper() in valid_currencies
    
    @staticmethod
    def validate_transaction_date(transaction_date: Any) -> bool:
        """
        Validate transaction date (not in future, not too old)
        
        Args:
            transaction_date: Date to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            if isinstance(transaction_date, str):
                parsed_date = datetime.strptime(transaction_date, '%Y-%m-%d').date()
            elif isinstance(transaction_date, datetime):
                parsed_date = transaction_date.date()
            elif isinstance(transaction_date, date):
                parsed_date = transaction_date
            else:
                return False
            
            today = date.today()
            # Not in future, not older than 10 years
            return (parsed_date <= today) and (parsed_date >= date(today.year - 10, 1, 1))
            
        except (ValueError, TypeError):
            return False
    
    def create_transaction_schema(self) -> DataFrameSchema:
        """
        Create Pandera schema for financial transaction validation
        
        Returns:
            DataFrameSchema for transaction validation
        """
        return DataFrameSchema({
            "transaction_id": Column(
                str,
                checks=[
                    Check.str_length(min_value=1, max_value=50),
                    Check.str_matches(r'^[A-Z0-9\-_]+$')
                ],
                nullable=False,
                description="Unique transaction identifier"
            ),
            "account_number": Column(
                str,
                checks=[
                    Check(self.validate_uk_account_number, 
                         error="Invalid UK account number format")
                ],
                nullable=False,
                description="UK account number (8 digits)"
            ),
            "sort_code": Column(
                str,
                checks=[
                    Check(self.validate_uk_sort_code,
                         error="Invalid UK sort code format (XX-XX-XX)")
                ],
                nullable=False,
                description="UK sort code"
            ),
            "amount": Column(
                float,
                checks=[
                    Check(lambda x: self.validate_financial_amount(x),
                         error="Invalid transaction amount")
                ],
                nullable=False,
                description="Transaction amount"
            ),
            "currency": Column(
                str,
                checks=[
                    Check(self.validate_currency_code,
                         error="Invalid ISO 4217 currency code")
                ],
                nullable=False,
                description="ISO 4217 currency code"
            ),
            "transaction_date": Column(
                str,  # Will be converted to date
                checks=[
                    Check(self.validate_transaction_date,
                         error="Invalid transaction date")
                ],
                nullable=False,
                description="Transaction date (YYYY-MM-DD)"
            ),
            "transaction_type": Column(
                str,
                checks=[
                    Check.isin(['CREDIT', 'DEBIT', 'TRANSFER', 'PAYMENT', 'WITHDRAWAL', 'DEPOSIT'])
                ],
                nullable=False,
                description="Type of transaction"
            ),
            "description": Column(
                str,
                checks=[
                    Check.str_length(max_value=255)
                ],
                nullable=True,
                description="Transaction description"
            ),
            "counterparty_name": Column(
                str,
                checks=[
                    Check.str_length(max_value=100)
                ],
                nullable=True,
                description="Counterparty name"
            ),
            "counterparty_account": Column(
                str,
                checks=[
                    Check(lambda x: pd.isna(x) or self.validate_uk_account_number(x) or self.validate_iban(x),
                         error="Invalid counterparty account format")
                ],
                nullable=True,
                description="Counterparty account number or IBAN"
            )
        })
    
    def create_customer_schema(self) -> DataFrameSchema:
        """
        Create Pandera schema for customer data validation
        
        Returns:
            DataFrameSchema for customer validation
        """
        return DataFrameSchema({
            "customer_id": Column(
                str,
                checks=[
                    Check.str_length(min_value=1, max_value=20),
                    Check.str_matches(r'^[A-Z0-9]+$')
                ],
                nullable=False,
                unique=True,
                description="Unique customer identifier"
            ),
            "title": Column(
                str,
                checks=[
                    Check.isin(['Mr', 'Mrs', 'Miss', 'Ms', 'Dr', 'Prof', 'Rev'])
                ],
                nullable=True,
                description="Customer title"
            ),
            "first_name": Column(
                str,
                checks=[
                    Check.str_length(min_value=1, max_value=50),
                    Check.str_matches(r'^[A-Za-z\s\-\']+$')
                ],
                nullable=False,
                description="Customer first name"
            ),
            "last_name": Column(
                str,
                checks=[
                    Check.str_length(min_value=1, max_value=50),
                    Check.str_matches(r'^[A-Za-z\s\-\']+$')
                ],
                nullable=False,
                description="Customer last name"
            ),
            "date_of_birth": Column(
                str,
                checks=[
                    Check(lambda x: self._validate_date_of_birth(x),
                         error="Invalid date of birth (must be 18+ years old)")
                ],
                nullable=False,
                description="Date of birth (YYYY-MM-DD)"
            ),
            "email": Column(
                str,
                checks=[
                    Check.str_matches(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
                ],
                nullable=True,
                description="Customer email address"
            ),
            "phone": Column(
                str,
                checks=[
                    Check.str_matches(r'^(\+44|0)[1-9]\d{8,9}$')
                ],
                nullable=True,
                description="UK phone number"
            ),
            "postcode": Column(
                str,
                checks=[
                    Check.str_matches(r'^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$')
                ],
                nullable=True,
                description="UK postcode"
            ),
            "risk_rating": Column(
                str,
                checks=[
                    Check.isin(['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH'])
                ],
                nullable=False,
                description="Customer risk rating"
            )
        })
    
    def _validate_date_of_birth(self, dob: str) -> bool:
        """
        Validate date of birth (must be 18+ years old)
        
        Args:
            dob: Date of birth string
            
        Returns:
            True if valid, False otherwise
        """
        try:
            birth_date = datetime.strptime(dob, '%Y-%m-%d').date()
            today = date.today()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            return 18 <= age <= 120
        except ValueError:
            return False
    
    def validate_dataframe(
        self,
        df: pd.DataFrame,
        schema: DataFrameSchema,
        data_classification: DataClassification = DataClassification.CONFIDENTIAL
    ) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame against schema with audit logging
        
        Args:
            df: DataFrame to validate
            schema: Pandera schema
            data_classification: Data classification level
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        try:
            # Validate with Pandera
            validated_df = schema.validate(df, lazy=True)
            
            # Log successful validation
            if self.audit_manager:
                self.audit_manager.log_data_access(
                    user_id="data-pipeline",
                    resource=f"dataframe-{len(df)}-records",
                    action="VALIDATE",
                    data_classification=data_classification,
                    compliance_level=ComplianceLevel.FCA_RULES,
                    record_count=len(df),
                    metadata={"validation_result": "PASSED"}
                )
            
            self.logger.info(f"Data validation passed for {len(df)} records")
            return True, []
            
        except pa.errors.SchemaErrors as e:
            # Collect all validation errors
            for error in e.schema_errors:
                errors.append(str(error))
            
            # Log validation failure
            if self.audit_manager:
                self.audit_manager.log_data_access(
                    user_id="data-pipeline",
                    resource=f"dataframe-{len(df)}-records",
                    action="VALIDATE",
                    data_classification=data_classification,
                    compliance_level=ComplianceLevel.FCA_RULES,
                    record_count=len(df),
                    metadata={
                        "validation_result": "FAILED",
                        "error_count": len(errors),
                        "errors": errors[:10]  # Limit errors in metadata
                    }
                )
            
            self.logger.error(f"Data validation failed with {len(errors)} errors")
            return False, errors
    
    def check_suspicious_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for suspicious transaction patterns
        
        Args:
            df: Transaction DataFrame
            
        Returns:
            DataFrame with suspicious transactions flagged
        """
        suspicious_flags = []
        
        for _, row in df.iterrows():
            flags = []
            
            # Large amount transactions
            if float(row['amount']) > 10000:
                flags.append('LARGE_AMOUNT')
            
            # Round number transactions (potential structuring)
            if float(row['amount']) % 1000 == 0 and float(row['amount']) >= 5000:
                flags.append('ROUND_AMOUNT')
            
            # Weekend transactions
            try:
                trans_date = datetime.strptime(row['transaction_date'], '%Y-%m-%d')
                if trans_date.weekday() >= 5:  # Saturday or Sunday
                    flags.append('WEEKEND_TRANSACTION')
            except ValueError:
                pass
            
            # Multiple transactions same day (if we had customer grouping)
            # This would require additional logic with customer grouping
            
            suspicious_flags.append('|'.join(flags) if flags else None)
        
        df_copy = df.copy()
        df_copy['suspicious_flags'] = suspicious_flags
        
        # Log suspicious activity
        suspicious_count = sum(1 for flag in suspicious_flags if flag)
        if suspicious_count > 0 and self.audit_manager:
            self.audit_manager.log_data_access(
                user_id="aml-system",
                resource="transaction-monitoring",
                action="SUSPICIOUS_ACTIVITY_CHECK",
                data_classification=DataClassification.RESTRICTED,
                compliance_level=ComplianceLevel.FCA_RULES,
                record_count=suspicious_count,
                metadata={
                    "total_transactions": len(df),
                    "suspicious_transactions": suspicious_count,
                    "suspicious_rate": suspicious_count / len(df)
                }
            )
        
        return df_copy
