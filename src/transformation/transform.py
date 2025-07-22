"""Data transformation module for the Coventry DW pipeline (Silver/Gold layers)."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import json
import re

from ..utils import get_logger, config
from ..schema import SchemaManager
from ..monitoring import PipelineMonitor
from ..data_quality import DataQualityValidator

logger = get_logger(__name__)


class DataTransformer:
    """Handles data transformation from Bronze to Silver to Gold layers."""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
        self.monitor = PipelineMonitor()
        self.data_quality = DataQualityValidator()
        self.storage_config = config.get_storage_config()
        
    def clean_data(self, df: pd.DataFrame, source_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean and standardize data (Bronze -> Silver)."""
        logger.info(f"Cleaning data for: {source_name}")
        
        start_time = datetime.utcnow()
        original_rows = len(df)
        
        cleaning_stats = {
            "original_rows": original_rows,
            "null_handling": {},
            "type_conversions": {},
            "duplicates_removed": 0,
            "date_parsing": {}
        }
        
        # Handle missing values
        for column in df.columns:
            null_count = df[column].isnull().sum()
            if null_count > 0:
                cleaning_stats["null_handling"][column] = null_count
                
                # Strategy based on column type and name
                if 'amount' in column.lower() or 'balance' in column.lower():
                    df[column] = df[column].fillna(0)
                elif 'date' in column.lower():
                    df[column] = df[column].fillna(method='ffill')
                elif df[column].dtype == 'object':
                    df[column] = df[column].fillna('UNKNOWN')
                else:
                    df[column] = df[column].fillna(df[column].median())
        
        # Type conversions
        for column in df.columns:
            if column.startswith('_'):  # Skip metadata columns
                continue
                
            original_type = str(df[column].dtype)
            
            # Convert amount/balance columns to float
            if any(keyword in column.lower() for keyword in ['amount', 'balance', 'fee', 'charge']):
                try:
                    # Remove currency symbols and convert
                    if df[column].dtype == 'object':
                        df[column] = df[column].astype(str).str.replace(r'[£$€,]', '', regex=True)
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    cleaning_stats["type_conversions"][column] = f"{original_type} -> float64"
                except Exception as e:
                    logger.warning(f"Failed to convert {column} to numeric", error=str(e))
            
            # Convert date columns
            elif any(keyword in column.lower() for keyword in ['date', 'time', 'timestamp']):
                try:
                    df[column] = pd.to_datetime(df[column], errors='coerce', infer_datetime_format=True)
                    cleaning_stats["type_conversions"][column] = f"{original_type} -> datetime64"
                    cleaning_stats["date_parsing"][column] = {
                        "parsed_successfully": df[column].notna().sum(),
                        "failed_to_parse": df[column].isna().sum()
                    }
                except Exception as e:
                    logger.warning(f"Failed to convert {column} to datetime", error=str(e))
            
            # Convert ID columns to string
            elif 'id' in column.lower():
                df[column] = df[column].astype(str)
                cleaning_stats["type_conversions"][column] = f"{original_type} -> string"
        
        # Remove duplicates (excluding metadata columns)
        business_columns = [col for col in df.columns if not col.startswith('_')]
        initial_count = len(df)
        df = df.drop_duplicates(subset=business_columns)
        duplicates_removed = initial_count - len(df)
        cleaning_stats["duplicates_removed"] = duplicates_removed
        
        # Add data quality flags
        df['_data_quality_score'] = self._calculate_quality_score(df)
        df['_cleaning_timestamp'] = datetime.utcnow()
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        cleaning_stats.update({
            "final_rows": len(df),
            "processing_time": processing_time,
            "data_quality_avg_score": df['_data_quality_score'].mean()
        })
        
        logger.log_data_processing(
            stage="data_cleaning",
            input_rows=original_rows,
            output_rows=len(df),
            processing_time=processing_time,
            source_name=source_name,
            duplicates_removed=duplicates_removed
        )
        
        return df, cleaning_stats
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate data quality score for each row."""
        business_columns = [col for col in df.columns if not col.startswith('_')]
        
        # Calculate completeness score (0-1)
        completeness = df[business_columns].notna().sum(axis=1) / len(business_columns)
        
        # Additional quality checks can be added here
        # For now, quality score is based on completeness
        return completeness
    
    def enrich_data(self, df: pd.DataFrame, source_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Enrich data with additional features and categorizations."""
        logger.info(f"Enriching data for: {source_name}")
        
        start_time = datetime.utcnow()
        enrichment_stats = {
            "features_added": [],
            "categorization_rules_applied": 0
        }
        
        # Transaction categorization (if transaction data)
        if 'transaction' in source_name.lower() and 'description' in df.columns:
            df['transaction_category'] = self._categorize_transactions(df['description'])
            enrichment_stats["features_added"].append("transaction_category")
            enrichment_stats["categorization_rules_applied"] = len(df)
        
        # Large transaction flags
        if 'amount' in df.columns:
            amount_threshold = df['amount'].quantile(0.95)  # Top 5% as large transactions
            df['is_large_transaction'] = df['amount'] > amount_threshold
            df['amount_percentile'] = df['amount'].rank(pct=True)
            enrichment_stats["features_added"].extend(["is_large_transaction", "amount_percentile"])
        
        # Time-based features
        date_columns = [col for col in df.columns if 'date' in col.lower() and df[col].dtype == 'datetime64[ns]']
        for date_col in date_columns:
            base_name = date_col.replace('_date', '').replace('date', '')
            df[f'{base_name}_year'] = df[date_col].dt.year
            df[f'{base_name}_month'] = df[date_col].dt.month
            df[f'{base_name}_day_of_week'] = df[date_col].dt.dayofweek
            df[f'{base_name}_is_weekend'] = df[date_col].dt.dayofweek >= 5
            enrichment_stats["features_added"].extend([
                f'{base_name}_year', f'{base_name}_month', 
                f'{base_name}_day_of_week', f'{base_name}_is_weekend'
            ])
        
        # Account-based features (if account data)
        if 'account' in source_name.lower() and 'balance' in df.columns:
            df['balance_category'] = pd.cut(df['balance'], 
                                          bins=[-np.inf, 0, 1000, 10000, 50000, np.inf],
                                          labels=['Negative', 'Low', 'Medium', 'High', 'Very High'])
            enrichment_stats["features_added"].append("balance_category")
        
        # Add enrichment metadata
        df['_enrichment_timestamp'] = datetime.utcnow()
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        enrichment_stats["processing_time"] = processing_time
        
        logger.log_data_processing(
            stage="data_enrichment",
            input_rows=len(df),
            output_rows=len(df),
            processing_time=processing_time,
            source_name=source_name,
            features_added=len(enrichment_stats["features_added"])
        )
        
        return df, enrichment_stats
    
    def _categorize_transactions(self, descriptions: pd.Series) -> pd.Series:
        """Categorize transactions based on description patterns."""
        categories = []
        
        # Define categorization rules
        category_rules = {
            'Grocery': r'(supermarket|grocery|tesco|sainsbury|asda|morrisons|aldi|lidl)',
            'Fuel': r'(petrol|fuel|gas station|shell|bp|esso)',
            'Restaurant': r'(restaurant|cafe|mcdonald|kfc|pizza|takeaway)',
            'Retail': r'(amazon|ebay|shop|store|retail|purchase)',
            'Utilities': r'(electric|gas|water|council tax|utility)',
            'Transport': r'(train|bus|taxi|uber|transport|parking)',
            'Healthcare': r'(pharmacy|hospital|doctor|medical|health)',
            'Entertainment': r'(cinema|theatre|netflix|spotify|entertainment)',
            'ATM': r'(atm|cash withdrawal|cashpoint)',
            'Transfer': r'(transfer|payment to|standing order|direct debit)',
            'Fee': r'(fee|charge|overdraft|interest)'
        }
        
        for desc in descriptions:
            if pd.isna(desc):
                categories.append('Unknown')
                continue
                
            desc_lower = str(desc).lower()
            category_found = False
            
            for category, pattern in category_rules.items():
                if re.search(pattern, desc_lower):
                    categories.append(category)
                    category_found = True
                    break
            
            if not category_found:
                categories.append('Other')
        
        return pd.Series(categories, index=descriptions.index)
    
    def create_aggregations(self, df: pd.DataFrame, source_name: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Create aggregated views for Gold layer."""
        logger.info(f"Creating aggregations for: {source_name}")
        
        start_time = datetime.utcnow()
        aggregations = []
        
        # Monthly aggregations by account (if applicable)
        if all(col in df.columns for col in ['account_id', 'amount']) and 'transaction_date' in df.columns:
            monthly_agg = df.groupby([
                'account_id',
                df['transaction_date'].dt.to_period('M').astype(str)
            ]).agg({
                'amount': ['sum', 'mean', 'count', 'std'],
                'is_large_transaction': 'sum',
                'transaction_category': lambda x: x.value_counts().to_dict()
            }).round(2)
            
            # Flatten column names
            monthly_agg.columns = ['_'.join(col).strip() for col in monthly_agg.columns]
            monthly_agg = monthly_agg.reset_index()
            monthly_agg['aggregation_type'] = 'monthly_by_account'
            monthly_agg['_aggregation_timestamp'] = datetime.utcnow()
            
            aggregations.append(('monthly_by_account', monthly_agg))
        
        # Category-based aggregations
        if 'transaction_category' in df.columns and 'amount' in df.columns:
            category_agg = df.groupby('transaction_category').agg({
                'amount': ['sum', 'mean', 'count', 'min', 'max'],
                'account_id': 'nunique'
            }).round(2)
            
            category_agg.columns = ['_'.join(col).strip() for col in category_agg.columns]
            category_agg = category_agg.reset_index()
            category_agg['aggregation_type'] = 'by_category'
            category_agg['_aggregation_timestamp'] = datetime.utcnow()
            
            aggregations.append(('by_category', category_agg))
        
        # Daily aggregations
        if 'transaction_date' in df.columns and 'amount' in df.columns:
            daily_agg = df.groupby(df['transaction_date'].dt.date).agg({
                'amount': ['sum', 'mean', 'count'],
                'account_id': 'nunique',
                'is_large_transaction': 'sum' if 'is_large_transaction' in df.columns else 'count'
            }).round(2)
            
            daily_agg.columns = ['_'.join(col).strip() for col in daily_agg.columns]
            daily_agg = daily_agg.reset_index()
            daily_agg['aggregation_type'] = 'daily'
            daily_agg['_aggregation_timestamp'] = datetime.utcnow()
            
            aggregations.append(('daily', daily_agg))
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        agg_stats = {
            "aggregations_created": len(aggregations),
            "aggregation_types": [agg[0] for agg in aggregations],
            "processing_time": processing_time
        }
        
        logger.log_data_processing(
            stage="data_aggregation",
            input_rows=len(df),
            output_rows=sum(len(agg[1]) for agg in aggregations),
            processing_time=processing_time,
            source_name=source_name,
            aggregations_created=len(aggregations)
        )
        
        return aggregations, agg_stats
    
    def save_to_silver(self, df: pd.DataFrame, source_name: str, metadata: Dict[str, Any]) -> Path:
        """Save cleaned and enriched data to Silver layer."""
        logger.info(f"Saving data to Silver layer: {source_name}")
        
        silver_path = Path(self.storage_config.get('silver_path', 'output/silver'))
        
        # Add partitioning
        current_date = datetime.utcnow()
        partition_path = silver_path / f"year={current_date.year}" / f"month={current_date.month:02d}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Save as Parquet
        output_file = partition_path / f"{source_name}_silver_{current_date.strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_file, index=False)
        
        # Save metadata
        metadata_file = partition_path / f"{source_name}_silver_{current_date.strftime('%Y%m%d_%H%M%S')}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Data saved to Silver: {output_file}", 
                   rows=len(df), file_size_mb=output_file.stat().st_size / 1024 / 1024)
        
        return output_file
    
    def save_to_gold(self, aggregations: List[Tuple[str, pd.DataFrame]], 
                     source_name: str, metadata: Dict[str, Any]) -> List[Path]:
        """Save aggregated data to Gold layer."""
        logger.info(f"Saving aggregations to Gold layer: {source_name}")
        
        gold_path = Path(self.storage_config.get('gold_path', 'output/gold'))
        output_files = []
        
        current_date = datetime.utcnow()
        
        for agg_type, agg_df in aggregations:
            # Create partition path
            partition_path = gold_path / agg_type / f"year={current_date.year}" / f"month={current_date.month:02d}"
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Save aggregation
            output_file = partition_path / f"{source_name}_{agg_type}_{current_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            agg_df.to_parquet(output_file, index=False)
            output_files.append(output_file)
            
            logger.info(f"Aggregation saved to Gold: {output_file}", 
                       aggregation_type=agg_type, rows=len(agg_df))
        
        # Save combined metadata
        metadata_file = gold_path / f"{source_name}_gold_{current_date.strftime('%Y%m%d_%H%M%S')}_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        return output_files
    
    def run_transformation_pipeline(self, run_id: str, bronze_files: List[Path]) -> Dict[str, Any]:
        """Run the complete transformation pipeline."""
        logger.log_pipeline_start("transformation", run_id)
        start_time = datetime.utcnow()
        
        results = {
            "run_id": run_id,
            "start_time": start_time.isoformat(),
            "files_processed": [],
            "total_rows_processed": 0,
            "status": "running"
        }
        
        try:
            for bronze_file in bronze_files:
                source_name = bronze_file.stem.split('_')[0]  # Extract source name from filename
                logger.info(f"Processing Bronze file: {bronze_file}")
                
                try:
                    # Load Bronze data
                    df = pd.read_parquet(bronze_file)
                    original_rows = len(df)
                    
                    # Data cleaning (Bronze -> Silver)
                    cleaned_df, cleaning_stats = self.clean_data(df, source_name)
                    
                    # Data validation
                    is_valid, validation_result = self.data_quality.validate_data(cleaned_df, source_name)
                    
                    if not is_valid and config.get_data_quality_config().get('fail_on_error', False):
                        raise ValueError(f"Data quality validation failed: {validation_result}")
                    
                    # Data enrichment
                    enriched_df, enrichment_stats = self.enrich_data(cleaned_df, source_name)
                    
                    # Save to Silver
                    silver_metadata = {
                        "cleaning_stats": cleaning_stats,
                        "enrichment_stats": enrichment_stats,
                        "validation_result": validation_result,
                        "transformation_timestamp": datetime.utcnow().isoformat()
                    }
                    silver_file = self.save_to_silver(enriched_df, source_name, silver_metadata)
                    
                    # Create aggregations (Silver -> Gold)
                    aggregations, agg_stats = self.create_aggregations(enriched_df, source_name)
                    
                    # Save to Gold
                    gold_metadata = {
                        "aggregation_stats": agg_stats,
                        "source_silver_file": str(silver_file),
                        "aggregation_timestamp": datetime.utcnow().isoformat()
                    }
                    gold_files = self.save_to_gold(aggregations, source_name, gold_metadata)
                    
                    # Update results
                    results["files_processed"].append({
                        "source_name": source_name,
                        "bronze_file": str(bronze_file),
                        "silver_file": str(silver_file),
                        "gold_files": [str(f) for f in gold_files],
                        "original_rows": original_rows,
                        "final_rows": len(enriched_df),
                        "aggregations_created": len(aggregations),
                        "data_quality_passed": is_valid,
                        "status": "success"
                    })
                    results["total_rows_processed"] += original_rows
                    
                except Exception as e:
                    logger.error(f"Failed to process Bronze file: {bronze_file}", error=str(e))
                    results["files_processed"].append({
                        "source_name": source_name,
                        "bronze_file": str(bronze_file),
                        "status": "failed",
                        "error": str(e)
                    })
            
            # Calculate final results
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "status": "completed"
            })
            
            logger.log_pipeline_end("transformation", run_id, "completed", duration)
            
            return results
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            results.update({
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "status": "failed",
                "error": str(e)
            })
            
            logger.error("Transformation pipeline failed", error=str(e), run_id=run_id)
            logger.log_pipeline_end("transformation", run_id, "failed", duration)
            
            return results


def main():
    """Main entry point for transformation pipeline."""
    import uuid
    from pathlib import Path
    
    transformer = DataTransformer()
    run_id = str(uuid.uuid4())
    
    # Find Bronze files to process
    from ..utils.config import config
    bronze_path = Path(config.get_storage_config().get('bronze_path', 'output/bronze'))
    bronze_files = list(bronze_path.rglob("*.parquet")) if bronze_path.exists() else []
    
    if not bronze_files:
        print("No Bronze files found to process")
        return
    
    results = transformer.run_transformation_pipeline(run_id, bronze_files)
    
    print(f"Transformation pipeline completed with status: {results['status']}")
    print(f"Total rows processed: {results['total_rows_processed']}")
    
    return results


if __name__ == "__main__":
    main()
