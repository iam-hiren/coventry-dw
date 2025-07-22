"""Schema management and validation for the Coventry DW pipeline."""

import json
import pandas as pd
import pandera as pa
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from ..utils import get_logger

logger = get_logger(__name__)


@dataclass
class SchemaField:
    """Represents a schema field definition."""
    name: str
    dtype: str
    nullable: bool = True
    description: str = ""
    constraints: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.constraints is None:
            self.constraints = {}


@dataclass
class SchemaVersion:
    """Represents a schema version."""
    version: str
    created_at: str
    fields: List[SchemaField]
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class SchemaManager:
    """Manages schema definitions, validation, and evolution."""
    
    def __init__(self, schema_dir: str = "schemas"):
        self.schema_dir = Path(schema_dir)
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        
    def auto_detect_schema(self, df: pd.DataFrame, schema_name: str) -> SchemaVersion:
        """Auto-detect schema from DataFrame."""
        logger.info(f"Auto-detecting schema for: {schema_name}")
        
        fields = []
        for col_name, dtype in df.dtypes.items():
            field = SchemaField(
                name=col_name,
                dtype=str(dtype),
                nullable=df[col_name].isnull().any(),
                description=f"Auto-detected field: {col_name}"
            )
            
            # Add basic constraints based on data
            if pd.api.types.is_numeric_dtype(dtype):
                field.constraints = {
                    "min_value": float(df[col_name].min()),
                    "max_value": float(df[col_name].max())
                }
            elif pd.api.types.is_string_dtype(dtype):
                field.constraints = {
                    "max_length": int(df[col_name].str.len().max()) if not df[col_name].empty else 0
                }
            
            fields.append(field)
        
        schema_version = SchemaVersion(
            version="1.0.0",
            created_at=datetime.utcnow().isoformat(),
            fields=fields,
            metadata={
                "auto_detected": True,
                "sample_rows": len(df),
                "detection_timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Detected schema with {len(fields)} fields", 
                   schema_name=schema_name, field_count=len(fields))
        
        return schema_version
    
    def save_schema(self, schema_name: str, schema_version: SchemaVersion) -> Path:
        """Save schema to file."""
        schema_file = self.schema_dir / f"{schema_name}_schema.json"
        
        # Convert to dict for JSON serialization
        schema_dict = {
            "version": schema_version.version,
            "created_at": schema_version.created_at,
            "fields": [asdict(field) for field in schema_version.fields],
            "metadata": schema_version.metadata
        }
        
        with open(schema_file, 'w') as f:
            json.dump(schema_dict, f, indent=2)
        
        logger.info(f"Schema saved: {schema_file}", schema_name=schema_name)
        return schema_file
    
    def load_schema(self, schema_name: str) -> Optional[SchemaVersion]:
        """Load schema from file."""
        schema_file = self.schema_dir / f"{schema_name}_schema.json"
        
        if not schema_file.exists():
            logger.warning(f"Schema file not found: {schema_file}")
            return None
        
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        
        # Convert back to SchemaVersion object
        fields = [SchemaField(**field_dict) for field_dict in schema_dict["fields"]]
        
        schema_version = SchemaVersion(
            version=schema_dict["version"],
            created_at=schema_dict["created_at"],
            fields=fields,
            metadata=schema_dict.get("metadata", {})
        )
        
        logger.info(f"Schema loaded: {schema_file}", schema_name=schema_name)
        return schema_version
    
    def compare_schemas(self, current_schema: SchemaVersion, 
                       reference_schema: SchemaVersion) -> Dict[str, Any]:
        """Compare two schemas and return differences."""
        logger.info("Comparing schemas for differences")
        
        current_fields = {field.name: field for field in current_schema.fields}
        reference_fields = {field.name: field for field in reference_schema.fields}
        
        differences = {
            "added_fields": [],
            "removed_fields": [],
            "modified_fields": [],
            "type_changes": [],
            "nullable_changes": []
        }
        
        # Find added fields
        for field_name in current_fields:
            if field_name not in reference_fields:
                differences["added_fields"].append(field_name)
        
        # Find removed fields
        for field_name in reference_fields:
            if field_name not in current_fields:
                differences["removed_fields"].append(field_name)
        
        # Find modified fields
        for field_name in current_fields:
            if field_name in reference_fields:
                current_field = current_fields[field_name]
                reference_field = reference_fields[field_name]
                
                if current_field.dtype != reference_field.dtype:
                    differences["type_changes"].append({
                        "field": field_name,
                        "old_type": reference_field.dtype,
                        "new_type": current_field.dtype
                    })
                
                if current_field.nullable != reference_field.nullable:
                    differences["nullable_changes"].append({
                        "field": field_name,
                        "old_nullable": reference_field.nullable,
                        "new_nullable": current_field.nullable
                    })
        
        # Save schema diff
        diff_file = self.schema_dir / "schema_diff.json"
        with open(diff_file, 'w') as f:
            json.dump(differences, f, indent=2)
        
        logger.info("Schema comparison completed", 
                   added_fields=len(differences["added_fields"]),
                   removed_fields=len(differences["removed_fields"]),
                   type_changes=len(differences["type_changes"]))
        
        return differences
    
    def create_pandera_schema(self, schema_version: SchemaVersion) -> pa.DataFrameSchema:
        """Create a Pandera schema from SchemaVersion."""
        columns = {}
        
        for field in schema_version.fields:
            # Map pandas dtypes to Pandera types
            if field.dtype.startswith('int'):
                pa_type = pa.Int64
            elif field.dtype.startswith('float'):
                pa_type = pa.Float64
            elif field.dtype == 'object' or field.dtype.startswith('string'):
                pa_type = pa.String
            elif field.dtype.startswith('datetime'):
                pa_type = pa.DateTime
            elif field.dtype == 'bool':
                pa_type = pa.Bool
            else:
                pa_type = pa.String  # Default fallback
            
            # Create column with constraints
            checks = []
            if field.constraints:
                if 'min_value' in field.constraints:
                    checks.append(pa.Check.greater_than_or_equal_to(field.constraints['min_value']))
                if 'max_value' in field.constraints:
                    checks.append(pa.Check.less_than_or_equal_to(field.constraints['max_value']))
                if 'max_length' in field.constraints and pa_type == pa.String:
                    checks.append(pa.Check.str_length(max_val=field.constraints['max_length']))
            
            columns[field.name] = pa.Column(
                pa_type,
                nullable=field.nullable,
                checks=checks,
                description=field.description
            )
        
        return pa.DataFrameSchema(columns)
    
    def validate_dataframe(self, df: pd.DataFrame, schema_name: str) -> Tuple[bool, Dict[str, Any]]:
        """Validate DataFrame against schema."""
        logger.info(f"Validating DataFrame against schema: {schema_name}")
        
        schema_version = self.load_schema(schema_name)
        if not schema_version:
            logger.error(f"Schema not found: {schema_name}")
            return False, {"error": "Schema not found"}
        
        try:
            pandera_schema = self.create_pandera_schema(schema_version)
            validated_df = pandera_schema.validate(df)
            
            result = {
                "valid": True,
                "rows_validated": len(df),
                "schema_version": schema_version.version,
                "validation_timestamp": datetime.utcnow().isoformat()
            }
            
            logger.log_schema_validation(schema_name, result)
            return True, result
            
        except pa.errors.SchemaError as e:
            result = {
                "valid": False,
                "error": str(e),
                "failure_cases": e.failure_cases.to_dict('records') if hasattr(e, 'failure_cases') else [],
                "schema_version": schema_version.version,
                "validation_timestamp": datetime.utcnow().isoformat()
            }
            
            logger.error(f"Schema validation failed: {schema_name}", error=str(e))
            logger.log_schema_validation(schema_name, result)
            return False, result
