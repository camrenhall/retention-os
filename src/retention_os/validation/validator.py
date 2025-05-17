"""
Data validation for RetentionOS data processing.
"""
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd
from loguru import logger

from retention_os.utils.utils import format_error_message


class Validator:
    """
    Validates data against validation rules and generates validation reports.
    """
    
    def __init__(self, validation_rules: Dict, strict: bool = False):
        """
        Initialize the validator with validation rules.
        
        Args:
            validation_rules: Dictionary of validation rules by entity and field
            strict: If True, fail on validation errors; if False, log warnings
        """
        self.validation_rules = validation_rules
        self.strict = strict
        self.errors = []
        self.warnings = []
    
    def validate_entity(self, entity_type: str, data: pd.DataFrame) -> Tuple[pd.DataFrame, bool]:
        """
        Validate a DataFrame against validation rules for an entity type.
        
        Args:
            entity_type: Type of entity to validate
            data: DataFrame to validate
            
        Returns:
            Tuple[pd.DataFrame, bool]: Clean DataFrame and validation success flag
        """
        if entity_type not in self.validation_rules:
            return data, True
        
        entity_rules = self.validation_rules[entity_type]
        clean_data = data.copy()
        valid = True
        
        # Track validation issues
        validation_issues = []
        
        # Check for required fields
        for field, rules in entity_rules.items():
            if field not in clean_data.columns and rules.get("required", False):
                msg = f"Required field '{field}' is missing"
                if self.strict:
                    validation_issues.append({"field": field, "error": msg})
                    valid = False
                else:
                    self.warnings.append(format_error_message(msg, entity_type, "schema"))
        
        # Validate data types and constraints
        for _, row in clean_data.iterrows():
            row_id = row.get("source_id") or row.get("id") or "unknown"
            row_issues = self._validate_row(entity_type, row, entity_rules)
            
            for issue in row_issues:
                if self.strict:
                    validation_issues.append({
                        "id": row_id, 
                        "field": issue["field"], 
                        "error": issue["error"]
                    })
                    valid = False
                else:
                    self.warnings.append(
                        format_error_message(issue["error"], entity_type, str(row_id))
                    )
        
        if validation_issues:
            self.errors.extend(validation_issues)
            logger.warning(f"Validation issues found for {entity_type}: {len(validation_issues)}")
        
        return clean_data, valid
    
    def _validate_row(self, entity_type: str, row: pd.Series, rules: Dict) -> List[Dict]:
        """
        Validate a single data row against validation rules.
        
        Args:
            entity_type: Type of entity
            row: Data row to validate
            rules: Validation rules for the entity type
            
        Returns:
            List[Dict]: List of validation issues
        """
        issues = []
        
        for field, field_rules in rules.items():
            if field not in row:
                if field_rules.get("required", False):
                    issues.append({
                        "field": field,
                        "error": f"Required field '{field}' is missing"
                    })
                continue
            
            value = row[field]
            
            # Skip validation for None/NaN values unless required
            if pd.isna(value):
                if field_rules.get("required", False):
                    issues.append({
                        "field": field,
                        "error": f"Required field '{field}' is null"
                    })
                continue
            
            # Check data type
            field_type = field_rules.get("type", "string")
            type_valid, type_issue = self._validate_type(field, value, field_type)
            if not type_valid:
                issues.append({
                    "field": field,
                    "error": type_issue
                })
            
            # Check allowed values
            if "allowed_values" in field_rules and value not in field_rules["allowed_values"]:
                issues.append({
                    "field": field,
                    "error": f"Value '{value}' not in allowed values: {field_rules['allowed_values']}"
                })
            
            # Check regex pattern
            if "pattern" in field_rules and not re.match(field_rules["pattern"], str(value)):
                issues.append({
                    "field": field,
                    "error": f"Value '{value}' does not match pattern: {field_rules['pattern']}"
                })
            
            # Check min/max constraints for numeric fields
            if field_type in ["number", "float", "integer", "int"]:
                try:
                    num_value = float(value)
                    
                    if "min" in field_rules and num_value < field_rules["min"]:
                        issues.append({
                            "field": field,
                            "error": f"Value {num_value} is less than minimum: {field_rules['min']}"
                        })
                        
                    if "max" in field_rules and num_value > field_rules["max"]:
                        issues.append({
                            "field": field,
                            "error": f"Value {num_value} is greater than maximum: {field_rules['max']}"
                        })
                except (ValueError, TypeError):
                    pass
        
        return issues
    
    def _validate_type(self, field: str, value: Any, expected_type: str) -> Tuple[bool, str]:
        """
        Validate a value against an expected data type.
        
        Args:
            field: Field name
            value: Value to validate
            expected_type: Expected data type
            
        Returns:
            Tuple[bool, str]: Validation result and error message
        """
        if expected_type == "string":
            if not isinstance(value, str):
                try:
                    str(value)  # Try to convert to string
                    return True, ""
                except:
                    return False, f"Value '{value}' is not a valid string"
        
        elif expected_type == "number" or expected_type == "float":
            try:
                float(value)
                return True, ""
            except (ValueError, TypeError):
                return False, f"Value '{value}' is not a valid number"
        
        elif expected_type == "integer" or expected_type == "int":
            try:
                int(float(value))
                return True, ""
            except (ValueError, TypeError):
                return False, f"Value '{value}' is not a valid integer"
        
        elif expected_type == "boolean" or expected_type == "bool":
            if isinstance(value, bool):
                return True, ""
            elif isinstance(value, str) and value.lower() in ["true", "false", "1", "0"]:
                return True, ""
            else:
                return False, f"Value '{value}' is not a valid boolean"
        
        elif expected_type == "date" or expected_type == "datetime":
            try:
                if isinstance(value, (datetime, pd.Timestamp)):
                    return True, ""
                pd.to_datetime(value)
                return True, ""
            except:
                return False, f"Value '{value}' is not a valid date/datetime"
        
        elif expected_type == "email":
            if isinstance(value, str) and re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', value):
                return True, ""
            else:
                return False, f"Value '{value}' is not a valid email address"
        
        elif expected_type == "phone":
            if isinstance(value, str) and re.match(r'^\+?[0-9]{10,15}$', re.sub(r'[^0-9+]', '', str(value))):
                return True, ""
            else:
                return False, f"Value '{value}' is not a valid phone number"
        
        return True, ""  # Default: accept any value for unknown types
    
    def get_validation_report(self) -> Dict:
        """
        Generate a validation report.
        
        Returns:
            Dict: Validation report
        """
        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "critical_errors": len(self.errors) > 0
        }
    
    def clear(self):
        """Clear validation errors and warnings."""
        self.errors = []
        self.warnings = []