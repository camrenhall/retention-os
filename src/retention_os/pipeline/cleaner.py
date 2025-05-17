# src/retention_os/pipeline/cleaner.py
import logging
import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any, Optional, Callable, Union

logger = logging.getLogger(__name__)

class CleaningRule:
    """Represents a cleaning rule for a field."""
    
    def __init__(self, 
                 field: str, 
                 rule_type: str, 
                 parameters: Dict[str, Any] = None):
        """
        Initialize a cleaning rule.
        
        Args:
            field: The field to clean
            rule_type: The type of cleaning rule
            parameters: Parameters for the rule
        """
        self.field = field
        self.rule_type = rule_type
        self.parameters = parameters or {}
    
    def __str__(self) -> str:
        """String representation of the rule."""
        return f"{self.rule_type} cleaning for {self.field}"

class DataCleaner:
    """Applies data cleaning rules to improve data quality."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data cleaner.
        
        Args:
            config: Configuration dictionary for cleaning
        """
        self.config = config or {}
        self._cleaners = self._build_cleaners()
    
    def _build_cleaners(self) -> Dict[str, Callable]:
        """Build dictionary of cleaning functions."""
        return {
            'standardize_text': self._standardize_text,
            'standardize_phone': self._standardize_phone,
            'standardize_email': self._standardize_email,
            'fill_missing': self._fill_missing,
            'remove_special_chars': self._remove_special_chars,
            'normalize_case': self._normalize_case,
            'trim_whitespace': self._trim_whitespace,
            'convert_to_number': self._convert_to_number,
            'convert_to_boolean': self._convert_to_boolean,
            'format_date': self._format_date
        }
    
    def clean_dataset(self, 
                      entity_type: str, 
                      data: List[Dict[str, Any]], 
                      rules: List[CleaningRule]) -> List[Dict[str, Any]]:
        """
        Clean a dataset using the specified rules.
        
        Args:
            entity_type: Type of entity being cleaned
            data: List of entity dictionaries
            rules: List of cleaning rules
            
        Returns:
            List of cleaned entity dictionaries
        """
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(data)
        
        # Apply cleaning rules
        for rule in rules:
            field = rule.field
            
            # Skip if field not in DataFrame
            if field not in df.columns:
                logger.warning(f"Field {field} not found in {entity_type} data, skipping cleaning")
                continue
            
            # Get the cleaning function
            cleaner = self._cleaners.get(rule.rule_type)
            if not cleaner:
                logger.warning(f"Unsupported cleaning rule type: {rule.rule_type}")
                continue
            
            # Apply the cleaning
            try:
                df[field] = df[field].apply(
                    lambda x: cleaner(x, rule.parameters) if pd.notna(x) else x
                )
            except Exception as e:
                logger.error(f"Error applying cleaning rule {rule.rule_type} to {field}: {e}")
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Convert back to list of dictionaries
        return df.to_dict('records')
    
    # Cleaning functions
    
    def _standardize_text(self, value: Any, params: Dict[str, Any]) -> Any:
        """Standardize text by removing extra spaces, normalizing case, etc."""
        if not isinstance(value, str):
            return value
        
        # Convert to string and trim
        result = str(value).strip()
        
        # Normalize case if specified
        case = params.get('case', None)
        if case == 'lower':
            result = result.lower()
        elif case == 'upper':
            result = result.upper()
        elif case == 'title':
            result = result.title()
        
        # Remove multiple spaces
        result = re.sub(r'\s+', ' ', result)
        
        return result
    
    def _standardize_phone(self, value: Any, params: Dict[str, Any]) -> Any:
        """Standardize phone numbers to a consistent format."""
        if not isinstance(value, str):
            return value
        
        # Remove all non-numeric characters
        digits = re.sub(r'\D', '', value)
        
        # Format based on parameters
        format_type = params.get('format', 'e164')
        
        if len(digits) == 10:  # US number without country code
            if format_type == 'e164':
                return f"+1{digits}"
            elif format_type == 'national':
                return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            else:
                return digits
        elif len(digits) == 11 and digits.startswith('1'):  # US number with country code
            if format_type == 'e164':
                return f"+{digits}"
            elif format_type == 'national':
                return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
            else:
                return digits
        else:
            # Unknown format, return as is with plus if e164
            if format_type == 'e164' and not digits.startswith('+'):
                return f"+{digits}"
            return digits
    
    def _standardize_email(self, value: Any, params: Dict[str, Any]) -> Any:
        """Standardize email addresses."""
        if not isinstance(value, str):
            return value
        
        # Trim and lowercase
        email = value.strip().lower()
        
        # Validate basic format
        if re.match(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$', email):
            return email
        else:
            # Invalid email, return original or None based on params
            return None if params.get('invalid_to_null', False) else value
    
    def _fill_missing(self, value: Any, params: Dict[str, Any]) -> Any:
        """Fill missing values with a default value."""
        if pd.isna(value) or value == "":
            return params.get('default_value', "")
        return value
    
    def _remove_special_chars(self, value: Any, params: Dict[str, Any]) -> Any:
        """Remove special characters from text."""
        if not isinstance(value, str):
            return value
        
        # Get pattern of characters to keep
        pattern = params.get('pattern', r'[^a-zA-Z0-9\s]')
        replacement = params.get('replacement', '')
        
        return re.sub(pattern, replacement, value)
    
    def _normalize_case(self, value: Any, params: Dict[str, Any]) -> Any:
        """Normalize case of text."""
        if not isinstance(value, str):
            return value
        
        case = params.get('case', 'lower')
        if case == 'lower':
            return value.lower()
        elif case == 'upper':
            return value.upper()
        elif case == 'title':
            return value.title()
        else:
            return value
    
    def _trim_whitespace(self, value: Any, params: Dict[str, Any]) -> Any:
        """Trim whitespace from text."""
        if not isinstance(value, str):
            return value
        
        return value.strip()
    
    def _convert_to_number(self, value: Any, params: Dict[str, Any]) -> Any:
        """Convert value to number."""
        if isinstance(value, (int, float)):
            return value
        
        if isinstance(value, str):
            # Remove currency symbols and separators
            value = re.sub(r'[^\d.-]', '', value)
            
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except (ValueError, TypeError):
                return None
        
        return None
    
    def _convert_to_boolean(self, value: Any, params: Dict[str, Any]) -> Any:
        """Convert value to boolean."""
        if isinstance(value, bool):
            return value
        
        true_values = params.get('true_values', ['true', 'yes', 'y', '1', 'on'])
        false_values = params.get('false_values', ['false', 'no', 'n', '0', 'off'])
        
        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in true_values:
                return True
            elif value_lower in false_values:
                return False
        
        return None
    
    def _format_date(self, value: Any, params: Dict[str, Any]) -> Any:
        """Format date/time values."""
        if pd.isna(value):
            return value
        
        input_format = params.get('input_format', None)
        output_format = params.get('output_format', '%Y-%m-%d')
        
        try:
            if isinstance(value, str) and input_format:
                dt = pd.to_datetime(value, format=input_format)
            else:
                dt = pd.to_datetime(value)
            
            return dt.strftime(output_format)
        except:
            return value