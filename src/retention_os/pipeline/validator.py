import logging
import re
from typing import Dict, List, Any, Optional, Callable, Union
import pandas as pd

logger = logging.getLogger(__name__)

class ValidationRule:
    """Represents a validation rule for a field."""
    
    def __init__(self, 
                 field: str, 
                 rule_type: str, 
                 parameters: Dict[str, Any] = None, 
                 error_message: str = None):
        """
        Initialize a validation rule.
        
        Args:
            field: The field to validate
            rule_type: The type of validation rule
            parameters: Parameters for the rule
            error_message: Custom error message
        """
        self.field = field
        self.rule_type = rule_type
        self.parameters = parameters or {}
        self.error_message = error_message
    
    def __str__(self) -> str:
        """String representation of the rule."""
        return f"{self.rule_type} validation for {self.field}"

class ValidationResult:
    """Results of data validation."""
    
    def __init__(self, entity_type: str):
        """
        Initialize validation results.
        
        Args:
            entity_type: The type of entity being validated
        """
        self.entity_type = entity_type
        self.total_records = 0
        self.valid_records = 0
        self.invalid_records = 0
        self.errors = []
        self.valid_indices = []
        self.invalid_indices = []
    
    def add_error(self, 
                  record_index: int, 
                  field: str, 
                  rule_type: str, 
                  error_message: str, 
                  value: Any) -> None:
        """
        Add a validation error.
        
        Args:
            record_index: Index of the record with the error
            field: Field with the error
            rule_type: Type of validation rule that failed
            error_message: Error message
            value: The value that failed validation
        """
        self.errors.append({
            'record_index': record_index,
            'field': field,
            'rule_type': rule_type,
            'message': error_message,
            'value': str(value)
        })
        
        if record_index not in self.invalid_indices:
            self.invalid_indices.append(record_index)
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """
        Calculate validation metrics.
        
        Returns:
            Dictionary of validation metrics
        """
        self.valid_indices = [i for i in range(self.total_records) if i not in self.invalid_indices]
        self.valid_records = len(self.valid_indices)
        self.invalid_records = len(self.invalid_indices)
        
        return {
            'entity_type': self.entity_type,
            'total_records': self.total_records,
            'valid_records': self.valid_records,
            'invalid_records': self.invalid_records,
            'valid_percentage': round(self.valid_records / self.total_records * 100, 2) if self.total_records > 0 else 0,
            'error_count': len(self.errors),
            'error_count_by_field': self._count_errors_by_field()
        }
    
    def _count_errors_by_field(self) -> Dict[str, int]:
        """Count errors by field."""
        error_counts = {}
        for error in self.errors:
            field = error['field']
            error_counts[field] = error_counts.get(field, 0) + 1
        return error_counts
    
    def get_valid_records(self, data: Union[pd.DataFrame, List[Dict[str, Any]]]) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get only valid records from the data.
        
        Args:
            data: The original data (DataFrame or list of dicts)
            
        Returns:
            Data containing only valid records
        """
        if isinstance(data, pd.DataFrame):
            return data.iloc[self.valid_indices].copy()
        else:
            return [data[i] for i in self.valid_indices]
    
    def generate_report(self) -> str:
        """
        Generate a human-readable validation report.
        
        Returns:
            Formatted validation report string
        """
        metrics = self.calculate_metrics()
        
        lines = [
            f"Validation Report for {self.entity_type}",
            "=" * 50,
            f"Total Records: {metrics['total_records']}",
            f"Valid Records: {metrics['valid_records']} ({metrics['valid_percentage']}%)",
            f"Invalid Records: {metrics['invalid_records']}",
            f"Total Errors: {metrics['error_count']}",
            "",
            "Errors by Field:",
            "-" * 40
        ]
        
        for field, count in metrics['error_count_by_field'].items():
            lines.append(f"{field}: {count} errors")
        
        if self.errors:
            lines.append("")
            lines.append("Sample Errors:")
            lines.append("-" * 40)
            
            for error in self.errors[:10]:  # Show first 10 errors
                lines.append(f"Record {error['record_index']}, Field: {error['field']}")
                lines.append(f"  Error: {error['message']}")
                lines.append(f"  Value: {error['value']}")
                lines.append("")
        
        return "\n".join(lines)

class DataValidator:
    """Validates data against business rules and constraints."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data validator.
        
        Args:
            config: Configuration dictionary for validation
        """
        self.config = config or {}
        self.fail_on_error = self.config.get('fail_on_error', False)
        self.error_threshold = self.config.get('error_threshold', 0.2)  # 20% by default
        self._validators = self._build_validators()
    
    def _build_validators(self) -> Dict[str, Callable]:
        """Build dictionary of validation functions."""
        return {
            'required': self._validate_required,
            'type': self._validate_type,
            'format': self._validate_format,
            'min_length': self._validate_min_length,
            'max_length': self._validate_max_length,
            'min_value': self._validate_min_value,
            'max_value': self._validate_max_value,
            'in_list': self._validate_in_list,
            'regex': self._validate_regex
        }
    
    def validate_dataset(self, 
                         entity_type: str, 
                         data: List[Dict[str, Any]], 
                         rules: List[ValidationRule]) -> ValidationResult:
        """
        Validate a dataset against a set of rules.
        
        Args:
            entity_type: Type of entity being validated
            data: List of entity dictionaries
            rules: List of validation rules
            
        Returns:
            ValidationResult with validation results
        """
        result = ValidationResult(entity_type)
        result.total_records = len(data)
        
        # Validate each record
        for i, record in enumerate(data):
            self.validate_record(record, rules, i, result)
        
        # Calculate final metrics
        result.calculate_metrics()
        
        # Check if validation fails based on threshold
        valid_percentage = result.valid_records / result.total_records if result.total_records > 0 else 1.0
        if self.fail_on_error and valid_percentage < (1.0 - self.error_threshold):
            raise ValueError(
                f"Validation failed: {valid_percentage:.2%} valid records is below threshold of {1.0 - self.error_threshold:.2%}")
        
        return result
    
    def validate_record(self, 
                        record: Dict[str, Any], 
                        rules: List[ValidationRule], 
                        record_index: int, 
                        result: ValidationResult) -> None:
        """
        Validate a single record against a set of rules.
        
        Args:
            record: Entity dictionary
            rules: List of validation rules
            record_index: Index of the record
            result: ValidationResult to update with errors
        """
        for rule in rules:
            field = rule.field
            
            # Skip validation if field is not in record and is not required
            if field not in record and rule.rule_type != 'required':
                continue
            
            # Get the validation function
            validator = self._validators.get(rule.rule_type)
            if not validator:
                logger.warning(f"Unsupported validation rule type: {rule.rule_type}")
                continue
            
            # Run the validation
            value = record.get(field)
            is_valid, error_message = validator(value, rule.parameters)
            
            if not is_valid:
                error_message = rule.error_message or error_message
                result.add_error(record_index, field, rule.rule_type, error_message, value)
    
    def validate_field(self, 
                       value: Any, 
                       rule: ValidationRule) -> tuple[bool, Optional[str]]:
        """
        Validate a field value against a rule.
        
        Args:
            value: The field value
            rule: The validation rule
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        validator = self._validators.get(rule.rule_type)
        if not validator:
            return True, None
        
        return validator(value, rule.parameters)
    
    # Validation functions
    
    def _validate_required(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate that a value is not None or empty."""
        is_valid = value is not None and value != ""
        return is_valid, "Value is required" if not is_valid else None
    
    def _validate_type(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate the type of a value."""
        expected_type = params.get('type', 'string')
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if expected_type == 'string':
            is_valid = isinstance(value, str)
        elif expected_type == 'number':
            is_valid = isinstance(value, (int, float))
        elif expected_type == 'integer':
            is_valid = isinstance(value, int)
        elif expected_type == 'boolean':
            is_valid = isinstance(value, bool)
        elif expected_type == 'array':
            is_valid = isinstance(value, list)
        elif expected_type == 'object':
            is_valid = isinstance(value, dict)
        else:
            is_valid = True  # Unknown type, assume valid
        
        return is_valid, f"Value must be of type {expected_type}" if not is_valid else None
    
    def _validate_format(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate the format of a value."""
        format_type = params.get('format', '')
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if not isinstance(value, str):
            return False, f"Value must be a string for format validation"
        
        if format_type == 'email':
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            is_valid = bool(re.match(pattern, value))
            error_msg = "Value must be a valid email address"
        elif format_type == 'phone':
            pattern = params.get('pattern', r'^\+?[0-9]{10,15}$')
            # Remove common separators before checking
            clean_value = re.sub(r'[\s\-\(\)\.]', '', value)
            is_valid = bool(re.match(pattern, clean_value))
            error_msg = "Value must be a valid phone number"
        else:
            # Use custom pattern if provided
            pattern = params.get('pattern', '')
            if pattern:
                is_valid = bool(re.match(pattern, value))
                error_msg = f"Value must match pattern: {pattern}"
            else:
                is_valid = True
                error_msg = None
        
        return is_valid, error_msg if not is_valid else None
    
    def _validate_min_length(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate minimum length of a string."""
        min_length = params.get('min_length', 0)
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if not isinstance(value, str):
            return False, "Value must be a string for length validation"
        
        is_valid = len(value) >= min_length
        return is_valid, f"Value must be at least {min_length} characters long" if not is_valid else None
    
    def _validate_max_length(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate maximum length of a string."""
        max_length = params.get('max_length', float('inf'))
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if not isinstance(value, str):
            return False, "Value must be a string for length validation"
        
        is_valid = len(value) <= max_length
        return is_valid, f"Value must be at most {max_length} characters long" if not is_valid else None
    
    def _validate_min_value(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate minimum value of a number."""
        min_value = params.get('min_value', float('-inf'))
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if not isinstance(value, (int, float)):
            return False, "Value must be a number for value validation"
        
        is_valid = value >= min_value
        return is_valid, f"Value must be at least {min_value}" if not is_valid else None
    
    def _validate_max_value(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate maximum value of a number."""
        max_value = params.get('max_value', float('inf'))
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if not isinstance(value, (int, float)):
            return False, "Value must be a number for value validation"
        
        is_valid = value <= max_value
        return is_valid, f"Value must be at most {max_value}" if not is_valid else None
    
    def _validate_in_list(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate that a value is in a list of allowed values."""
        allowed_values = params.get('values', [])
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        is_valid = value in allowed_values
        return is_valid, f"Value must be one of: {allowed_values}" if not is_valid else None
    
    def _validate_regex(self, value: Any, params: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Validate that a value matches a regular expression."""
        pattern = params.get('pattern', '')
        nullable = params.get('nullable', True)
        
        # If nullable and value is None, it's valid
        if nullable and (value is None or value == ""):
            return True, None
        
        if not isinstance(value, str):
            return False, "Value must be a string for regex validation"
        
        is_valid = bool(re.match(pattern, value))
        return is_valid, f"Value must match pattern: {pattern}" if not is_valid else None
    
    def _validate_config(self) -> bool:
        """
        Validate pipeline configuration.
        
        Returns:
            True if valid, False otherwise
        """
        # Required configuration sections
        required_sections = ['crm', 'processing', 'output']
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required configuration section: {section}")
                return False
        
        # Validate CRM configuration
        crm_config = self.config.get('crm', {})
        if 'type' not in crm_config:
            logger.error("Missing CRM type in configuration")
            return False
        
        if 'config_file' not in crm_config:
            logger.error("Missing CRM config file in configuration")
            return False
        
        # Validate processing configuration
        processing_config = self.config.get('processing', {})
        if 'entity_types' not in processing_config:
            logger.error("Missing entity types in processing configuration")
            return False
        
        if 'processing_order' not in processing_config:
            logger.error("Missing processing order in processing configuration")
            return False
        
        # Validate directory existence
        input_dir = self.config.get('input_dir', 'data/input')
        if not os.path.exists(input_dir):
            logger.warning(f"Input directory does not exist: {input_dir}")
            try:
                os.makedirs(input_dir, exist_ok=True)
                logger.info(f"Created input directory: {input_dir}")
            except Exception as e:
                logger.error(f"Failed to create input directory: {e}")
                return False
        
        output_dir = self.config.get('output_dir', 'data/processed/canonical')
        if not os.path.exists(output_dir):
            logger.warning(f"Output directory does not exist: {output_dir}")
            try:
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Created output directory: {output_dir}")
            except Exception as e:
                logger.error(f"Failed to create output directory: {e}")
                return False
        
        return True