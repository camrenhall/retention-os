import os
import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
import yaml

logger = logging.getLogger(__name__)

class CRMAdapter(ABC):
    """
    Abstract base class for all CRM adapters.
    Defines the interface and common functionality for adapters.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the adapter with a configuration file.
        
        Args:
            config_path: Path to the adapter configuration file
        """
        self.config = self._load_config(config_path)
        self.input_dir = self.config.get('input_dir', 'data/input')
        self.output_dir = self.config.get('output_dir', 'data/processed')
        self.file_mappings = self.config.get('file_mappings', {})
        self.field_mappings = self.config.get('field_mappings', {})
        self.type_conversions = self.config.get('type_conversions', {})
        self._loaded_dataframes = {}
    
    def _load_config(self, config_path: str) -> dict:
        """Load and parse configuration file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            raise
    
    @abstractmethod
    def get_source_files(self) -> Dict[str, str]:
        """
        Get the mapping of entity types to source file paths.
        
        Returns:
            A dictionary mapping entity types to file paths
        """
        pass
    
    def load_source_files(self) -> Dict[str, pd.DataFrame]:
        """
        Load all source files into DataFrames.
        
        Returns:
            Dictionary of entity types to DataFrames
        """
        result = {}
        for entity_type, file_pattern in self.file_mappings.items():
            try:
                file_path = self._find_file(file_pattern)
                if file_path:
                    encoding = self._detect_file_encoding(file_path)
                    df = pd.read_csv(file_path, encoding=encoding)
                    result[entity_type] = df
                    self._loaded_dataframes[entity_type] = df
                    logger.info(f"Loaded {entity_type} data from {file_path}")
                else:
                    logger.warning(f"Could not find file for {entity_type} using pattern {file_pattern}")
            except Exception as e:
                logger.error(f"Error loading {entity_type} data: {e}")
        return result
    
    def _find_file(self, pattern: str) -> Optional[str]:
        """Find a file that matches the given pattern in the input directory."""
        for filename in os.listdir(self.input_dir):
            if pattern in filename:
                return os.path.join(self.input_dir, filename)
        return None
    
    def _detect_file_encoding(self, file_path: str) -> str:
        """
        Detect the encoding of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Detected encoding (defaults to 'utf-8')
        """
        # Simple detection - could be enhanced with chardet or similar
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                f.read(1024)
                return 'utf-8'
        except UnicodeDecodeError:
            # Try another common encoding
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    f.read(1024)
                    return 'latin-1'
            except UnicodeDecodeError:
                # Default to utf-8 with error handling
                return 'utf-8'
    
    def standardize_schema(self, entity_type: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize the schema of a DataFrame to match our intermediate format.
        
        Args:
            entity_type: The type of entity (patient, appointment, etc.)
            df: The source DataFrame
                
        Returns:
            DataFrame with standardized schema
        """
        if entity_type not in self.field_mappings:
            logger.warning(f"No field mappings defined for {entity_type}")
            return df
        
        # Create a new DataFrame with our standard schema
        mapped_data = {}
        
        for target_field, source_field in self.field_mappings[entity_type].items():
            if source_field in df.columns:
                mapped_data[target_field] = df[source_field]
            else:
                logger.warning(f"Source field '{source_field}' not found in {entity_type} data")
                mapped_data[target_field] = None
        
        # Check if we have any non-None values
        if not any(v is not None for v in mapped_data.values()):
            logger.warning(f"No valid fields found for {entity_type}, returning empty DataFrame")
            # Return an empty DataFrame with the correct columns
            return pd.DataFrame(columns=list(mapped_data.keys()))
        
        # Create DataFrame with the index from the original DataFrame to avoid errors
        if len(df) > 0:
            result_df = pd.DataFrame(mapped_data, index=df.index)
        else:
            # Handle empty source DataFrame
            result_df = pd.DataFrame(mapped_data)
        
        # Apply type conversions
        self._apply_type_conversions(entity_type, result_df)
        
        return result_df
    
    def _apply_type_conversions(self, entity_type: str, df: pd.DataFrame) -> None:
        """Apply type conversions to the DataFrame."""
        type_conversions = {}
        
        # Get entity-specific type conversions
        for field_key, conversion in self.type_conversions.items():
            if field_key.startswith(f"{entity_type}."):
                field_name = field_key.split('.')[1]
                type_conversions[field_name] = conversion
        
        for field, conversion in type_conversions.items():
            if field in df.columns:
                try:
                    if isinstance(conversion, dict) and conversion.get('format') == 'datetime':
                        pattern = conversion.get('pattern', '%Y-%m-%d %H:%M:%S')
                        df[field] = pd.to_datetime(df[field], format=pattern, errors='coerce')
                    elif conversion == 'string':
                        df[field] = df[field].astype(str)
                    elif conversion == 'float':
                        df[field] = pd.to_numeric(df[field], errors='coerce')
                    elif conversion == 'int':
                        df[field] = pd.to_numeric(df[field], errors='coerce').astype('Int64')  # Nullable integer
                    elif conversion == 'boolean':
                        df[field] = df[field].map({'True': True, 'true': True, '1': True, 
                                                  'False': False, 'false': False, '0': False}).astype('boolean')
                except Exception as e:
                    logger.error(f"Error converting {field} in {entity_type}: {e}")
    
    @abstractmethod
    def transform_to_canonical(self) -> Dict[str, List[dict]]:
        """
        Transform loaded DataFrames to canonical model entities.
        
        Returns:
            Dictionary mapping entity types to lists of entity dictionaries
        """
        pass
    
    def _safe_read_csv(self, file_path: str, encoding: str = 'utf-8', **kwargs) -> Optional[pd.DataFrame]:
        """
        Safely read a CSV file with error handling.
        
        Args:
            file_path: Path to the CSV file
            encoding: File encoding
            kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame or None if error
        """
        try:
            return pd.read_csv(file_path, encoding=encoding, **kwargs)
        except UnicodeDecodeError:
            # Try another encoding
            try:
                return pd.read_csv(file_path, encoding='latin-1', **kwargs)
            except Exception as e:
                logger.error(f"Failed to read {file_path} with latin-1 encoding: {e}")
                return None
        except pd.errors.ParserError:
            # Try with error recovery options
            try:
                return pd.read_csv(
                    file_path, 
                    encoding=encoding, 
                    error_bad_lines=False, 
                    warn_bad_lines=True,
                    **kwargs
                )
            except Exception as e:
                logger.error(f"Failed to read {file_path} with error recovery: {e}")
                return None
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None