# src/retention_os/data_ingestion/transformers/type_converter.py
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Union, List
from datetime import datetime

class TypeConverter:
    """
    Utility class for converting data types in pandas DataFrames.
    """
    
    def __init__(self, type_conversions: Dict[str, Any]):
        """
        Initialize the type converter with conversion configurations.
        
        Args:
            type_conversions: Dictionary of field-specific type conversions
        """
        self.type_conversions = type_conversions
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def convert_types(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        """
        Apply data type conversions to a dataframe.
        
        Args:
            df: Source dataframe
            entity_name: Name of the entity being processed
            
        Returns:
            Dataframe with converted data types
        """
        # Make a copy to avoid modifying the original
        result_df = df.copy()
        
        # Filter conversions for the specified entity
        entity_conversions = {
            k.split('.')[1]: v for k, v in self.type_conversions.items() 
            if k.startswith(f"{entity_name}.")
        }
        
        if not entity_conversions:
            self.logger.info(f"No type conversions found for entity {entity_name}")
            return result_df
            
        # Apply each conversion
        for field, conversion in entity_conversions.items():
            if field not in result_df.columns:
                self.logger.warning(f"Field {field} not found in dataframe for type conversion")
                continue
                
            try:
                result_df = self._apply_conversion(result_df, field, conversion)
            except Exception as e:
                self.logger.error(f"Error converting field {field}: {str(e)}")
                
        return result_df
    
    def _apply_conversion(self, df: pd.DataFrame, field: str, conversion: Union[str, Dict[str, Any]]) -> pd.DataFrame:
        """
        Apply a specific type conversion to a field.
        
        Args:
            df: Source dataframe
            field: Field name to convert
            conversion: Conversion specification (string or dictionary)
            
        Returns:
            Dataframe with the field converted
        """
        # Make a copy of the dataframe to avoid modifying the original
        result_df = df.copy()
        
        if isinstance(conversion, str):
            # Simple type conversion
            if conversion == 'string':
                # Handle NaN values first
                result_df[field] = result_df[field].astype(str).replace('nan', None)
            elif conversion == 'float':
                result_df[field] = pd.to_numeric(result_df[field], errors='coerce')
            elif conversion == 'int':
                # Use Int64 to preserve NaN values
                result_df[field] = pd.to_numeric(result_df[field], errors='coerce').astype('Int64')
            elif conversion == 'boolean':
                # Handle various boolean representations
                bool_map = {
                    'true': True, 'True': True, '1': True, 1: True, 'yes': True, 'Y': True,
                    'false': False, 'False': False, '0': False, 0: False, 'no': False, 'N': False
                }
                result_df[field] = result_df[field].map(bool_map)
            else:
                self.logger.warning(f"Unknown simple conversion type: {conversion}")
                
        elif isinstance(conversion, dict):
            # Complex conversion with format
            if conversion.get('format') == 'datetime':
                pattern = conversion.get('pattern', '%Y-%m-%d %H:%M:%S')
                result_df[field] = pd.to_datetime(result_df[field], format=pattern, errors='coerce')
                
            elif conversion.get('format') == 'date':
                pattern = conversion.get('pattern', '%Y-%m-%d')
                result_df[field] = pd.to_datetime(result_df[field], format=pattern, errors='coerce').dt.date
                
            elif conversion.get('format') == 'money':
                # Remove currency symbols and commas, then convert to float
                result_df[field] = result_df[field].astype(str) \
                    .str.replace(r'[$,]', '', regex=True) \
                    .pipe(pd.to_numeric, errors='coerce')
                    
            elif conversion.get('format') == 'phone':
                # Standardize phone numbers (basic implementation)
                result_df[field] = result_df[field].astype(str) \
                    .str.replace(r'[^0-9]', '', regex=True) \
                    .str.replace(r'^(\d{10})$', r'+1\1', regex=True) \
                    .str.replace(r'^1(\d{10})$', r'+1\1', regex=True) \
                    .str.replace(r'^(\d{11})$', r'+\1', regex=True)
                    
            else:
                self.logger.warning(f"Unknown complex conversion format: {conversion.get('format')}")
                
        else:
            self.logger.warning(f"Unsupported conversion specification: {conversion}")
            
        return result_df