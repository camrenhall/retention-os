"""
Utility functions for RetentionOS data processing.
"""
import re
from datetime import datetime
from typing import Any, List, Dict, Optional, Union

import pandas as pd
from loguru import logger


def clean_column_names(columns: List[str]) -> List[str]:
    """
    Clean column names by removing special characters and converting to lowercase.
    
    Args:
        columns: List of column names
        
    Returns:
        List[str]: List of cleaned column names
    """
    return [
        re.sub(r'[^a-zA-Z0-9_]', '', col.lower().replace(' ', '_'))
        for col in columns
    ]


def standardize_datetime(date_value: Any) -> Optional[datetime]:
    """
    Standardize a date or datetime value to ISO format.
    
    Args:
        date_value: A date or datetime value in various formats
        
    Returns:
        Optional[datetime]: Standardized datetime object, or None if invalid
    """
    if pd.isna(date_value) or date_value is None:
        return None
    
    if isinstance(date_value, (datetime, pd.Timestamp)):
        return date_value
    
    # Try different datetime formats
    try:
        # First try pandas to_datetime which handles many formats
        result = pd.to_datetime(date_value)
        if pd.isna(result):
            return None
        return result.to_pydatetime()
    except Exception as e:
        # If pandas fails, try manual parsing with common formats
        formats = [
            '%Y-%m-%d',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %I:%M:%S %p',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%Y/%m/%d'
        ]
        
        if isinstance(date_value, str):
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt)
                except ValueError:
                    continue
                    
        logger.warning(f"Could not parse date value: {date_value}")
        return None


def parse_phone_number(phone_value: Any) -> Optional[str]:
    """
    Parse and standardize a phone number.
    
    Args:
        phone_value: A phone number in various formats
        
    Returns:
        Optional[str]: Standardized phone number string, or None if invalid
    """
    if pd.isna(phone_value) or phone_value is None:
        return None
    
    # Convert to string if it's a number
    if isinstance(phone_value, (int, float)):
        phone_value = str(int(phone_value))
    
    # Extract digits only
    if isinstance(phone_value, str):
        digits = re.sub(r'[^0-9]', '', phone_value)
        
        # Handle common US formats
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits.startswith('1'):
            return f"+{digits}"
        elif len(digits) > 0:
            return f"+{digits}"
    
    logger.warning(f"Could not parse phone number: {phone_value}")
    return None


def generate_id() -> str:
    """
    Generate a unique ID.
    
    Returns:
        str: Unique ID
    """
    import uuid
    return str(uuid.uuid4())


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, on: str, how: str = 'inner') -> pd.DataFrame:
    """
    Merge two DataFrames with error handling.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        on: Column to merge on
        how: Type of merge (inner, left, right, outer)
        
    Returns:
        pd.DataFrame: Merged DataFrame
    """
    try:
        if df1.empty or df2.empty:
            logger.warning("One of the DataFrames is empty for merge")
            return df1 if not df1.empty else df2
            
        if on not in df1.columns or on not in df2.columns:
            logger.error(f"Merge column {on} not found in both DataFrames")
            return pd.DataFrame()
            
        return df1.merge(df2, on=on, how=how)
    except Exception as e:
        logger.error(f"Error merging DataFrames: {e}")
        return pd.DataFrame()


def validate_data_types(df: pd.DataFrame, type_dict: Dict[str, str]) -> pd.DataFrame:
    """
    Validate and convert DataFrame column types based on a type dictionary.
    
    Args:
        df: DataFrame to validate
        type_dict: Dictionary of column name to type
        
    Returns:
        pd.DataFrame: DataFrame with validated types
    """
    result_df = df.copy()
    
    for col, dtype in type_dict.items():
        if col not in result_df.columns:
            continue
            
        try:
            if dtype == 'datetime':
                result_df[col] = pd.to_datetime(result_df[col], errors='coerce')
            elif dtype == 'int':
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')
            elif dtype == 'float':
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
            elif dtype == 'bool':
                result_df[col] = result_df[col].map({'True': True, 'False': False}, na_action='ignore')
            else:
                result_df[col] = result_df[col].astype(str)
        except Exception as e:
            logger.warning(f"Error converting {col} to {dtype}: {e}")
    
    return result_df


def format_error_message(message: str, entity: str, id_value: str) -> str:
    """
    Format an error message with entity and ID information.
    
    Args:
        message: Error message
        entity: Entity type
        id_value: Entity ID
        
    Returns:
        str: Formatted error message
    """
    return f"{entity}:{id_value} - {message}"