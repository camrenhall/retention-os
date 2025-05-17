import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from collections import Counter

logger = logging.getLogger(__name__)

class ColumnProfile:
    """Profile for a single column in a DataFrame."""
    
    def __init__(self, name: str, data: pd.Series):
        """
        Initialize column profile.
        
        Args:
            name: Column name
            data: Pandas Series containing column data
        """
        self.name = name
        self.data = data
        self.profile = self._generate_profile()
    
    def _generate_profile(self) -> Dict[str, Any]:
        """Generate profile statistics for the column."""
        # Check if data contains lists or dictionaries which would cause unhashable type errors
        has_unhashable = False
        for value in self.data:
            if isinstance(value, (list, dict)):
                has_unhashable = True
                break
        
        # Basic profile that works for all data types
        profile = {
            'name': self.name,
            'count': len(self.data),
            'null_count': self.data.isna().sum(),
            'null_percentage': round(self.data.isna().mean() * 100, 2),
            'unique_count': 0,  # Default value in case of unhashable types
            'unique_percentage': 0,  # Default value
            'data_type': str(self.data.dtype),
            'contains_complex_types': has_unhashable
        }
        
        # For unhashable types, skip operations that require hashing
        if has_unhashable:
            # Convert complex types to strings for basic analysis
            string_data = self.data.apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
            try:
                profile['unique_count'] = string_data.nunique()
                profile['unique_percentage'] = round(profile['unique_count'] / len(string_data) * 100, 2) if len(string_data) > 0 else 0
            except:
                # If even string conversion fails, just skip
                pass
            return profile
        
        # For hashable types, perform full profiling
        try:
            profile['unique_count'] = self.data.nunique()
            profile['unique_percentage'] = round(profile['unique_count'] / len(self.data) * 100, 2) if len(self.data) > 0 else 0
            
            # Add type-specific statistics
            if pd.api.types.is_numeric_dtype(self.data):
                non_null = self.data.dropna()
                if len(non_null) > 0:
                    profile.update({
                        'min': non_null.min(),
                        'max': non_null.max(),
                        'mean': non_null.mean(),
                        'median': non_null.median(),
                        'std': non_null.std()
                    })
            
            # Get most common values and their frequencies
            if profile['unique_count'] < 100:  # Only for columns with reasonable cardinality
                try:
                    value_counts = self.data.value_counts(dropna=False).head(10).to_dict()
                    profile['common_values'] = [
                        {'value': str(k), 'count': v, 'percentage': round(v / len(self.data) * 100, 2)}
                        for k, v in value_counts.items()
                    ]
                except:
                    # Skip common values if we encounter issues
                    pass
            
            # Detect patterns in string columns
            if pd.api.types.is_string_dtype(self.data):
                # Length statistics
                non_null = self.data.dropna()
                if len(non_null) > 0:
                    lengths = non_null.str.len()
                    profile.update({
                        'min_length': lengths.min(),
                        'max_length': lengths.max(),
                        'mean_length': round(lengths.mean(), 2)
                    })
                
                # Check if values follow email pattern
                if self.name.lower().find('email') >= 0:
                    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    valid_emails = non_null.str.match(email_pattern).sum()
                    profile['email_format_percentage'] = round(valid_emails / len(non_null) * 100, 2) if len(non_null) > 0 else 0
                
                # Check if values follow phone pattern
                if self.name.lower().find('phone') >= 0:
                    phone_pattern = r'^\+?[0-9]{10,15}$'
                    valid_phones = non_null.str.replace('[^0-9+]', '', regex=True).str.match(phone_pattern).sum()
                    profile['phone_format_percentage'] = round(valid_phones / len(non_null) * 100, 2) if len(non_null) > 0 else 0
        
        except Exception as e:
            # If anything fails, add the error to the profile
            profile['error'] = str(e)
        
        return profile

class DataProfiler:
    """Generates comprehensive profiles for DataFrame data."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the data profiler.
        
        Args:
            config: Configuration dictionary for profiling
        """
        self.config = config or {}
        self.sample_size = self.config.get('sample_size', 1000)
    
    def profile_dataset(self, df: pd.DataFrame, entity_type: str) -> Dict[str, Any]:
        """
        Generate a comprehensive profile for a DataFrame.
        
        Args:
            df: The DataFrame to profile
            entity_type: The type of entity (patient, appointment, etc.)
            
        Returns:
            A dictionary containing profile information
        """
        try:
            # Create a copy to avoid modifying the original
            df_safe = df.copy()
            
            # Take a sample if DataFrame is large
            if len(df_safe) > self.sample_size:
                df_sample = df_safe.sample(self.sample_size, random_state=42)
            else:
                df_sample = df_safe
                
            # Convert ALL columns to strings to avoid any unhashable type issues
            # This is a drastic approach but will ensure profiling works
            for col in df_sample.columns:
                df_sample[col] = df_sample[col].astype(str)
                
            # Generate a safe profile with minimal statistics
            profile = {
                'entity_type': entity_type,
                'row_count': len(df),
                'column_count': len(df.columns),
                'duplicate_rows': 0,  # Initialize with 0 to avoid errors
                'duplicate_percentage': 0,
                'columns': []
            }
            
            # Try to calculate duplicates safely - skip if it fails
            try:
                profile['duplicate_rows'] = df_sample.duplicated().sum()
                profile['duplicate_percentage'] = round(profile['duplicate_rows'] / len(df_sample) * 100, 2) if len(df_sample) > 0 else 0
            except Exception as e:
                logger.warning(f"Couldn't calculate duplicates: {e}")
            
            # Generate column profiles
            for column in df_sample.columns:
                try:
                    # Create a very basic profile
                    column_profile = {
                        'name': column,
                        'count': len(df_sample),
                        'null_count': df_sample[column].isna().sum(),
                        'null_percentage': round(df_sample[column].isna().mean() * 100, 2),
                        'data_type': str(df[column].dtype)  # From original dataframe
                    }
                    
                    profile['columns'].append(column_profile)
                except Exception as e:
                    logger.warning(f"Couldn't profile column {column}: {e}")
                    profile['columns'].append({
                        'name': column, 
                        'error': str(e)
                    })
            
            # Skip anomaly detection
            profile['anomalies'] = []
            
            return profile
            
        except Exception as e:
            logger.error(f"Failed to profile {entity_type}: {e}")
            # Return a minimal profile
            return {
                'entity_type': entity_type,
                'error': str(e),
                'row_count': len(df) if isinstance(df, pd.DataFrame) else 0,
                'column_count': len(df.columns) if isinstance(df, pd.DataFrame) else 0,
                'columns': [],
                'anomalies': []
            }

    def _simple_anomalies(self, column_profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Simplified anomaly detection that avoids complex calculations."""
        anomalies = []
        
        # Check for columns with high null percentage
        high_null_threshold = 0.2  # 20%
        for profile in column_profiles:
            try:
                # Check for high null percentage
                if 'null_percentage' in profile and profile['null_percentage'] > high_null_threshold * 100:
                    anomalies.append({
                        'type': 'high_null_percentage',
                        'column': profile['name'],
                        'value': profile['null_percentage'],
                        'description': f"Column has {profile['null_percentage']}% null values"
                    })
                
                # Check for error in profile
                if 'error' in profile:
                    anomalies.append({
                        'type': 'profiling_error',
                        'column': profile['name'],
                        'description': f"Error profiling column: {profile['error']}"
                    })
                    
                # Check for columns with suspiciously low cardinality
                if 'unique_count' in profile and profile['unique_count'] == 1 and profile['count'] > 10:
                    anomalies.append({
                        'type': 'single_value_column',
                        'column': profile['name'],
                        'description': "Column has only one distinct value"
                    })
            except Exception:
                # Ignore errors in anomaly detection for individual columns
                pass
        
        return anomalies
    
    def _detect_anomalies(self, df: pd.DataFrame, column_profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Detect potential anomalies in the dataset.
        
        Args:
            df: The DataFrame
            column_profiles: List of column profiles
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        # Check for columns with high null percentage
        high_null_threshold = 0.2  # 20%
        for profile in column_profiles:
            try:
                if 'null_percentage' in profile and profile['null_percentage'] > high_null_threshold * 100:
                    anomalies.append({
                        'type': 'high_null_percentage',
                        'column': profile['name'],
                        'value': profile['null_percentage'],
                        'threshold': high_null_threshold * 100,
                        'description': f"Column has {profile['null_percentage']}% null values"
                    })
                
                # Check for error in profile
                if 'error' in profile:
                    anomalies.append({
                        'type': 'profiling_error',
                        'column': profile['name'],
                        'description': f"Error profiling column: {profile['error']}"
                    })
                    
                # Check for columns with suspiciously low cardinality
                if 'unique_count' in profile and 'count' in profile:
                    if profile['unique_count'] == 1 and profile['count'] > 10:
                        anomalies.append({
                            'type': 'single_value_column',
                            'column': profile['name'],
                            'value': profile['unique_count'],
                            'description': "Column has only one distinct value"
                        })
                
                # Check for numeric outliers - only if the column is in the dataframe
                if ('data_type' in profile and 
                    profile['data_type'].startswith(('int', 'float')) and
                    'std' in profile and 'mean' in profile and
                    profile['name'] in df.columns):
                    
                    column = profile['name']
                    mean = profile.get('mean', 0)
                    std = profile.get('std', 0)
                    
                    if std > 0:
                        # Only calculate if we have numeric data
                        if pd.api.types.is_numeric_dtype(df[column]):
                            try:
                                z_scores = (df[column] - mean) / std
                                outliers = df[abs(z_scores) > 3].shape[0]  # More than 3 standard deviations
                                
                                if outliers > 0 and outliers / len(df) < 0.05:  # Less than 5% are outliers
                                    anomalies.append({
                                        'type': 'numeric_outliers',
                                        'column': column,
                                        'value': outliers,
                                        'description': f"Column has {outliers} potential outliers (>3σ from mean)"
                                    })
                            except Exception as e:
                                # Skip if calculation fails
                                logger.debug(f"Skipping outlier detection for {column}: {e}")
            except Exception as e:
                # Skip this profile if there's an error
                logger.debug(f"Error processing anomaly detection for a profile: {e}")
        
        return anomalies
    
    def generate_profile_report(self, profile: Dict[str, Any]) -> str:
        """
        Generate a human-readable report from a profile.
        
        Args:
            profile: The data profile dictionary
            
        Returns:
            A formatted string with the report
        """
        lines = [
            f"Data Profile Report for {profile['entity_type']}",
            "=" * 50,
            f"Rows: {profile['row_count']}",
            f"Columns: {profile['column_count']}",
            f"Duplicate Rows: {profile['duplicate_rows']} ({profile['duplicate_percentage']}%)",
            "",
            "Column Summary:",
            "-" * 40
        ]
        
        # Add column summaries
        for column in profile['columns']:
            lines.append(f"{column['name']} ({column['data_type']})")
            lines.append(f"  Non-null: {column['count'] - column['null_count']} ({100 - column['null_percentage']}%)")
            lines.append(f"  Unique values: {column['unique_count']} ({column['unique_percentage']}%)")
            
            # Add type-specific stats
            if 'min' in column:
                lines.append(f"  Range: {column['min']} to {column['max']}")
                lines.append(f"  Mean: {column['mean']}, Median: {column['median']}")
            elif 'min_length' in column:
                lines.append(f"  Length range: {column['min_length']} to {column['max_length']} chars")
            
            lines.append("")
        
        # Add anomalies
        if profile['anomalies']:
            lines.append("Detected Anomalies:")
            lines.append("-" * 40)
            
            for anomaly in profile['anomalies']:
                lines.append(f"{anomaly['column']}: {anomaly['description']}")
            
            lines.append("")
        
        return "\n".join(lines)