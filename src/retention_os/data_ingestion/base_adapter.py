# src/retention_os/data_ingestion/base_adapter.py
import os
import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple

class BaseCRMAdapter(ABC):
    """
    Abstract base class for all CRM adapters. Provides common functionality
    and defines the interface that all CRM adapters must implement.
    """
    
    def __init__(self, config: Dict[str, Any], input_dir: str, **kwargs):
        """
        Initialize the adapter with configuration and input directory.
        
        Args:
            config: Configuration dictionary with mappings and settings
            input_dir: Directory containing raw input files
            **kwargs: Additional adapter-specific parameters
        """
        self.config = config
        self.input_dir = input_dir
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.dataframes = {}  # Will hold the loaded dataframes
        
    def process(self) -> Dict[str, pd.DataFrame]:
        """
        Execute the full processing pipeline for this adapter.
        
        Returns:
            Dictionary of standardized entity dataframes
        """
        self.logger.info(f"Beginning data ingestion process for {self.__class__.__name__}")
        
        # Load raw files
        self.load_source_files()
        
        # Check if any files were loaded
        if not self.dataframes:
            self.logger.warning("No files were loaded. Check file paths and configurations.")
            return {}
        
        # Log the loaded dataframes
        self.logger.info(f"Loaded {len(self.dataframes)} dataframes: {list(self.dataframes.keys())}")
        
        # Apply standardization and transformations
        standardized_data = self.standardize_schema()
        
        # Check if any standardized data was created
        if not standardized_data:
            self.logger.warning("No standardized data was created. Check field mappings and transformations.")
            return {}
        
        self.logger.info(f"Created {len(standardized_data)} standardized entities: {list(standardized_data.keys())}")
        
        self.logger.info(f"Completed data ingestion process for {self.__class__.__name__}")
        return standardized_data
        
    def load_source_files(self) -> None:
        """
        Load all required source files from the input directory
        into pandas dataframes.
        """
        self.logger.info(f"Loading source files from {self.input_dir}")
        
        file_mappings = self.config.get('file_mappings', {})
        self.logger.info(f"File mappings: {file_mappings}")
        
        for entity_name, file_pattern in file_mappings.items():
            file_path = os.path.join(self.input_dir, file_pattern)
            self.logger.info(f"Looking for file: {file_path}")
            
            if not os.path.exists(file_path):
                self.logger.warning(f"File not found for entity {entity_name}: {file_path}")
                continue
                
            try:
                encoding = self.detect_file_encoding(file_path)
                self.logger.info(f"Loading {file_path} with encoding {encoding}")
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                self.dataframes[entity_name] = df
                self.logger.info(f"Loaded {len(df)} rows for entity {entity_name}")
            except Exception as e:
                self.logger.error(f"Error loading file {file_path}: {str(e)}")
                # Continue with other files instead of raising
                continue
    
    def detect_file_encoding(self, file_path: str) -> str:
        """
        Detect the encoding of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Detected encoding (defaults to utf-8)
        """
        # In a real implementation, we would use chardet or a similar library
        # For simplicity, we'll default to utf-8
        return 'utf-8'
    
    @abstractmethod
    def standardize_schema(self) -> Dict[str, pd.DataFrame]:
        """
        Transform source dataframes into standardized canonical format.
        
        This method must be implemented by each concrete adapter class.
        
        Returns:
            Dictionary of standardized dataframes, keyed by entity name
        """
        pass
    
    # src/retention_os/data_ingestion/base_adapter.py

    def apply_field_mapping(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        """
        Apply field mapping from source to target schema based on configuration.
        
        Args:
            df: Source dataframe
            entity_name: Name of the entity being processed
            
        Returns:
            Dataframe with standardized field names
        """
        field_mappings = self.config.get('field_mappings', {}).get(entity_name, {})
        if not field_mappings:
            self.logger.warning(f"No field mappings found for entity {entity_name}")
            return pd.DataFrame()
            
        # Create a new dataframe with mapped columns
        mapped_df = pd.DataFrame()
        
        # Track missing source fields for debugging
        missing_source_fields = []
        
        for target_field, source_field in field_mappings.items():
            if source_field in df.columns:
                mapped_df[target_field] = df[source_field]
            else:
                missing_source_fields.append(source_field)
                mapped_df[target_field] = None
        
        if missing_source_fields:
            self.logger.warning(f"Missing source fields for {entity_name}: {', '.join(missing_source_fields)}")
        
        # Print some diagnostic information
        self.logger.info(f"Applied field mapping for {entity_name}, resulting in {len(mapped_df.columns)} columns: {list(mapped_df.columns)}")
        
        return mapped_df
    
    def apply_type_conversions(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        """
        Apply data type conversions based on configuration.
        
        Args:
            df: Source dataframe
            entity_name: Name of the entity being processed
            
        Returns:
            Dataframe with converted data types
        """
        type_conversions = self.config.get('type_conversions', {})
        entity_conversions = {k: v for k, v in type_conversions.items() 
                             if k.startswith(f"{entity_name}.")}
        
        for field_key, conversion in entity_conversions.items():
            field = field_key.split('.')[1]
            
            if field not in df.columns:
                continue
                
            try:
                if isinstance(conversion, str):
                    # Simple type conversion
                    if conversion == 'string':
                        df[field] = df[field].astype(str)
                    elif conversion == 'float':
                        df[field] = pd.to_numeric(df[field], errors='coerce')
                    elif conversion == 'int':
                        df[field] = pd.to_numeric(df[field], errors='coerce').astype('Int64')
                    elif conversion == 'boolean':
                        df[field] = df[field].map({'true': True, 'false': False, True: True, False: False})
                elif isinstance(conversion, dict):
                    # Complex conversion with format
                    if conversion.get('format') == 'datetime':
                        pattern = conversion.get('pattern', '%Y-%m-%d %H:%M:%S')
                        df[field] = pd.to_datetime(df[field], format=pattern, errors='coerce')
            except Exception as e:
                self.logger.error(f"Error converting field {field}: {str(e)}")
                
        return df
    
    def generate_surrogate_key(self, df: pd.DataFrame, key_field: str, prefix: str) -> pd.DataFrame:
        """
        Generate a surrogate key for the entity.
        
        Args:
            df: Source dataframe
            key_field: Name of the target surrogate key field
            prefix: Prefix for the surrogate key (e.g., 'P' for patient)
            
        Returns:
            Dataframe with added surrogate key
        """
        df[key_field] = [f"{prefix}{i+1:06d}" for i in range(len(df))]
        return df   