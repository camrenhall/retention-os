"""
Base adapter interface for CRM data transformation.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class BaseAdapter(ABC):
    """
    Base adapter interface that all CRM-specific adapters must implement.
    """
    
    def __init__(self, config: Dict, input_dir: Path):
        """
        Initialize the adapter with configuration.
        
        Args:
            config: Configuration dictionary for the adapter
            input_dir: Directory containing input files
        """
        self.config = config
        self.input_dir = input_dir
        self.dataframes = {}
        self.field_mappings = {}
        
    @abstractmethod
    def load_mappings(self) -> Dict:
        """
        Load field mappings from configuration.
        
        Returns:
            Dict: Field mappings for the adapter
        """
        pass
        
    @abstractmethod
    def load_files(self) -> Dict[str, pd.DataFrame]:
        """
        Load data files from the input directory.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of entity name to DataFrame
        """
        pass
    
    @abstractmethod
    def transform_entity(self, entity_type: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform a specific entity from source format to canonical format.
        
        Args:
            entity_type: Type of entity to transform
            df: Source DataFrame
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        pass
    
    @abstractmethod
    def map_fields(self, entity_type: str, data: Dict) -> Dict:
        """
        Map fields from source format to canonical format for a specific entity.
        
        Args:
            entity_type: Type of entity
            data: Source data dictionary
            
        Returns:
            Dict: Transformed data dictionary
        """
        pass
    
    def get_entity_dataframe(self, entity_type: str) -> Optional[pd.DataFrame]:
        """
        Get the DataFrame for a specific entity type.
        
        Args:
            entity_type: Type of entity
            
        Returns:
            Optional[pd.DataFrame]: DataFrame for the entity type, or None if not available
        """
        return self.dataframes.get(entity_type)
    
    def get_available_entities(self) -> List[str]:
        """
        Get list of available entity types.
        
        Returns:
            List[str]: List of available entity types
        """
        return list(self.dataframes.keys())