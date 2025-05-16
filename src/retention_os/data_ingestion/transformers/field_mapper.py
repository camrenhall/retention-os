# src/retention_os/data_ingestion/transformers/field_mapper.py
import pandas as pd
import logging
from typing import Dict, List, Optional, Any

class FieldMapper:
    """
    Utility class for mapping fields from source to target schema.
    """
    
    def __init__(self, mappings: Dict[str, Dict[str, str]]):
        """
        Initialize the field mapper with mapping configurations.
        
        Args:
            mappings: Dictionary of entity-specific field mappings
        """
        self.mappings = mappings
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def map_fields(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        """
        Map fields from source dataframe to target schema.
        
        Args:
            df: Source dataframe
            entity_name: Name of the entity being processed
            
        Returns:
            Dataframe with mapped field names
        """
        if entity_name not in self.mappings:
            self.logger.warning(f"No mappings found for entity {entity_name}")
            return df
            
        entity_mappings = self.mappings[entity_name]
        
        # Create a new dataframe with mapped columns
        mapped_df = pd.DataFrame()
        
        # Track missing source fields for debugging
        missing_source_fields = []
        
        for target_field, source_field in entity_mappings.items():
            if source_field in df.columns:
                mapped_df[target_field] = df[source_field]
            else:
                missing_source_fields.append(source_field)
                mapped_df[target_field] = None
        
        if missing_source_fields:
            self.logger.warning(f"Missing source fields for {entity_name}: {', '.join(missing_source_fields)}")
        
        return mapped_df
    
    def get_reverse_mapping(self, entity_name: str) -> Dict[str, str]:
        """
        Get the reverse mapping (target to source) for an entity.
        
        This is useful for debugging and validation purposes.
        
        Args:
            entity_name: Name of the entity
            
        Returns:
            Dictionary mapping target field names to source field names
        """
        if entity_name not in self.mappings:
            return {}
            
        return {v: k for k, v in self.mappings[entity_name].items()}