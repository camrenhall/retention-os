# src/retention_os/data_ingestion/adapter_factory.py
from typing import Dict, Any
import logging

from .base_adapter import BaseCRMAdapter
from .adapters.boulevard_adapter import BoulevardAdapter
# Import future adapters as they are implemented

class AdapterFactory:
    """
    Factory class for creating CRM adapters based on configuration.
    """
    
    @staticmethod
    def create_adapter(adapter_type: str, config: Dict[str, Any], input_dir: str, **kwargs) -> BaseCRMAdapter:
        """
        Create and return an appropriate CRM adapter based on the type.
        
        Args:
            adapter_type: Type of adapter to create (e.g., 'boulevard', 'mindbody')
            config: Configuration dictionary for the adapter
            input_dir: Directory containing input files
            **kwargs: Additional adapter-specific parameters
            
        Returns:
            Initialized CRM adapter
            
        Raises:
            ValueError: If adapter_type is not supported
        """
        logger = logging.getLogger("AdapterFactory")
        logger.info(f"Creating adapter of type: {adapter_type}")
        
        if adapter_type.lower() == 'boulevard':
            return BoulevardAdapter(config, input_dir, **kwargs)
        # Add more adapter types as they are implemented
        # elif adapter_type.lower() == 'mindbody':
        #     return MindbodyAdapter(config, input_dir, **kwargs)
        # elif adapter_type.lower() == 'zenoti':
        #     return ZenotiAdapter(config, input_dir, **kwargs)
        else:
            raise ValueError(f"Unsupported adapter type: {adapter_type}")