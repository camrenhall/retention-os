# src/retention_os/data_ingestion/orchestrator.py
import os
import logging
import yaml
from typing import Dict, Any, Optional

from .adapter_factory import AdapterFactory

class DataIngestionOrchestrator:
    """
    Main orchestrator for the data ingestion process.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize the orchestrator with a configuration file.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config_path = config_path
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = None
        
    def load_config(self) -> Dict[str, Any]:
        """
        Load the configuration from the YAML file.
        
        Returns:
            Loaded configuration dictionary
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
                
            self.logger.info(f"Loaded configuration from {self.config_path}")
            return self.config
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    # src/retention_os/data_ingestion/orchestrator.py

    def run(self, input_dir: str, output_dir: str) -> bool:
        """
        Run the data ingestion process.
        
        Args:
            input_dir: Directory containing input files
            output_dir: Directory to write output files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load configuration if not already loaded
            if self.config is None:
                self.load_config()
            
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Get the CRM type from configuration
            crm_type = self.config.get('crm', {}).get('type')
            if not crm_type:
                raise ValueError("CRM type not specified in configuration")
                
            # Load CRM-specific configuration
            crm_config_file = self.config.get('crm', {}).get('config_file')
            crm_config = {}
            
            if crm_config_file:
                try:
                    with open(crm_config_file, 'r') as f:
                        crm_config = yaml.safe_load(f)
                    self.logger.info(f"Loaded CRM configuration from {crm_config_file}")
                    self.logger.info(f"CRM config keys: {list(crm_config.keys())}")
                    
                    # For boulevard config, ensure we're using the right key
                    if crm_type.lower() == 'boulevard' and 'boulevard' in crm_config:
                        crm_config = crm_config['boulevard']
                        self.logger.info(f"Using boulevard-specific config with keys: {list(crm_config.keys())}")
                except Exception as e:
                    self.logger.error(f"Error loading CRM configuration: {str(e)}")
                    raise
            else:
                self.logger.error(f"No CRM configuration file specified for {crm_type}")
                return False
            
            # Check if file_mappings exist in the CRM config
            if 'file_mappings' not in crm_config:
                self.logger.error(f"No file_mappings found in {crm_type} configuration")
                return False
            
            # Create the appropriate adapter
            adapter = AdapterFactory.create_adapter(
                crm_type, 
                crm_config, 
                input_dir
            )
            
            # Process the data
            self.logger.info(f"Processing data with {crm_type} adapter")
            standardized_data = adapter.process()
            
            self.logger.info(f"Standardized data contains the following entities: {list(standardized_data.keys())}")
            for entity_name, df in standardized_data.items():
                self.logger.info(f"Entity {entity_name} has {len(df)} records")
            
            # Write the results to output files
            self.logger.info(f"Writing results to {output_dir}")
            
            if not standardized_data:
                self.logger.warning("No standardized data to write")
                return False
            
            for entity_name, df in standardized_data.items():
                try:
                    output_file = os.path.join(output_dir, f"{entity_name}.json")
                    self.logger.info(f"Writing {len(df)} records for {entity_name} to {output_file}")
                    df.to_json(output_file, orient='records', date_format='iso')
                    
                    # Verify the file was created
                    if os.path.exists(output_file):
                        file_size = os.path.getsize(output_file)
                        self.logger.info(f"Successfully wrote {file_size} bytes to {output_file}")
                    else:
                        self.logger.error(f"Failed to create output file {output_file}")
                except Exception as e:
                    self.logger.error(f"Error writing {entity_name} to {output_file}: {str(e)}")
            
            self.logger.info("Data ingestion completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in data ingestion process: {str(e)}")
            return False