import os
import logging
import yaml
import json
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import pandas as pd

from ..adapters.base import CRMAdapter
from ..adapters.boulevard import BoulevardAdapter
from ..models.canonical import entity_models
from ..pipeline.profiler import DataProfiler
from ..pipeline.dictionary import DataDictionary
from ..pipeline.validator import DataValidator
from ..pipeline.cleaner import DataCleaner
from ..pipeline.resolver import EntityResolver
from ..pipeline.enricher import SemanticEnricher

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Main orchestrator for the data processing pipeline.
    Coordinates all the steps in the pipeline.
    """
    
    def __init__(self, config_path_or_dict: Union[str, Dict[str, Any]]):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config_path_or_dict: Path to the pipeline configuration file or a configuration dictionary
        """
        # Find the project root directory (parent of src directory)
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
        
        if isinstance(config_path_or_dict, dict):
            self.config = config_path_or_dict
        else:
            # Convert relative path to absolute path based on project root
            if not os.path.isabs(config_path_or_dict):
                config_path_or_dict = os.path.join(self.project_root, config_path_or_dict)
            self.config = self._load_config(config_path_or_dict)
        if isinstance(config_path_or_dict, dict):
            self.config = config_path_or_dict
        else:
            self.config = self._load_config(config_path_or_dict)
        
        self.input_dir = self.config.get('input_dir', 'data/input')
        self.output_dir = self.config.get('output_dir', 'data/processed/canonical')
        self.crm_type = self.config.get('crm', {}).get('type', 'boulevard')
        self.crm_config = self.config.get('crm', {}).get('config_file', '')
        
        # Initialize component attributes
        self.adapter = None
        self.processing_statistics = {
            'start_time': None,
            'end_time': None,
            'adapter': None,
            'entities_processed': {},
            'errors': []
        }
    
    def _load_config(self, config_path: str) -> dict:
        """Load and parse configuration file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            raise
        
    # Add to the Pipeline class in src/retention_os/pipeline/orchestrator.py
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
    
    def initialize(self) -> None:
        """Initialize the pipeline components."""
        # Validate configuration
        if not self._validate_config():
            raise ValueError("Invalid pipeline configuration")
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize adapter based on CRM type
        if self.crm_type.lower() == 'boulevard':
            self.adapter = BoulevardAdapter(self.crm_config)
        else:
            raise ValueError(f"Unsupported CRM type: {self.crm_type}")
        
        # Initialize other components
        try:
            self.profiler = DataProfiler(self.config.get('profiler', {}))
            self.data_dictionary = DataDictionary(self.config.get('data_dictionary', {}))
            self.validator = DataValidator(self.config.get('validator', {}))
            self.cleaner = DataCleaner(self.config.get('cleaner', {}))
            self.resolver = EntityResolver(self.config.get('resolver', {}))
            self.enricher = SemanticEnricher(self.config.get('enricher', {}))
        except Exception as e:
            logger.error(f"Error initializing pipeline components: {e}")
            raise
        
        self.processing_statistics['adapter'] = self.crm_type
    
    def process(self) -> Dict[str, Any]:
        """Run the complete data processing pipeline."""
        self.processing_statistics['start_time'] = datetime.now().isoformat()
        
        try:
            logger.info("Initializing pipeline")
            self.initialize()
            
            # 1. Load and standardize data
            logger.info("Loading source files")
            source_files = self.adapter.get_source_files()
            logger.info(f"Found {len(source_files)} source files")
            self.processing_statistics['source_files'] = list(source_files.keys())
            self.adapter.load_source_files()
            
            # 2. Transform data to canonical model
            logger.info("Transforming to canonical model")
            canonical_data = self.adapter.transform_to_canonical()
            
            
            # 3. Optional: Profile data
            profiles = {}
            try:
                logger.info("Profiling data")
                for entity_type, entities in canonical_data.items():
                    if entities and isinstance(entities, list) and len(entities) > 0:
                        # Check if entities contain dictionaries with nested dictionaries
                        # which could cause unhashable type errors
                        try:
                            # Convert entities to DataFrame safely
                            # First flatten any nested dictionaries
                            flattened_entities = []
                            for entity in entities:
                                flat_entity = {}
                                for key, value in entity.items():
                                    if isinstance(value, dict):
                                        # For nested dictionaries, add each nested key as a separate column
                                        for nested_key, nested_value in value.items():
                                            flat_key = f"{key}_{nested_key}"
                                            flat_entity[flat_key] = nested_value
                                    else:
                                        flat_entity[key] = value
                                flattened_entities.append(flat_entity)
                            
                            df = pd.DataFrame(flattened_entities)
                            profiles[entity_type] = self.profiler.profile_dataset(df, entity_type)
                            
                            # Save profile report if configured
                            report_dir = self.config.get('profiler', {}).get('report_path', 'data/processed/profiles/')
                            os.makedirs(report_dir, exist_ok=True)
                            report_path = os.path.join(report_dir, f"{entity_type}_profile.txt")
                            with open(report_path, 'w') as f:
                                f.write(self.profiler.generate_profile_report(profiles[entity_type]))
                        except Exception as e:
                            logger.warning(f"Could not profile {entity_type}: {str(e)}")
                            # Add detailed error info
                            import traceback
                            logger.debug(f"Profiling error details: {traceback.format_exc()}")
            except Exception as e:
                logger.warning(f"Data profiling failed: {str(e)}")
                # Continue without profiling
            
            # 4. Validate canonical data
            validated_data = canonical_data  # Default to original data if validation fails
            try:
                logger.info("Validating canonical data")
                validation_results = {}
                validated_data = {}
                
                for entity_type, entities in canonical_data.items():
                    if not entities:
                        validated_data[entity_type] = entities
                        continue
                    
                    # Get validation rules (simplified for MVP)
                    validation_rules = []
                    # TODO: Load rules from configuration or data dictionary
                    
                    # Validate entities
                    result = self.validator.validate_dataset(entity_type, entities, validation_rules)
                    validation_results[entity_type] = result
                    
                    # Keep valid records
                    validated_data[entity_type] = result.get_valid_records(entities)
                    
                    # Save validation report if configured
                    report_dir = self.config.get('validator', {}).get('report_path', 'data/processed/validation/')
                    os.makedirs(report_dir, exist_ok=True)
                    report_path = os.path.join(report_dir, f"{entity_type}_validation.txt")
                    with open(report_path, 'w') as f:
                        f.write(result.generate_report())
            except Exception as e:
                logger.warning(f"Data validation failed: {e}")
                # Continue with original data
            
            # 5. Clean data
            cleaned_data = validated_data  # Default to validated data if cleaning fails
            try:
                logger.info("Cleaning data")
                cleaned_data = {}
                
                for entity_type, entities in validated_data.items():
                    if not entities:
                        cleaned_data[entity_type] = entities
                        continue
                    
                    # Get cleaning rules (simplified for MVP)
                    cleaning_rules = []
                    # TODO: Load rules from configuration
                    
                    # Clean entities
                    cleaned_data[entity_type] = self.cleaner.clean_dataset(entity_type, entities, cleaning_rules)
            except Exception as e:
                logger.warning(f"Data cleaning failed: {e}")
                # Continue with validated data
            
            # 6. Resolve entities
            resolved_data = cleaned_data  # Default to cleaned data if resolution fails
            try:
                logger.info("Resolving entities")
                resolved_data = {}
                
                for entity_type, entities in cleaned_data.items():
                    if not entities or len(entities) <= 1:
                        resolved_data[entity_type] = entities
                        continue
                    
                    # Get match rules (simplified for MVP)
                    match_rules = []
                    # TODO: Load rules from configuration
                    
                    # Resolve entities
                    resolved_entities, matches = self.resolver.resolve_entities(entities, match_rules)
                    resolved_data[entity_type] = resolved_entities
                    
                    # Save resolution report if configured
                    if matches:
                        report_dir = self.config.get('resolver', {}).get('report_path', 'data/processed/resolution/')
                        os.makedirs(report_dir, exist_ok=True)
                        report_path = os.path.join(report_dir, f"{entity_type}_resolution.txt")
                        with open(report_path, 'w') as f:
                            f.write(f"Entity Resolution Report for {entity_type}\n")
                            f.write("=" * 50 + "\n")
                            f.write(f"Total entities: {len(entities)}\n")
                            f.write(f"Resolved entities: {len(resolved_entities)}\n")
                            f.write(f"Matches found: {len(matches)}\n\n")
                            f.write("Sample matches:\n")
                            for match in matches[:10]:
                                f.write(f"- {match}\n")
            except Exception as e:
                logger.warning(f"Entity resolution failed: {e}")
                # Continue with cleaned data
            
            # 7. Enrich data
            enriched_data = resolved_data  # Default to resolved data if enrichment fails
            try:
                logger.info("Enriching data")
                
                # Build relationships between entities
                resolved_data = self.enricher.build_relationships(resolved_data)
                
                # Enrich each entity type
                enriched_data = {}
                for entity_type, entities in resolved_data.items():
                    if not entities:
                        enriched_data[entity_type] = entities
                        continue
                    
                    # Get enrichment rules (simplified for MVP)
                    enrichment_rules = []
                    # TODO: Load rules from configuration
                    
                    # Apply enrichment
                    enriched_data[entity_type] = self.enricher.enrich_entities(
                        entity_type, entities, resolved_data, enrichment_rules
                    )
            except Exception as e:
                logger.warning(f"Data enrichment failed: {e}")
                # Continue with resolved data
            
            # 8. Save canonical data
            logger.info("Saving canonical data")
            self._save_canonical_data(enriched_data)
            
            # Record entity counts
            for entity_type, entities in enriched_data.items():
                self.processing_statistics['entities_processed'][entity_type] = len(entities)
            
            logger.info("Pipeline completed successfully")
            return self._finalize_statistics()
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            self.processing_statistics['errors'].append(str(e))
            return self._finalize_statistics()
    
    def _validate_canonical_data(self, canonical_data: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
        """
        Validate canonical data using Pydantic models.
        
        Args:
            canonical_data: Dictionary mapping entity types to lists of entity dictionaries
            
        Returns:
            Validated canonical data
        """
        validated_data = {}
        
        for entity_type, entities in canonical_data.items():
            if entity_type in entity_models:
                model_class = entity_models[entity_type]
                validated_entities = []
                
                for entity_dict in entities:
                    try:
                        # Parse datetime strings to datetime objects
                        for field, value in entity_dict.items():
                            if isinstance(value, str) and field.endswith('_at'):
                                try:
                                    entity_dict[field] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                except (ValueError, TypeError):
                                    # Keep as string if parsing fails
                                    pass
                        
                        # Validate using Pydantic model
                        model_instance = model_class(**entity_dict)
                        validated_entities.append(model_instance.dict())
                    except Exception as e:
                        logger.warning(f"Validation error for {entity_type}: {e}")
                
                validated_data[entity_type] = validated_entities
            else:
                logger.warning(f"No model defined for entity type: {entity_type}")
                validated_data[entity_type] = entities
        
        return validated_data
    
    def _save_canonical_data(self, canonical_data: Dict[str, List[dict]]) -> None:
        """
        Save canonical data to output files.
        
        Args:
            canonical_data: Dictionary mapping entity types to lists of entity dictionaries
        """
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Save each entity type to a separate file
        for entity_type, entities in canonical_data.items():
            if entities:  # Only save if there are entities
                # Convert datetime objects to strings for JSON serialization
                serialized_entities = []
                for entity in entities:
                    serialized_entity = {}
                    for key, value in entity.items():
                        if isinstance(value, datetime):
                            serialized_entity[key] = value.isoformat()
                        else:
                            serialized_entity[key] = value
                    serialized_entities.append(serialized_entity)
                
                # Write to file
                output_path = os.path.join(self.output_dir, f"{entity_type}.json")
                with open(output_path, 'w') as f:
                    json.dump(serialized_entities, f, indent=2)
                
                logger.info(f"Saved {len(entities)} {entity_type} entities to {output_path}")
    
    def _finalize_statistics(self) -> Dict[str, Any]:
        """Finalize and return processing statistics."""
        self.processing_statistics['end_time'] = datetime.now().isoformat()
        
        # Calculate duration
        start_time = datetime.fromisoformat(self.processing_statistics['start_time'])
        end_time = datetime.fromisoformat(self.processing_statistics['end_time'])
        duration_seconds = (end_time - start_time).total_seconds()
        self.processing_statistics['duration_seconds'] = duration_seconds
        
        # Write statistics to file
        stats_path = os.path.join(self.output_dir, "processing_statistics.json")
        with open(stats_path, 'w') as f:
            json.dump(self.processing_statistics, f, indent=2)
        
        return self.processing_statistics