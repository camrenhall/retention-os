"""
Output generation for RetentionOS data processing.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd

from loguru import logger

from retention_os.models.canonical_model import CanonicalDataModel


class OutputGenerator:
    """
    Generates output files in the canonical data model format.
    """
    
    def __init__(self, output_dir: Path, output_format: str = "json"):
        """
        Initialize the output generator.
        
        Args:
            output_dir: Directory for output files
            output_format: Output format (currently only json is supported)
        """
        self.output_dir = output_dir
        self.output_format = output_format.lower()
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_output(
        self, 
        entities: Dict[str, Dict], 
        source_system: str,
        business_name: str,
        files_processed: List[str],
        validation_report: Dict
    ) -> Path:
        """
        Generate output files from resolved entities.
        
        Args:
            entities: Dictionary of resolved entities
            source_system: Name of the source system
            business_name: Name of the business
            files_processed: List of processed file names
            validation_report: Validation report
            
        Returns:
            Path: Path to the main output file
        """
        if self.output_format == "json":
            return self._generate_json_output(entities, source_system, business_name, files_processed, validation_report)
        else:
            logger.warning(f"Unsupported output format: {self.output_format}, using JSON instead")
            return self._generate_json_output(entities, source_system, business_name, files_processed, validation_report)
    
    def _generate_json_output(
        self, 
        entities: Dict[str, Dict], 
        source_system: str,
        business_name: str,
        files_processed: List[str],
        validation_report: Dict
    ) -> Path:
        """
        Generate JSON output files.
        
        Args:
            entities: Dictionary of resolved entities
            source_system: Name of the source system
            business_name: Name of the business
            files_processed: List of processed file names
            validation_report: Validation report
            
        Returns:
            Path: Path to the main output file
        """
        # Create canonical data model
        model_data = {
            "process_date": datetime.now().isoformat(),
            "source_system": source_system,
            "business_name": business_name,
            "files_processed": files_processed,
            "entities_processed": {entity_type: len(entities_dict) for entity_type, entities_dict in entities.items()}
        }
        
        # Add entities
        for entity_type, entities_dict in entities.items():
            model_data[entity_type] = list(entities_dict.values())
        
        # Create a timestamp string for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create safe business name for filename
        safe_business_name = "".join(c if c.isalnum() else "_" for c in business_name)
        
        # Create output file paths
        output_file = self.output_dir / f"{safe_business_name}_{timestamp}_canonical_data.json"
        validation_file = self.output_dir / f"{safe_business_name}_{timestamp}_validation_report.json"
        
        # Write canonical data model to JSON file
        with open(output_file, "w") as f:
            json.dump(self._prepare_data_for_json(model_data), f, indent=2)
            
        # Write validation report to JSON file
        with open(validation_file, "w") as f:
            json.dump(validation_report, f, indent=2)
            
        logger.info(f"Generated canonical data model output: {output_file}")
        logger.info(f"Generated validation report: {validation_file}")
        
        return output_file
    
    def _prepare_data_for_json(self, data: Any) -> Any:
        """
        Prepare data for JSON serialization by converting datetime objects to strings.
        
        Args:
            data: Data to prepare
            
        Returns:
            Any: Prepared data
        """
        if isinstance(data, dict):
            return {k: self._prepare_data_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._prepare_data_for_json(item) for item in data]
        elif isinstance(data, (datetime, pd.Timestamp)):
            return data.isoformat()
        elif hasattr(data, "to_dict"):
            return self._prepare_data_for_json(data.to_dict())
        else:
            return data
    
    def generate_processing_report(
        self,
        entities_count: Dict[str, int],
        validation_errors: List[Dict],
        processing_warnings: List[str],
        files_processed: List[str],
        processing_time: float,
        critical_errors: bool = False
    ) -> Path:
        """
        Generate a processing report.
        
        Args:
            entities_count: Dictionary of entity type to count
            validation_errors: List of validation errors
            processing_warnings: List of processing warnings
            files_processed: List of processed file names
            processing_time: Processing time in seconds
            critical_errors: Whether critical errors occurred
            
        Returns:
            Path: Path to the processing report file
        """
        report = {
            "process_date": datetime.now().isoformat(),
            "files_processed": len(files_processed),
            "file_list": files_processed,
            "entities_processed": entities_count,
            "validation_errors": validation_errors,
            "processing_warnings": processing_warnings,
            "processing_time_seconds": processing_time,
            "critical_errors": critical_errors
        }
        
        # Create a timestamp string for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output file path
        report_file = self.output_dir / f"processing_report_{timestamp}.json"
        
        # Write report to JSON file
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Generated processing report: {report_file}")
        
        return report_file