#!/usr/bin/env python
"""
Main processing script for RetentionOS data transformation.

Usage:
    python process.py --config config/config.json --business "Med Spa Name" --adapter boulevard
"""
import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

# Add the src directory to the Python path for imports to work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from retention_os.adapters import BoulevardAdapter
from retention_os.validation import Validator
from retention_os.resolution import EntityResolver
from retention_os.output import OutputGenerator


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="RetentionOS data processing")
    parser.add_argument(
        "--config", 
        type=str, 
        default="config/config.json", 
        help="Path to configuration file"
    )
    parser.add_argument(
        "--business", 
        type=str, 
        required=True, 
        help="Name of the business to process"
    )
    parser.add_argument(
        "--adapter", 
        type=str, 
        default="boulevard", 
        choices=["boulevard"], 
        help="Adapter to use for data processing"
    )
    parser.add_argument(
        "--input-dir", 
        type=str, 
        help="Input directory override"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        help="Output directory override"
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
        help="Logging level"
    )
    parser.add_argument(
        "--show-columns", 
        action="store_true",
        help="Show CSV column names for each file"
    )
    
    return parser.parse_args()


def load_config(config_path: str) -> Dict:
    """
    Load configuration from file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dict: Configuration
    """
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        sys.exit(1)


def setup_logging(config: Dict, log_level: str):
    """
    Set up logging.
    
    Args:
        config: Configuration
        log_level: Logging level
    """
    # Get log file path from config
    log_file = config.get("logging", {}).get("file", "logs/processing.log")
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up loguru
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level=log_level)
    logger.add(log_file, level=log_level, rotation="10 MB", retention="30 days")
    
    logger.info(f"Logging set up at level {log_level}")


def create_adapter(adapter_name: str, config: Dict, input_dir: Path) -> BoulevardAdapter:
    """
    Create the appropriate adapter based on the adapter name.
    
    Args:
        adapter_name: Name of the adapter
        config: Configuration
        input_dir: Input directory
        
    Returns:
        Adapter instance
    """
    if adapter_name == "boulevard":
        adapter_config = config.get("adapters", {}).get("boulevard", {})
        return BoulevardAdapter(adapter_config, input_dir)
    else:
        logger.error(f"Unsupported adapter: {adapter_name}")
        sys.exit(1)


def examine_input_files(input_dir: Path, show_columns: bool = False):
    """
    Examine input files and print information about them.
    
    Args:
        input_dir: Input directory
        show_columns: Whether to show CSV column names
    """
    logger.info(f"Examining input files in {input_dir}")
    
    # Get all CSV files in the input directory
    csv_files = list(input_dir.glob("*.csv"))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files:")
    
    for file_path in csv_files:
        file_size = file_path.stat().st_size
        logger.info(f"  - {file_path.name} ({file_size / 1024:.2f} KB)")
        
        if show_columns:
            try:
                df = pd.read_csv(file_path)
                logger.info(f"    Columns: {list(df.columns)}")
                logger.info(f"    Shape: {df.shape}")
            except Exception as e:
                logger.error(f"    Error reading file: {e}")


def process_data(
    adapter: BoulevardAdapter,
    business_name: str,
    config: Dict,
    output_dir: Path
) -> Tuple[bool, Dict]:
    """
    Process data using the adapter.
    
    Args:
        adapter: Adapter instance
        business_name: Name of the business
        config: Configuration
        output_dir: Output directory
        
    Returns:
        Tuple[bool, Dict]: Success flag and summary
    """
    start_time = time.time()
    
    try:
        # Step 1: Load data
        logger.info(f"Loading data for {business_name}")
        dataframes = adapter.load_files()
        
        files_processed = [
            f"{entity_type}: {len(df)} rows" 
            for entity_type, df in dataframes.items()
        ]
        
        # Check if any data was loaded
        if not dataframes:
            logger.error("No data loaded")
            return False, {}
        
        logger.info(f"Loaded {len(dataframes)} files")
        
        # Step 2: Transform data
        transformed_dataframes = {}
        validation_rules = adapter.validation_rules
        validator = Validator(validation_rules, strict=config.get("validation", {}).get("strict", False))
        
        # First, process regular entities
        for entity_type, df in dataframes.items():
            logger.info(f"Transforming {entity_type} data")
            
            # Skip derived entities for now
            mapping = adapter.entity_mappings.get(entity_type, {})
            if mapping.get("derived", False):
                continue
                
            transformed_df = adapter.transform_entity(entity_type, df)
            
            # Validate transformed data
            clean_df, valid = validator.validate_entity(entity_type, transformed_df)
            transformed_dataframes[entity_type] = clean_df
            
            if not valid:
                logger.warning(f"Validation issues found in {entity_type} data")
        
        # Now explicitly process derived entities
        for entity_type, mapping in adapter.entity_mappings.items():
            if mapping.get("derived", False):
                logger.info(f"Transforming derived entity: {entity_type}")
                transformed_df = adapter.transform_entity(entity_type, pd.DataFrame())
                
                # Validate derived entity
                clean_df, valid = validator.validate_entity(entity_type, transformed_df)
                transformed_dataframes[entity_type] = clean_df
                
                if not valid:
                    logger.warning(f"Validation issues found in derived entity {entity_type}")
        
        # Log transformed entities
        for entity_type, df in transformed_dataframes.items():
            logger.info(f"Transformed {entity_type}: {len(df)} rows")
            if not df.empty and len(df) > 0:
                logger.debug(f"Columns: {list(df.columns)}")
                logger.debug(f"First row: {df.iloc[0].to_dict()}")
        
        validation_report = validator.get_validation_report()
        
        # Step 3: Resolve entities
        logger.info("Resolving entities")
        resolver = EntityResolver()
        entities = resolver.resolve_entities(transformed_dataframes)
        
        for entity_type, df in transformed_dataframes.items():
            logger.info(f"Transformed {entity_type}: {len(df)} rows")
            if not df.empty and len(df) > 0:
                logger.debug(f"Columns: {list(df.columns)}")
                logger.debug(f"First row: {df.iloc[0].to_dict()}")
        
        # Step 4: Generate output
        logger.info("Generating output")
        output_generator = OutputGenerator(output_dir, config.get("output", {}).get("format", "json"))
        output_file = output_generator.generate_output(
            entities,
            adapter.__class__.__name__,
            business_name,
            list(dataframes.keys()),
            validation_report
        )
        
        
        # Generate processing report
        entities_count = {entity_type: len(entities_list) for entity_type, entities_list in entities.items()}
        processing_time = time.time() - start_time
        report_file = output_generator.generate_processing_report(
            entities_count,
            validation_report.get("errors", []),
            validation_report.get("warnings", []),
            list(dataframes.keys()),
            processing_time,
            validation_report.get("critical_errors", False)
        )
        
        summary = {
            "files_processed": len(dataframes),
            "file_list": list(dataframes.keys()),
            "entities_processed": entities_count,
            "total_entities": sum(entities_count.values()),
            "validation_errors": len(validation_report.get("errors", [])),
            "validation_warnings": len(validation_report.get("warnings", [])),
            "output_file": str(output_file),
            "report_file": str(report_file),
            "processing_time": processing_time
        }
        
        return True, summary
    
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        logger.error(traceback.format_exc())
        return False, {"error": str(e)}


def main():
    """Main entry point."""
    # Parse arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config(args.config)
    
    # Set up logging
    setup_logging(config, args.log_level)
    
    # Log startup information
    logger.info("=" * 80)
    logger.info(f"RetentionOS data processing started for {args.business}")
    logger.info(f"Using adapter: {args.adapter}")
    
    # Set input and output directories
    input_dir = Path(args.input_dir) if args.input_dir else Path(config.get("input", {}).get("directory", "data/input"))
    output_dir = Path(args.output_dir) if args.output_dir else Path(config.get("output", {}).get("directory", "data/output"))
    
    # Create directories if they don't exist
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    
    # Examine input files
    examine_input_files(input_dir, args.show_columns)
    
    # Create adapter
    adapter = create_adapter(args.adapter, config, input_dir)
    
    # Process data
    success, summary = process_data(adapter, args.business, config, output_dir)
    
    # Log summary
    if success:
        logger.info("Data processing completed successfully")
        logger.info(f"Summary: {json.dumps(summary, indent=2)}")
    else:
        logger.error("Data processing failed")
        if "error" in summary:
            logger.error(f"Error: {summary['error']}")
    
    logger.info("=" * 80)


if __name__ == "__main__":
    main()