# scripts/run_ingestion.py
import os
import logging
import sys
from pathlib import Path
import json

# Add the parent directory to the path so we can import our package
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.retention_os.data_ingestion.orchestrator import DataIngestionOrchestrator

def setup_logging():
    """Set up logging configuration."""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/ingestion.log', mode='w')
        ]
    )

def check_paths(input_dir, output_dir, config_path):
    """Check if required paths exist."""
    logger = logging.getLogger("path_checker")
    
    errors = []
    
    # Check input directory
    if not os.path.exists(input_dir):
        msg = f"Input directory {input_dir} does not exist"
        logger.error(msg)
        errors.append(msg)
    else:
        # List files in input directory
        files = os.listdir(input_dir)
        logger.info(f"Found {len(files)} files in input directory: {files}")
        
    # Check output directory, create if not exists
    if not os.path.exists(output_dir):
        logger.info(f"Creating output directory {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
    
    # Check config file
    if not os.path.exists(config_path):
        msg = f"Config file {config_path} does not exist"
        logger.error(msg)
        errors.append(msg)
    
    # Check config/crm_adapters directory
    crm_adapters_dir = os.path.join(os.path.dirname(config_path), "crm_adapters")
    if not os.path.exists(crm_adapters_dir):
        msg = f"CRM adapters directory {crm_adapters_dir} does not exist"
        logger.error(msg)
        errors.append(msg)
    
    return len(errors) == 0

def main():
    """Run the data ingestion pipeline."""
    # Set up logging
    setup_logging()
    logger = logging.getLogger("main")
    
    try:
        # Define paths
        config_path = "config/pipeline.yaml"
        input_dir = "data/input"
        output_dir = "data/processed/canonical"
        
        # Check paths
        if not check_paths(input_dir, output_dir, config_path):
            logger.error("Path validation failed. Check log for details.")
            return 1
        
        # Initialize and run the orchestrator
        logger.info("Initializing orchestrator")
        orchestrator = DataIngestionOrchestrator(config_path)
        
        logger.info("Loading configuration")
        config = orchestrator.load_config()
        logger.info(f"Configuration loaded: {json.dumps(config, indent=2)}")
        
        logger.info(f"Running orchestrator with input_dir={input_dir}, output_dir={output_dir}")
        success = orchestrator.run(input_dir, output_dir)
        
        # Check output directory
        if success:
            if os.path.exists(output_dir):
                output_files = os.listdir(output_dir)
                if output_files:
                    logger.info(f"Found {len(output_files)} files in output directory: {output_files}")
                else:
                    logger.warning(f"No files found in output directory {output_dir}")
            else:
                logger.error(f"Output directory {output_dir} does not exist")
            
            logger.info("Data ingestion completed successfully")
            return 0
        else:
            logger.error("Data ingestion failed")
            return 1
    
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())