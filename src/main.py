import os
import sys
import logging
import argparse
import yaml
from datetime import datetime

from retention_os.pipeline.orchestrator import Pipeline
from retention_os.pipeline.profiler import DataProfiler
from retention_os.pipeline.dictionary import DataDictionary
from retention_os.pipeline.validator import DataValidator
from retention_os.pipeline.cleaner import DataCleaner
from retention_os.pipeline.resolver import EntityResolver
from retention_os.pipeline.enricher import SemanticEnricher

# Configure logging
def setup_logging(log_level='INFO'):
    """Set up logging configuration."""
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    level = log_level_map.get(log_level.upper(), logging.INFO)
    
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    # Set up log format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # Console handler
            logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')  # File handler
        ]
    )
    
    # Reduce verbosity of some third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('PIL').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def ensure_directories():
    """Create all required directories if they don't exist."""
    required_dirs = [
        'logs',
        'data/input',
        'data/processed',
        'data/processed/profiles',
        'data/processed/validation',
        'data/processed/resolution',
        'data/processed/canonical',
        'data/dictionary',
        'docs/data_dictionary'
    ]
    
    for directory in required_dirs:
        try:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}")
            raise

def load_config(config_path):
    """Load and parse configuration file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config
    except Exception as e:
        logger.error(f"Failed to load config file {config_path}: {e}")
        raise

def run_pipeline(args):
    """Run the data processing pipeline."""
    try:
        # Load configuration
        if args.config and os.path.isfile(args.config):
            # Load from file
            with open(args.config, 'r') as f:
                config = yaml.safe_load(f)
        else:
            # Use default configuration
            logger.info("Using default configuration")
            config = {
                'name': "RetentionOS Pipeline",
                'version': "1.0",
                'logging': {
                    'level': "INFO",
                    'format': "structured",
                    'output': "logs/pipeline.log"
                },
                'input_dir': args.input_dir or 'data/input',
                'output_dir': args.output_dir or 'data/processed/canonical',
                'crm': {
                    'type': args.crm or 'boulevard',
                    'config_file': 'config/adapters/boulevard.yaml'
                },
                'processing': {
                    'entity_types': [
                        'clinic', 'patient', 'provider', 'service', 'package',
                        'packageComponent', 'appointment', 'appointmentLine', 'payment',
                        'patientPackage', 'outreachMessage', 'retailSale', 'retailSaleLine'
                    ],
                    'processing_order': [
                        'clinic', 'provider', 'service', 'package', 'packageComponent',
                        'patient', 'appointment', 'appointmentLine', 'payment',
                        'patientPackage', 'retailSale', 'retailSaleLine', 'outreachMessage'
                    ]
                },
                'output': {
                    'format': "json",
                    'compress': False,
                    'path': args.output_dir or 'data/processed/canonical/',
                    'include_validation_report': True
                }
            }
        
        # Override config with command line arguments if provided
        if args.input_dir:
            config['input_dir'] = args.input_dir
        
        if args.output_dir:
            config['output_dir'] = args.output_dir
            config['output']['path'] = args.output_dir
        
        if args.crm:
            config['crm']['type'] = args.crm
        
        # Initialize and run pipeline
        logger.info(f"Initializing pipeline")
        pipeline = Pipeline(config)
        
        # Run the pipeline
        stats = pipeline.process()
        
        # Log results
        logger.info(f"Pipeline completed successfully")
        logger.info(f"Processed {sum(stats['entities_processed'].values())} entities")
        logger.info(f"Processing time: {stats['duration_seconds']:.2f} seconds")
        
        return 0
    
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        return 1

def main():
    """Main entry point for the RetentionOS data processing pipeline."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='RetentionOS Data Processing Pipeline')
    
    parser.add_argument(
        '--config', 
        default='../config/pipeline.yaml',
        help='Path to pipeline configuration file'
    )
    
    parser.add_argument(
        '--input-dir', 
        help='Directory containing input files (overrides config)'
    )
    
    parser.add_argument(
        '--output-dir', 
        help='Directory for output files (overrides config)'
    )
    
    parser.add_argument(
        '--crm', 
        choices=['boulevard', 'mindbody', 'zenoti'],
        help='CRM type (overrides config)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (overrides config)'
    )
    
    parser.add_argument(
        '--include-vouchers',
        action='store_true',
        help='Process voucher redemptions'
    )
    
    parser.add_argument(
        '--include-staff-metrics',
        action='store_true',
        help='Include staff performance metrics'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate data without processing'
    )
    
    parser.add_argument(
        '--profile-only',
        action='store_true',
        help='Only profile data without full processing'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip data validation step'
    )
    
    parser.add_argument(
        '--skip-enrichment',
        action='store_true',
        help='Skip data enrichment step'
    )
    
    parser.add_argument(
        '--export-dictionary',
        action='store_true',
        help='Export data dictionary documentation'
    )
    
    args = parser.parse_args()
    
    try:
        # Setup logging
        setup_logging()
        
        # Ensure all required directories exist
        ensure_directories()
        
        # Handle specific modes
        if args.validate_only:
            logger.info("Running in validation-only mode")
            # Implementation for validation-only mode
            config = load_config(args.config)
            dictionary = DataDictionary(config.get('data_dictionary', {}))
            validator = DataValidator(config.get('validator', {}))
            # ... validation logic ...
            return 0
        
        if args.profile_only:
            logger.info("Running in profile-only mode")
            # Implementation for profile-only mode
            config = load_config(args.config)
            profiler = DataProfiler(config.get('profiler', {}))
            # ... profiling logic ...
            return 0
        
        if args.export_dictionary:
            logger.info("Exporting data dictionary documentation")
            config = load_config(args.config)
            dictionary = DataDictionary(config.get('data_dictionary', {}))
            output_path = dictionary.export_documentation(
                output_format='markdown',
                output_path='docs/data_dictionary'
            )
            logger.info(f"Data dictionary documentation exported to {output_path}")
            return 0
        
        # Run full pipeline
        return run_pipeline(args)
        
    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())