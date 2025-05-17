# RetentionOS MVP

RetentionOS is a lightweight data processing system that transforms CRM data into a standardized Canonical Data Model for client re-engagement.

## Overview

The MVP implementation focuses on transforming Boulevard CRM CSV exports into a clean, standardized data model to enable basic client segmentation for re-engagement in med spa businesses.

## Features

- Boulevard CSV file parsing with field mappings
- Data validation and cleansing
- Entity resolution across data sources
- JSON output generation in Canonical Data Model format
- Validation and error reporting

## Getting Started

### Prerequisites

- Python 3.11+
- Required packages (see `requirements.txt`)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/retention-os-mvp.git
cd retention-os-mvp
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Directory Structure

```
retention-os-mvp/
├── config/                      # Configuration files
│   ├── config.json              # Main configuration
│   └── adapters/
│       └── boulevard_mappings.json  # Boulevard field mappings
├── src/                         # Source code
│   ├── retention_os/            # Main package
│   └── scripts/                 # Processing scripts
├── data/                        # Data files
│   ├── input/                   # Raw input files
│   └── output/                  # Processed outputs
├── logs/                        # Log files
└── README.md                    # This file
```

## Usage

1. Place Boulevard CSV exports in the `data/input/` directory.

2. Run the processing script:
```bash
python src/scripts/process.py --business "Med Spa Name" --adapter boulevard
```

3. Check the output files in the `data/output/` directory.

### Command-Line Arguments

- `--config`: Path to configuration file (default: `config/config.json`)
- `--business`: Name of the business to process (required)
- `--adapter`: Adapter to use for data processing (default: boulevard)
- `--input-dir`: Input directory override
- `--output-dir`: Output directory override
- `--log-level`: Logging level (default: INFO)

## Configuration

### Main Configuration (`config/config.json`)

```json
{
  "version": "0.1.0",
  "adapters": {
    "boulevard": {
      "enabled": true,
      "mapping_file": "config/adapters/boulevard_mappings.json"
    }
  },
  "input": {
    "directory": "data/input"
  },
  "output": {
    "directory": "data/output",
    "format": "json"
  },
  "validation": {
    "strict": false,
    "report_file": "validation_report.json"
  },
  "logging": {
    "level": "INFO",
    "file": "logs/processing.log"
  }
}
```

## Canonical Data Model

The MVP implements all 13 entities from the Canonical Data Model:

1. **Business** - Store med spa information
2. **Client** - Customer records
3. **Professional** - Staff/providers
4. **Service** - Treatment offerings
5. **Package** - Bundles of services
6. **PackageComponent** - Services within packages
7. **Appointment** - Scheduled visits
8. **AppointmentLine** - Services within appointments
9. **Payment** - Financial transactions
10. **ClientPackage** - Packages purchased by clients
11. **OutreachMessage** - Marketing communications
12. **ProductSale** - Retail product sales
13. **ProductSaleLine** - Individual products within sales

## Adding Support for Additional CRMs

To add support for additional CRMs:

1. Create a new adapter class that inherits from `BaseAdapter`
2. Implement all required methods for the adapter
3. Create a mapping configuration file for the new CRM
4. Update the `create_adapter` function in `process.py` to include the new adapter

## License

This project is proprietary and confidential.

## Contact

For support or questions, please contact support@yourcompany.com.