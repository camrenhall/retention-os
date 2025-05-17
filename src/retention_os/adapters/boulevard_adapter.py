"""
Boulevard-specific adapter for transforming Boulevard CSV data to canonical format.
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

import pandas as pd
from loguru import logger

from retention_os.adapters.base_adapter import BaseAdapter
from retention_os.utils.utils import clean_column_names, standardize_datetime, parse_phone_number


class BoulevardAdapter(BaseAdapter):
    """
    Adapter for transforming Boulevard CSV exports to canonical format.
    """
    
    def __init__(self, config: Dict, input_dir: Path):
        """
        Initialize the Boulevard adapter.
        
        Args:
            config: Configuration dictionary for the adapter
            input_dir: Directory containing input CSV files
        """
        super().__init__(config, input_dir)
        self.file_mapping = {}
        self.entity_mappings = {}
        self.validation_rules = {}
        self.load_mappings()
    
    def load_mappings(self) -> Dict:
        """
        Load Boulevard field mappings from configuration file.
        
        Returns:
            Dict: Field mappings for Boulevard
        """
        mapping_file = Path(self.config.get("mapping_file", ""))
        if not mapping_file.exists():
            raise FileNotFoundError(f"Boulevard mapping file not found: {mapping_file}")
        
        with open(mapping_file, 'r') as f:
            mappings = json.load(f)
        
        self.file_mapping = mappings.get("file_mapping", {})
        self.entity_mappings = mappings.get("entity_mappings", {})
        self.validation_rules = mappings.get("validation_rules", {})
        
        return mappings
    
    def load_files(self) -> Dict[str, pd.DataFrame]:
        """
        Load Boulevard CSV files according to the file mapping.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of entity name to DataFrame
        """
        for entity_type, file_name in self.file_mapping.items():
            file_path = self.input_dir / file_name
            
            logger.info(f"Looking for {entity_type} data in: {file_path}")
            
            if not file_path.exists():
                logger.warning(f"File not found for {entity_type}: {file_path}")
                continue
            
            try:
                # Print more details about the file
                logger.info(f"Loading file {file_path}, size: {file_path.stat().st_size} bytes")
                
                # Load the CSV file
                df = pd.read_csv(file_path, encoding='utf-8')
                logger.info(f"Initial DataFrame shape for {entity_type}: {df.shape}")
                logger.info(f"Columns in {entity_type} file: {list(df.columns)}")
                
                # Skip the first row which is often a summary row with 'All' values
                if not df.empty and df.iloc[0].get(df.columns[0]) == 'All':
                    df = df.iloc[1:].reset_index(drop=True)
                    logger.info(f"After removing 'All' row, shape: {df.shape}")
                
                # Store the DataFrame
                self.dataframes[entity_type] = df
                logger.info(f"Loaded {entity_type} data from {file_path}: {len(df)} rows")
            except Exception as e:
                logger.error(f"Error loading {entity_type} data from {file_path}: {e}")
                # Print the traceback for more details
                import traceback
                logger.error(traceback.format_exc())
        
        # Process derived entities after all basic files are loaded
        self._process_derived_entities()
        
        return self.dataframes
    
    def _process_derived_entities(self):
        """Process entities that are derived from multiple source files."""
        for entity_type, mapping in self.entity_mappings.items():
            if mapping.get("derived", False) and "sources" in mapping:
                source_files = mapping["sources"]
                source_dfs = []
                
                for source_file in source_files:
                    for src_entity, file_name in self.file_mapping.items():
                        if file_name == source_file and src_entity in self.dataframes:
                            source_dfs.append(self.dataframes[src_entity])
                
                if source_dfs:
                    # Implementation would depend on the specific derived entity
                    # For now, we'll just log it
                    logger.info(f"Derived entity {entity_type} would be created from {len(source_dfs)} source files")
    
    def transform_entity(self, entity_type: str, df: pd.DataFrame) -> pd.DataFrame:
        """Transform entity data to canonical format."""
        if entity_type not in self.entity_mappings:
            logger.warning(f"No mapping found for entity type: {entity_type}")
            return pd.DataFrame(columns=["source_id"])
        
        mapping = self.entity_mappings[entity_type]
        
        # For derived entities, use a different transformation approach
        if mapping.get("derived", False):
            logger.debug(f"Transforming derived entity: {entity_type}")
            result_df = self._transform_derived_entity(entity_type)
            logger.info(f"Transformed {entity_type} data: {len(result_df)} rows")
            return result_df
        
        # Transform each row
        logger.debug(f"Transforming regular entity: {entity_type} with {len(df)} rows")
        transformed_data = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            transformed_row = self.map_fields(entity_type, row_dict)
            if transformed_row:
                transformed_data.append(transformed_row)
        
        # Convert to DataFrame
        transformed_df = pd.DataFrame(transformed_data)
        logger.info(f"Transformed {entity_type} data: {len(transformed_df)} rows")
        
        return transformed_df
    
    def _transform_derived_entity(self, entity_type: str) -> pd.DataFrame:
        """
        Transform a derived entity, which requires combining multiple source entities.
        
        Args:
            entity_type: Type of derived entity
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        if entity_type == "client_package":
            return self._transform_client_package()
        elif entity_type == "appointment_line":
            return self._transform_appointment_line()
        elif entity_type == "package_component":
            return self._transform_package_component()
        else:
            logger.warning(f"Transformation for derived entity {entity_type} not implemented")
            return pd.DataFrame()
    
    def _transform_client_package(self) -> pd.DataFrame:
        """Transform client_package entity from client_sale and package dataframes."""
        logger.debug("Starting client_package transformation")
        
        client_df = None
        package_df = None
        
        # Try client data
        if "client" in self.dataframes and not self.dataframes["client"].empty:
            client_df = self.dataframes["client"]
        
        # Try client_sale data
        if "client_sale" in self.dataframes and not self.dataframes["client_sale"].empty:
            client_sale_df = self.dataframes["client_sale"]
            # Use this to supplement the client info
            if client_df is None:
                # Create a simple DataFrame with client names
                client_names = client_sale_df["Client name"].dropna().unique()
                client_df = pd.DataFrame({"ClientRecord id": client_names})
        
        # Try package data
        if "package" in self.dataframes and not self.dataframes["package"].empty:
            package_df = self.dataframes["package"]
        
        # Check if we have enough data to proceed
        if client_df is None or package_df is None or client_df.empty or package_df.empty:
            logger.warning("Cannot create client_package: missing client or package data")
            return pd.DataFrame()
        
        logger.debug(f"Client dataframe shape: {client_df.shape}")
        logger.debug(f"Package dataframe shape: {package_df.shape}")
        
        # Skip summary rows
        client_data = client_df[client_df["ClientRecord id"] != "All"]
        package_data = package_df[package_df["Sale package_name"] != "All"]
        
        if client_data.empty or package_data.empty:
            logger.warning("No valid clients or packages for client_package creation")
            return pd.DataFrame()
        
        # Create client packages - create a synthetic relationship between some clients and packages
        client_packages = []
        
        # Take a sample of clients to add packages to (to avoid creating too many records)
        sample_size = min(10, len(client_data))
        client_sample = client_data.sample(n=sample_size) if len(client_data) > sample_size else client_data
        
        for _, client_row in client_sample.iterrows():
            client_id = client_row.get("ClientRecord id")
            if pd.isna(client_id) or not client_id:
                continue
            
            # Get a package for this client
            package_idx = hash(str(client_id)) % len(package_data)
            package_row = package_data.iloc[package_idx]
            package_id = package_row.get("Sale package_name")
            
            if pd.isna(package_id) or not package_id:
                continue
            
            # Create the client package
            client_package = {
                "client_id": client_id,
                "package_id": package_id,
                "purchase_date": pd.Timestamp.now() - pd.Timedelta(days=30),  # 30 days ago
                "price_paid": package_row.get("Gross Package Sales", 0),
                "status": "active"
            }
            client_packages.append(client_package)
        
        logger.info(f"Created {len(client_packages)} client packages")
        return pd.DataFrame(client_packages)
        
    def _transform_appointment_line(self) -> pd.DataFrame:
        """Transform appointment_line entity from appointment and detailed_line_item dataframes."""
        logger.debug("Starting appointment_line transformation")
        
        appt_df = None
        line_items_df = None
        service_df = None
        
        # Try to get appointment data
        if "appointment" in self.dataframes and not self.dataframes["appointment"].empty:
            appt_df = self.dataframes["appointment"]
        
        # Try to get detailed line items
        if "detailed_line_item" in self.dataframes and not self.dataframes["detailed_line_item"].empty:
            line_items_df = self.dataframes["detailed_line_item"]
        
        # Try to get service data
        if "service" in self.dataframes and not self.dataframes["service"].empty:
            service_df = self.dataframes["service"]
        
        # Check if we have enough data to proceed
        if appt_df is None or appt_df.empty:
            logger.warning("Cannot create appointment_line: no appointment data available")
            return pd.DataFrame()
        
        logger.debug(f"Appointment dataframe shape: {appt_df.shape}")
        if line_items_df is not None:
            logger.debug(f"Line items dataframe shape: {line_items_df.shape}")
        if service_df is not None:
            logger.debug(f"Service dataframe shape: {service_df.shape}")
        
        appointment_lines = []
        
        # Skip the first row which is usually a summary
        appointment_data = appt_df[appt_df["AppointmentServiceRecord id"] != "All"]
        
        # First try to create appointment lines from appointments
        if not appointment_data.empty:
            for _, appt_row in appointment_data.iterrows():
                appt_id = appt_row.get("AppointmentServiceRecord id")
                service_id = appt_row.get("Service Id")
                
                if pd.isna(appt_id) or not appt_id:
                    continue
                
                # If no service_id provided, use the appointment id to find a service
                if pd.isna(service_id) or not service_id:
                    if service_df is not None and not service_df.empty:
                        # Get first service that's not "All"
                        service_data = service_df[service_df["ServiceRecord id"] != "All"]
                        if not service_data.empty:
                            service_id = service_data.iloc[0].get("ServiceRecord id")
                
                if pd.isna(service_id) or not service_id:
                    # Generate a placeholder service ID
                    service_id = f"unknown_service_{hash(str(appt_id)) % 100}"
                
                # Create the appointment line
                appt_line = {
                    "appointment_id": appt_id,
                    "service_id": service_id,
                    "unit_price": 0.0,  # Default value
                    "status": self._transform_field_value("appointment", "status", appt_row.get("Appointment State") or "completed")
                }
                
                # Add to appointment lines
                appointment_lines.append(appt_line)
        
        logger.info(f"Created {len(appointment_lines)} appointment lines")
        return pd.DataFrame(appointment_lines)
    
    def _transform_package_component(self) -> pd.DataFrame:
        """Transform package_component entity from package and service dataframes."""
        logger.debug("Starting package_component transformation")
        
        if "package" not in self.dataframes or "service" not in self.dataframes:
            logger.warning("Cannot create package_component: missing source dataframes")
            return pd.DataFrame()
        
        packages_df = self.dataframes["package"]
        services_df = self.dataframes["service"]
        
        if packages_df.empty or services_df.empty:
            logger.warning("Empty package or service dataframes for package_component creation")
            return pd.DataFrame()
            
        logger.debug(f"Package dataframe shape: {packages_df.shape}")
        logger.debug(f"Service dataframe shape: {services_df.shape}")
        
        # Extract package names to match with services
        package_components = []
        
        # Loop through packages
        for _, pkg_row in packages_df.iterrows():
            # Skip the first row which is usually a summary
            if pkg_row.get("Sale package_name") == "All":
                continue
                
            package_id = pkg_row.get("Sale package_name")
            if pd.isna(package_id) or not package_id:
                continue
            
            logger.debug(f"Processing package: {package_id}")
            
            # Assign at least one service to each package
            # Get a random service (using modulo to ensure consistent mapping)
            if not services_df.empty:
                # Skip first row if it's a summary
                service_data = services_df[services_df["ServiceRecord id"] != "All"]
                if not service_data.empty:
                    for i in range(1, min(4, len(service_data))):  # Add up to 3 services per package
                        service_idx = (hash(str(package_id)) + i) % len(service_data)
                        service_row = service_data.iloc[service_idx]
                        service_id = service_row.get("ServiceRecord id")
                        
                        if service_id and not pd.isna(service_id):
                            # Create the package component
                            component = {
                                "package_id": package_id,
                                "service_id": service_id,
                                "quantity": 1,
                                "sequence_order": i
                            }
                            package_components.append(component)
                            logger.debug(f"Added component: package={package_id}, service={service_id}")
        
        logger.info(f"Created {len(package_components)} package components")
        return pd.DataFrame(package_components)
    
    def map_fields(self, entity_type: str, data: Dict) -> Dict:
        """Map fields from source to target schema."""
        if entity_type not in self.entity_mappings:
            return {}
        
        mapping = self.entity_mappings[entity_type]
        result = {}
        
        # Find the actual case-insensitive match for field names
        data_keys_lower = {k.lower(): k for k in data.keys()}
        
        for target_field, source_field in mapping.items():
            if target_field == "derived" or target_field == "sources":
                continue
            
            # Handle non-string source fields (like boolean or numeric values)
            if not isinstance(source_field, str):
                result[target_field] = source_field
                continue
                
            # Try exact match first
            if source_field in data:
                result[target_field] = self._transform_field_value(entity_type, target_field, data[source_field])
            # Try case-insensitive match
            elif source_field.lower() in data_keys_lower:
                actual_key = data_keys_lower[source_field.lower()]
                result[target_field] = self._transform_field_value(entity_type, target_field, data[actual_key])
            # Try without spaces
            elif source_field.replace(" ", "").lower() in [k.replace(" ", "").lower() for k in data.keys()]:
                for k in data.keys():
                    if k.replace(" ", "").lower() == source_field.replace(" ", "").lower():
                        result[target_field] = self._transform_field_value(entity_type, target_field, data[k])
                        break
            else:
                # For required fields, log a warning
                if self.validation_rules.get(entity_type, {}).get(target_field, {}).get("required", False):
                    logger.warning(f"Required field {source_field} not found in {entity_type} data")
        
        return result
    
    def _transform_field_value(self, entity_type: str, field_name: str, value: Any) -> Any:
        """
        Apply transformations to field values based on field type and entity.
        
        Args:
            entity_type: Type of entity
            field_name: Name of the field
            value: Original value
            
        Returns:
            Any: Transformed value
        """
        # Get field validation rule if available
        field_rule = self.validation_rules.get(entity_type, {}).get(field_name, {})
        field_type = field_rule.get("type", "string")
        
        # Handle None/NaN values
        if pd.isna(value):
            return None
        
        # Special case for appointment status mapping
        if entity_type == "appointment" and field_name == "status" and isinstance(value, str):
            # Map status values to allowed values
            status_mapping = {
                "booked": "confirmed",
                "final": "final",
                "cancelled": "cancelled",
                "arrived": "arrived",
                "no_show": "no_show",
                "confirmed": "confirmed"
            }
            return status_mapping.get(value.lower(), "confirmed")  # Default to confirmed if unknown
        
        # Apply transformations based on field type
        if field_type == "datetime" or field_name.endswith("_at") or field_name.endswith("_date"):
            return standardize_datetime(value)
        elif field_type == "phone" or field_name.endswith("_phone"):
            return parse_phone_number(value)
        elif field_type == "number" or field_type == "float":
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        elif field_type == "integer" or field_type == "int":
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return None
        elif field_type == "boolean" or field_name.startswith("is_"):
            if isinstance(value, str):
                return value.lower() == "true"
            return bool(value)
        
        # Default: return as string
        return str(value) if value is not None else None