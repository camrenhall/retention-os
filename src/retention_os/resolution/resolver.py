"""
Entity resolution for RetentionOS data processing.
"""
from typing import Dict, List, Optional, Set, Tuple
import uuid

import pandas as pd
from loguru import logger


class EntityResolver:
    """
    Resolves and links entities across datasets, creating surrogate keys and relationships.
    """
    
    def __init__(self):
        """Initialize the entity resolver."""
        self.entities = {
            "business": {},
            "client": {},
            "service": {},
            "package": {},
            "package_component": {},
            "appointment": {},
            "appointment_line": {},
            "payment": {},
            "client_package": {}
        }
        self.source_to_canonical = {
            entity_type: {} for entity_type in self.entities.keys()
        }
    
    def resolve_entities(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
        """
        Resolve entities from dataframes, maintaining relationships.
        
        Args:
            dataframes: Dictionary of entity type to DataFrame
            
        Returns:
            Dict[str, Dict]: Dictionary of resolved entities
        """
        # Reset entities and mappings for fresh resolution
        self.entities = {entity_type: {} for entity_type in self.entities.keys()}
        self.source_to_canonical = {entity_type: {} for entity_type in self.entities.keys()}
        
        # Log what's being passed in
        logger.info(f"Resolving entities from {len(dataframes)} dataframes:")
        for entity_type, df in dataframes.items():
            logger.info(f"  - {entity_type}: {len(df)} rows")
        
        # Process entities in dependency order
        self._resolve_business_entities(dataframes.get("business", pd.DataFrame()))
        self._resolve_client_entities(dataframes.get("client", pd.DataFrame()))
        self._resolve_service_entities(dataframes.get("service", pd.DataFrame()))
        self._resolve_package_entities(dataframes.get("package", pd.DataFrame()))
        
        # Derived entities that require dependencies above
        if "package_component" in dataframes and not dataframes["package_component"].empty:
            self._resolve_package_component_entities(dataframes["package_component"])
        else:
            logger.warning("No package_component data available for resolution")
        
        self._resolve_appointment_entities(dataframes.get("appointment", pd.DataFrame()))
        
        # Derived entities that require appointments
        if "appointment_line" in dataframes and not dataframes["appointment_line"].empty:
            self._resolve_appointment_line_entities(dataframes["appointment_line"])
        else:
            logger.warning("No appointment_line data available for resolution")
        
        self._resolve_payment_entities(dataframes.get("payment", pd.DataFrame()))
        
        # Derived entities that require clients and packages
        if "client_package" in dataframes and not dataframes["client_package"].empty:
            self._resolve_client_package_entities(dataframes["client_package"])
        else:
            logger.warning("No client_package data available for resolution")
        
        # Count entities by type
        entity_counts = {entity_type: len(entities) for entity_type, entities in self.entities.items()}
        logger.info(f"Entity resolution complete. Entity counts: {entity_counts}")
        logger.info(f"Total entities resolved: {sum(entity_counts.values())}")
        
        return self.entities
    
    def _generate_id(self) -> str:
        """
        Generate a unique ID.
        
        Returns:
            str: Unique ID
        """
        return str(uuid.uuid4())
    
    def _map_source_to_canonical(self, entity_type: str, source_id: str, canonical_id: str):
        """
        Map a source ID to a canonical ID.
        
        Args:
            entity_type: Entity type
            source_id: Source ID
            canonical_id: Canonical ID
        """
        if source_id:
            self.source_to_canonical[entity_type][source_id] = canonical_id
    
    def _get_canonical_id(self, entity_type: str, source_id: str) -> Optional[str]:
        """
        Get the canonical ID for a source ID.
        
        Args:
            entity_type: Entity type
            source_id: Source ID
            
        Returns:
            Optional[str]: Canonical ID, or None if not found
        """
        return self.source_to_canonical[entity_type].get(source_id)
    
    def _add_entity(self, entity_type: str, entity_data: Dict) -> str:
        """
        Add an entity to the resolved entities.
        
        Args:
            entity_type: Entity type
            entity_data: Entity data
            
        Returns:
            str: Canonical ID
        """
        canonical_id = entity_data.get("id") or self._generate_id()
        entity_data["id"] = canonical_id
        self.entities[entity_type][canonical_id] = entity_data
        
        source_id = entity_data.get("source_id")
        if source_id:
            self._map_source_to_canonical(entity_type, source_id, canonical_id)
            
        # Log the added entity for debugging
        if len(self.entities[entity_type]) % 50 == 0 or len(self.entities[entity_type]) <= 5:
            logger.debug(f"Added {entity_type} entity: ID={canonical_id}, source_id={source_id}")
            
        return canonical_id
    
    def _resolve_business_entities(self, df: pd.DataFrame):
        """Resolve business entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            # Create a default business if none exists
            business_id = self._generate_id()
            business_data = {
                "id": business_id,
                "name": "Default Business",
                "source_id": "default"
            }
            self._add_entity("business", business_data)
            logger.info("Created default business entity")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null source_id
            source_id = row_dict.get("source_id")
            if pd.isna(source_id) or not source_id:
                continue
                
            # Use existing entity if available
            canonical_id = self._get_canonical_id("business", source_id)
            if canonical_id:
                self.entities["business"][canonical_id].update(row_dict)
            else:
                self._add_entity("business", row_dict)
        
        if not self.entities["business"]:
            # Create a default business if none was resolved
            business_id = self._generate_id()
            business_data = {
                "id": business_id,
                "name": "Default Business",
                "source_id": "default"
            }
            self._add_entity("business", business_data)
            logger.info("Created default business entity after processing")
    
    def _resolve_client_entities(self, df: pd.DataFrame):
        """Resolve client entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No client data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null source_id
            source_id = row_dict.get("source_id")
            if pd.isna(source_id) or not source_id:
                continue
                
            # Get business ID (use first one if multiple exist)
            if self.entities["business"]:
                business_id = next(iter(self.entities["business"].keys()))
                row_dict["business_id"] = business_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("client", source_id)
            if canonical_id:
                self.entities["client"][canonical_id].update(row_dict)
            else:
                self._add_entity("client", row_dict)
    
    def _resolve_service_entities(self, df: pd.DataFrame):
        """Resolve service entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No service data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null source_id
            source_id = row_dict.get("source_id")
            if pd.isna(source_id) or not source_id:
                continue
                
            # Get business ID (use first one if multiple exist)
            if self.entities["business"]:
                business_id = next(iter(self.entities["business"].keys()))
                row_dict["business_id"] = business_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("service", source_id)
            if canonical_id:
                self.entities["service"][canonical_id].update(row_dict)
            else:
                self._add_entity("service", row_dict)
    
    def _resolve_package_entities(self, df: pd.DataFrame):
        """Resolve package entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No package data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null source_id
            source_id = row_dict.get("source_id")
            if pd.isna(source_id) or not source_id:
                continue
                
            # Get business ID (use first one if multiple exist)
            if self.entities["business"]:
                business_id = next(iter(self.entities["business"].keys()))
                row_dict["business_id"] = business_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("package", source_id)
            if canonical_id:
                self.entities["package"][canonical_id].update(row_dict)
            else:
                self._add_entity("package", row_dict)
    
    def _resolve_package_component_entities(self, df: pd.DataFrame):
        """Resolve package_component entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No package_component data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null package_id or service_id
            package_id = row_dict.get("package_id")
            service_id = row_dict.get("service_id")
            
            if pd.isna(package_id) or not package_id or pd.isna(service_id) or not service_id:
                continue
                
            # Resolve referenced entities
            canonical_package_id = self._get_canonical_id("package", package_id)
            canonical_service_id = self._get_canonical_id("service", service_id)
            
            if not canonical_package_id or not canonical_service_id:
                logger.warning(f"Skipping package_component: missing canonical IDs for package={package_id} or service={service_id}")
                continue
                
            row_dict["package_id"] = canonical_package_id
            row_dict["service_id"] = canonical_service_id
            
            # Generate a source_id if not present
            source_id = row_dict.get("source_id") or f"{package_id}_{service_id}"
            row_dict["source_id"] = source_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("package_component", source_id)
            if canonical_id:
                self.entities["package_component"][canonical_id].update(row_dict)
            else:
                self._add_entity("package_component", row_dict)
    
    def _resolve_appointment_entities(self, df: pd.DataFrame):
        """Resolve appointment entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No appointment data available for resolution")
            return
            
        # Status mapping for validation
        status_mapping = {
            "booked": "confirmed",
            "final": "final",
            "cancelled": "cancelled", 
            "arrived": "arrived",
            "no_show": "no_show",
            "confirmed": "confirmed"
        }
                
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null source_id
            source_id = row_dict.get("source_id") or row_dict.get("appointment_id")
            if pd.isna(source_id) or not source_id:
                continue
                
            # Resolve referenced entities
            client_id = row_dict.get("client_id")
            canonical_client_id = self._get_canonical_id("client", client_id) if client_id else None
            
            # Get business ID (use first one if multiple exist)
            if self.entities["business"]:
                business_id = next(iter(self.entities["business"].keys()))
                row_dict["business_id"] = business_id
            
            if canonical_client_id:
                row_dict["client_id"] = canonical_client_id
            
            # Map appointment status to valid values
            if "status" in row_dict and row_dict["status"]:
                status_val = str(row_dict["status"]).lower()
                row_dict["status"] = status_mapping.get(status_val, "confirmed")
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("appointment", source_id)
            if canonical_id:
                self.entities["appointment"][canonical_id].update(row_dict)
            else:
                self._add_entity("appointment", row_dict)
    
    def _resolve_appointment_line_entities(self, df: pd.DataFrame):
        """Resolve appointment_line entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No appointment_line data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null appointment_id or service_id
            appointment_id = row_dict.get("appointment_id")
            service_id = row_dict.get("service_id")
            
            if pd.isna(appointment_id) or not appointment_id or pd.isna(service_id) or not service_id:
                continue
                
            # Resolve referenced entities
            canonical_appointment_id = self._get_canonical_id("appointment", appointment_id)
            canonical_service_id = self._get_canonical_id("service", service_id)
            
            if not canonical_appointment_id or not canonical_service_id:
                logger.warning(f"Skipping appointment_line: missing canonical IDs for appointment={appointment_id} or service={service_id}")
                continue
                
            row_dict["appointment_id"] = canonical_appointment_id
            row_dict["service_id"] = canonical_service_id
            
            # Generate a source_id if not present
            source_id = row_dict.get("source_id") or f"{appointment_id}_{service_id}"
            row_dict["source_id"] = source_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("appointment_line", source_id)
            if canonical_id:
                self.entities["appointment_line"][canonical_id].update(row_dict)
            else:
                self._add_entity("appointment_line", row_dict)
    
    def _resolve_payment_entities(self, df: pd.DataFrame):
        """Resolve payment entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No payment data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null source_id
            source_id = row_dict.get("source_payment_id") or row_dict.get("source_id")
            if pd.isna(source_id) or not source_id:
                continue
                
            # Resolve client ID if present
            client_name = row_dict.get("client_id") or row_dict.get("client_name")
            if client_name and not pd.isna(client_name):
                # Find client by name (simplified approach)
                for client_id, client_data in self.entities["client"].items():
                    if client_data.get("full_name", "") == client_name or client_data.get("name", "") == client_name:
                        row_dict["client_id"] = client_id
                        break
            
            # Get business ID (use first one if multiple exist)
            if self.entities["business"]:
                business_id = next(iter(self.entities["business"].keys()))
                row_dict["business_id"] = business_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("payment", source_id)
            if canonical_id:
                self.entities["payment"][canonical_id].update(row_dict)
            else:
                self._add_entity("payment", row_dict)
    
    def _resolve_client_package_entities(self, df: pd.DataFrame):
        """Resolve client_package entities."""
        if df is not None and not df.empty:
            logger.debug(f"Dataframe passed to resolver: {df.shape}")
            logger.debug(f"Dataframe columns: {list(df.columns)}")
            logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else None}")
            
        if df.empty:
            logger.warning("No client_package data available for resolution")
            return
            
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Skip entries with null client_id or package_id
            client_id = row_dict.get("client_id")
            package_id = row_dict.get("package_id")
            
            if pd.isna(client_id) or not client_id or pd.isna(package_id) or not package_id:
                continue
                
            # Resolve referenced entities
            canonical_client_id = self._get_canonical_id("client", client_id)
            canonical_package_id = self._get_canonical_id("package", package_id)
            
            if not canonical_client_id or not canonical_package_id:
                logger.warning(f"Skipping client_package: missing canonical IDs for client={client_id} or package={package_id}")
                continue
                
            row_dict["client_id"] = canonical_client_id
            row_dict["package_id"] = canonical_package_id
            
            # Generate a source_id if not present
            source_id = row_dict.get("source_id") or f"{client_id}_{package_id}"
            row_dict["source_id"] = source_id
            
            # Use existing entity if available
            canonical_id = self._get_canonical_id("client_package", source_id)
            if canonical_id:
                self.entities["client_package"][canonical_id].update(row_dict)
            else:
                self._add_entity("client_package", row_dict)
    
    def _safe_float(self, value):
        """Convert value to float safely."""
        if pd.isna(value):
            return 0.0
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0