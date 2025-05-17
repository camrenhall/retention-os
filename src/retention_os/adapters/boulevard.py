import uuid
import logging
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from ..adapters.base import CRMAdapter
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class BoulevardAdapter(CRMAdapter):
    """
    Boulevard-specific implementation of the CRM adapter.
    """
    
    def __init__(self, config_path: str):
        """Initialize the Boulevard adapter."""
        super().__init__(config_path)
        self.source_system = "Boulevard"
    
    def get_source_files(self) -> Dict[str, str]:
        """Get the mapping of entity types to source file paths."""
        source_files = {}
        for entity_type, file_pattern in self.file_mappings.items():
            file_path = self._find_file(file_pattern)
            if file_path:
                source_files[entity_type] = file_path
        return source_files
    
    def transform_to_canonical(self) -> Dict[str, List[dict]]:
        """Transform Boulevard data to canonical model entities."""
        # Ensure all source files are loaded
        if not self._loaded_dataframes:
            self.load_source_files()
        
        # Apply standardization to all loaded dataframes
        standardized_dfs = {}
        for entity_type, df in self._loaded_dataframes.items():
            standardized_dfs[entity_type] = self.standardize_schema(entity_type, df)
        
        # Transform each entity type to canonical model
        canonical_data = {}
        
        # Process in order of dependencies
        processing_order = [
            'clinic', 'provider', 'service', 'package', 'packageComponent',
            'patient', 'appointment', 'appointmentLine', 'payment',
            'patientPackage', 'retailSale', 'retailSaleLine', 'outreachMessage'
        ]
        
        # Initialize entity containers
        for entity_type in processing_order:
            canonical_data[entity_type] = []
        
        # Transform each entity type
        for entity_type in processing_order:
            transform_method = getattr(self, f"_transform_{entity_type}", None)
            if transform_method:
                try:
                    # Check if we have data for this entity type
                    if entity_type in standardized_dfs and not standardized_dfs[entity_type].empty:
                        canonical_data[entity_type] = transform_method(
                            standardized_dfs[entity_type], 
                            canonical_data
                        )
                        logger.info(f"Transformed {len(canonical_data[entity_type])} {entity_type} entities")
                    else:
                        # Create empty list for this entity type
                        logger.warning(f"No data available for {entity_type}, creating empty list")
                        canonical_data[entity_type] = []
                except Exception as e:
                    logger.error(f"Error transforming {entity_type}: {e}")
                    # Create empty list for this entity type
                    canonical_data[entity_type] = []
            else:
                # If no transform method exists, attempt a generic transform or skip
                if entity_type in standardized_dfs and not standardized_dfs[entity_type].empty:
                    logger.warning(f"No specific transform method for {entity_type}, using generic transform")
                    canonical_data[entity_type] = self._generic_transform(
                        entity_type, 
                        standardized_dfs[entity_type]
                    )
                else:
                    logger.warning(f"No data available for {entity_type}")
                    canonical_data[entity_type] = []
        
        return canonical_data
    
    def _generic_transform(self, entity_type: str, df: pd.DataFrame) -> List[dict]:
        """Generic transformation for entities without specific transform methods."""
        result = []
        for _, row in df.iterrows():
            entity = {
                f"{entity_type}_id": str(uuid.uuid4()),
                "source_system": self.source_system,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            # Add all non-null fields from the DataFrame
            for column in df.columns:
                if pd.notna(row[column]):
                    entity[column] = row[column]
            
            result.append(entity)
        
        return result
    
    # Specific transformation methods for each entity type
    
    def _transform_clinic(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform clinic data to canonical model."""
        clinics = []
        
        for _, row in df.iterrows():
            clinic = {
                "clinic_id": str(uuid.uuid4()),
                "source_clinic_id": str(row.get("id", "")),
                "source_system": self.source_system,
                "name": row.get("name", ""),
                "timezone": "America/Chicago",  # Default, can be enhanced
                "address": {
                    "street": "",
                    "city": row.get("city", ""),
                    "state": row.get("state", ""),
                    "postal_code": "",
                    "country": "US"  # Default
                },
                "contact_info": {
                    "phone": "",
                    "email": ""
                },
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            clinics.append(clinic)
        
        return clinics
    
    def _transform_patient(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform patient data to canonical model."""
        patients = []
        
        # Get clinic IDs for reference
        clinic_ids = {}
        for clinic in canonical_data.get('clinic', []):
            clinic_ids[clinic['source_clinic_id']] = clinic['clinic_id']
        
        default_clinic_id = next(iter(clinic_ids.values())) if clinic_ids else str(uuid.uuid4())
        
        for _, row in df.iterrows():
            # Extract data
            source_id = str(row.get("source_id", ""))
            
            # Handle phone number - ensure it's a string
            mobile_phone = row.get("mobile_phone", "")
            if pd.notna(mobile_phone):
                if isinstance(mobile_phone, (int, float)):
                    # Convert numerical phone to string
                    mobile_phone = str(int(mobile_phone))
                    # Format if it looks like a 10-digit number
                    if len(mobile_phone) == 10:
                        mobile_phone = "+1" + mobile_phone
                else:
                    mobile_phone = str(mobile_phone)
            else:
                mobile_phone = ""
            
            # Handle marketing preferences safely
            email_marketing = row.get("email_marketing_enabled", "")
            if isinstance(email_marketing, str):
                email_opt_in = email_marketing.lower() == "true"
            else:
                email_opt_in = False
                
            sms_marketing = row.get("sms_marketing_enabled", "")
            if isinstance(sms_marketing, str):
                sms_opt_in = sms_marketing.lower() == "true"
            else:
                sms_opt_in = False
            
            patient = {
                "patient_id": str(uuid.uuid4()),
                "source_patient_id": source_id,
                "source_system": self.source_system,
                "clinic_id": default_clinic_id,  # Link to clinic
                "first_name": str(row.get("first_name", "")),
                "last_name": str(row.get("last_name", "")),
                "date_of_birth": row.get("date_of_birth", None),
                "gender": "Unknown",  # Default
                "email": str(row.get("email", "")),
                "mobile_phone": mobile_phone,
                "address": {
                    "street": str(row.get("address_line_one", "")),
                    "city": str(row.get("address_city", "")),
                    "state": str(row.get("address_state", "")),
                    "postal_code": str(row.get("address_zip", "")),
                    "country": "US"  # Default
                },
                "communication_preferences": {
                    "email_opt_in": email_opt_in,
                    "sms_opt_in": sms_opt_in,
                    "marketing_opt_in": email_opt_in or sms_opt_in
                },
                "created_at": row.get("created_at", datetime.now().isoformat()),
                "updated_at": datetime.now().isoformat()
            }
            patients.append(patient)
        
        return patients
    
    def _transform_provider(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform provider data to canonical model."""
        providers = []
        
        # Get clinic IDs for reference
        clinic_ids = {}
        for clinic in canonical_data.get('clinic', []):
            clinic_ids[clinic['source_clinic_id']] = clinic['clinic_id']
        
        default_clinic_id = next(iter(clinic_ids.values())) if clinic_ids else str(uuid.uuid4())
        
        for _, row in df.iterrows():
            # Create provider entity - ensure all values are simple types
            provider = {
                "provider_id": str(uuid.uuid4()),
                "source_provider_id": str(row.get("id", "")),
                "source_system": self.source_system,
                "clinic_id": default_clinic_id,
                "name": f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                "first_name": str(row.get('first_name', '')),
                "last_name": str(row.get('last_name', '')),
                "email": str(row.get('email_address', '')),
                "specialties_str": "[]",  # String representation of empty list
                "active": True,  # Default to active
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            providers.append(provider)
        
        return providers
    
    def _transform_service(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform service data to canonical model."""
        services = []
        
        # Get clinic IDs for reference
        clinic_ids = {}
        for clinic in canonical_data.get('clinic', []):
            clinic_ids[clinic['source_clinic_id']] = clinic['clinic_id']
        
        default_clinic_id = next(iter(clinic_ids.values())) if clinic_ids else str(uuid.uuid4())
        
        for _, row in df.iterrows():
            # Handle base_price safely - convert None to 0
            base_price = row.get("base_price", 0)
            if base_price is None:
                base_price = 0.0
            else:
                try:
                    base_price = float(base_price)
                except (ValueError, TypeError):
                    base_price = 0.0
            
            service = {
                "service_id": str(uuid.uuid4()),
                "source_service_id": str(row.get("id", "")),
                "source_system": self.source_system,
                "clinic_id": default_clinic_id,
                "name": str(row.get("name", "")),
                "category": str(row.get("category_name", "")),
                "description": "",  # Default
                "duration_minutes": 60,  # Default
                "base_price": base_price,
                "active": str(row.get("is_active", "")).lower() == "true",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            services.append(service)
        
        return services
    
    def _transform_package(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform package data to canonical model."""
        packages = []
        
        # Get clinic IDs for reference
        clinic_ids = {}
        for clinic in canonical_data.get('clinic', []):
            clinic_ids[clinic['source_clinic_id']] = clinic['clinic_id']
        
        default_clinic_id = next(iter(clinic_ids.values())) if clinic_ids else str(uuid.uuid4())
        
        for _, row in df.iterrows():
            package = {
                "package_id": str(uuid.uuid4()),
                "source_package_id": str(row.get("id", "")),
                "source_system": self.source_system,
                "clinic_id": default_clinic_id,
                "name": row.get("package_name", ""),
                "description": "",  # Default
                "list_price": float(row.get("gross_package_sales", 0)),
                "validity_days": 365,  # Default to 1 year
                "active": True,  # Default
                "is_synthetic": False,  # Not created by the system
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            packages.append(package)
        
        return packages
    
    def _transform_packageComponent(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform package component data to canonical model."""
        # Create synthetic package components based on packages and services
        components = []
        
        packages = canonical_data.get('package', [])
        services = canonical_data.get('service', [])
        
        if not packages or not services:
            logger.warning("Can't create package components: missing packages or services")
            return []
        
        # Use fixed seed for reproducibility
        random.seed(42)
        
        # Create 2-3 components for each package
        for package in packages:
            package_id = package['package_id']
            
            # Randomly select 2-3 services (or fewer if not enough services)
            num_services = min(len(services), random.randint(2, 3))
            selected_services = random.sample(services, num_services)
            
            # Create components
            for i, service in enumerate(selected_services):
                component = {
                    "package_component_id": str(uuid.uuid4()),
                    "package_id": package_id,
                    "service_id": service['service_id'],
                    "quantity": random.randint(1, 3),  # Random quantity between 1-3
                    "sequence_order": i + 1,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                components.append(component)
        
        logger.info(f"Created {len(components)} synthetic package components")
        return components
    
    def _transform_appointment(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform appointment data to canonical model."""
        appointments = []
        
        # Log the actual columns for debugging
        logger.info(f"Appointment DataFrame shape: {df.shape}")
        logger.info(f"Appointment DataFrame columns: {', '.join(df.columns.tolist())}")
        
        # Get mapping dictionaries
        patient_map = {}
        for patient in canonical_data.get('patient', []):
            if 'source_patient_id' in patient and patient['source_patient_id']:
                patient_map[patient['source_patient_id']] = patient['patient_id']
        
        provider_map = {}
        for provider in canonical_data.get('provider', []):
            if 'source_provider_id' in provider and provider['source_provider_id']:
                provider_map[provider['source_provider_id']] = provider['provider_id']
        
        clinic_map = {}
        for clinic in canonical_data.get('clinic', []):
            if 'source_clinic_id' in clinic and clinic['source_clinic_id']:
                clinic_map[clinic['source_clinic_id']] = clinic['clinic_id']
        
        # Get default clinic ID
        default_clinic_id = next(iter(clinic_map.values())) if clinic_map else str(uuid.uuid4())
        
        # Log the column mappings from the field mappings
        logger.info(f"Appointment field mappings: {self.field_mappings.get('appointment', {})}")
        
        # Look for the real column names without relying on the field mappings
        column_map = {}
        for target_field, source_field in self.field_mappings.get('appointment', {}).items():
            # Check if the source field exists in the dataframe
            if source_field in df.columns:
                column_map[target_field] = source_field
            else:
                # Try to find a close match
                for col in df.columns:
                    if source_field.lower() in col.lower() or (
                        target_field.lower() in col.lower()):
                        column_map[target_field] = col
                        logger.info(f"Mapping {target_field} to {col} (instead of {source_field})")
                        break
        
        # Ensure we have mappings for critical fields
        for critical_field in ['client_id', 'staff_id', 'location_id', 'start_at']:
            if critical_field not in column_map:
                logger.error(f"Could not find a column for {critical_field} in appointment data")
                # Try a more aggressive approach to find a match
                for col in df.columns:
                    if critical_field.replace('_', '').lower() in col.replace(' ', '').lower():
                        column_map[critical_field] = col
                        logger.info(f"Using fuzzy match: {critical_field} -> {col}")
                        break
        
        for _, row in df.iterrows():
            try:
                # Extract IDs and fields using the mapped column names
                client_id = ""
                if 'client_id' in column_map:
                    client_id = str(row.get(column_map['client_id'], ""))
                
                staff_id = ""
                if 'staff_id' in column_map:
                    staff_id = str(row.get(column_map['staff_id'], ""))
                
                location_id = ""
                if 'location_id' in column_map:
                    location_id = str(row.get(column_map['location_id'], ""))
                
                # Map to canonical IDs
                patient_id = patient_map.get(client_id)
                if not patient_id:
                    patient_id = str(uuid.uuid4())
                
                provider_id = provider_map.get(staff_id)
                if not provider_id:
                    provider_id = str(uuid.uuid4())
                
                clinic_id = clinic_map.get(location_id, default_clinic_id)
                
                # Parse dates with mapped column names
                start_at = None
                if 'start_at' in column_map:
                    start_at = row.get(column_map['start_at'])
                
                if pd.isna(start_at):
                    start_at = datetime.now().isoformat()
                elif isinstance(start_at, str):
                    start_at = start_at
                else:
                    try:
                        start_at = pd.to_datetime(start_at).isoformat()
                    except:
                        start_at = datetime.now().isoformat()
                
                # Determine status with mapped column names
                status = "completed"  # Default
                
                cancellation_reason = ""
                if 'cancellation_reason' in column_map:
                    cancellation_reason = row.get(column_map['cancellation_reason'], "")
                    if pd.notna(cancellation_reason) and cancellation_reason:
                        status = "cancelled"
                
                appointment_state = ""
                if 'appointment_state' in column_map:
                    appointment_state = row.get(column_map['appointment_state'], "")
                    if pd.notna(appointment_state) and isinstance(appointment_state, str) and appointment_state.lower() == "no_show":
                        status = "no-show"
                
                # Create appointment entity
                appointment = {
                    "appointment_id": str(uuid.uuid4()),
                    "source_appointment_id": str(row.get("id", "") or row.get("appointment_id", "") or ""),
                    "source_system": self.source_system,
                    "patient_id": patient_id,
                    "clinic_id": clinic_id,
                    "provider_id": provider_id,
                    "scheduled_at": start_at,
                    "status": status,
                    "cancellation_reason": str(cancellation_reason) if pd.notna(cancellation_reason) else "",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                appointments.append(appointment)
            except Exception as e:
                logger.error(f"Error transforming appointment: {str(e)}")
                # Log the row data for debugging
                logger.debug(f"Row data: {row}")
                # Continue to next appointment instead of failing completely
                continue
        
        return appointments
    
    def _transform_appointmentLine(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform appointment line data to canonical model."""
        # Create synthetic appointment lines based on appointments and services
        lines = []
        
        appointments = canonical_data.get('appointment', [])
        services = canonical_data.get('service', [])
        
        if not appointments or not services:
            logger.warning("Can't create appointment lines: missing appointments or services")
            return []
        
        # Use fixed seed for reproducibility
        random.seed(43)  # Different seed from package components
        
        # Create 1-2 service lines for each appointment
        for appointment in appointments:
            appointment_id = appointment['appointment_id']
            
            # Randomly select 1-2 services
            num_services = min(len(services), random.randint(1, 2))
            selected_services = random.sample(services, num_services)
            
            # Create lines
            for service in selected_services:
                line = {
                    "appointment_line_id": str(uuid.uuid4()),
                    "appointment_id": appointment_id,
                    "service_id": service['service_id'],
                    "package_id": None,  # Most appointments aren't from packages
                    "unit_price": float(service.get('base_price', 0.0)),
                    "discount_amount": 0.0,
                    "discount_reason": "",
                    "tax_amount": 0.0,
                    "quantity": 1,
                    "notes": "",
                    "created_at": appointment.get('created_at', datetime.now().isoformat()),
                    "updated_at": appointment.get('updated_at', datetime.now().isoformat())
                }
                lines.append(line)
        
        logger.info(f"Created {len(lines)} synthetic appointment lines")
        return lines
        
    def _transform_payment(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform payment data to canonical model."""
        payments = []
        
        # Get patient mappings
        patient_map = {}
        for patient in canonical_data.get('patient', []):
            name = f"{patient['first_name']} {patient['last_name']}".strip().lower()
            patient_map[name] = patient['patient_id']
        
        for _, row in df.iterrows():
            # Safe handling of client_name
            client_name = row.get("client_name", "")
            if isinstance(client_name, str):
                client_name = client_name.lower()
                patient_id = patient_map.get(client_name, str(uuid.uuid4()))
            else:
                patient_id = str(uuid.uuid4())
            
            # Parse payment methods safely
            payment_method = row.get("payment_method", "")
            method = "card"  # Default
            
            if isinstance(payment_method, str):
                payment_method_lower = payment_method.lower()
                if "cash" in payment_method_lower:
                    method = "cash"
                elif "insurance" in payment_method_lower:
                    method = "insurance"
                elif "financing" in payment_method_lower:
                    method = "financing"
            
            # Handle amount safely
            amount = row.get("transaction_amount", 0)
            if amount is None or pd.isna(amount):
                amount = 0.0
            else:
                try:
                    amount = float(amount)
                except (ValueError, TypeError):
                    amount = 0.0
            
            # Handle dates safely
            created_on = row.get("created_on")
            if pd.isna(created_on):
                paid_at = datetime.now().isoformat()
            else:
                try:
                    paid_at = pd.to_datetime(created_on).isoformat()
                except:
                    paid_at = datetime.now().isoformat()
            
            payment = {
                "payment_id": str(uuid.uuid4()),
                "source_payment_id": str(row.get("transaction_id", "")),
                "source_system": self.source_system,
                "patient_id": patient_id,
                "appointment_id": None,  # We don't have this relation in the data
                "amount": amount,
                "method": method,
                "paid_at": paid_at,
                "status": "completed",  # Default
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            payments.append(payment)
        
        return payments
    
    def _transform_patientPackage(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform patient package data to canonical model."""
        # Create synthetic patient packages
        patient_packages = []
        
        patients = canonical_data.get('patient', [])
        packages = canonical_data.get('package', [])
        
        if not patients or not packages:
            logger.warning("Can't create patient packages: missing patients or packages")
            return []
        
        # Use fixed seed for reproducibility
        random.seed(44)  # Different seed
        
        # Select ~10% of patients randomly to have packages
        num_patients_with_packages = max(1, int(len(patients) * 0.1))
        patients_with_packages = random.sample(patients, num_patients_with_packages)
        
        # Create 1-2 packages for each selected patient
        for patient in patients_with_packages:
            patient_id = patient['patient_id']
            
            # Randomly select 1-2 packages
            num_packages = min(len(packages), random.randint(1, 2))
            selected_packages = random.sample(packages, num_packages)
            
            # Create patient packages
            for package in selected_packages:
                # Random purchase date in the past year
                days_ago = random.randint(30, 365)
                purchase_date = datetime.now() - timedelta(days=days_ago)
                
                # Expiration date 1 year from purchase
                expiration_date = purchase_date + timedelta(days=365)
                
                # Random services remaining (0-3)
                services_remaining = random.randint(0, 3)
                
                # Status based on expiration date and services remaining
                if services_remaining == 0:
                    status = "fully_redeemed"
                elif expiration_date < datetime.now():
                    status = "expired"
                else:
                    status = "active"
                
                patient_package = {
                    "patient_package_id": str(uuid.uuid4()),
                    "source_patient_package_id": "",  # No source ID
                    "source_system": self.source_system,
                    "patient_id": patient_id,
                    "package_id": package['package_id'],
                    "purchase_date": purchase_date.isoformat(),
                    "expiration_date": expiration_date.isoformat(),
                    "price_paid": float(package.get('list_price', 0.0)),
                    "discount_amount": 0.0,
                    "status": status,
                    "services_remaining": services_remaining,
                    "created_at": purchase_date.isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                patient_packages.append(patient_package)
        
        logger.info(f"Created {len(patient_packages)} synthetic patient packages")
        return patient_packages
    
    def _transform_outreachMessage(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform outreach message data to canonical model."""
        outreach_messages = []
        
        # Get mappings
        clinic_map = {c['source_clinic_id']: c['clinic_id'] 
                     for c in canonical_data.get('clinic', [])}
        
        default_clinic_id = next(iter(clinic_map.values())) if clinic_map else str(uuid.uuid4())
        
        for _, row in df.iterrows():
            # Parse campaign data
            campaign_type = row.get("campaign_type_name", "")
            location_name = row.get("location_name", "")
            
            # For each campaign, create sample outreach messages for patients
            for patient in canonical_data.get('patient', [])[:5]:  # Limit to first 5 patients for demo
                sent_at = (datetime.now() - timedelta(days=30)).isoformat()
                
                message = {
                    "message_id": str(uuid.uuid4()),
                    "patient_id": patient['patient_id'],
                    "clinic_id": default_clinic_id,
                    "sent_at": sent_at,
                    "channel": "email",  # Default
                    "template_id": f"template_{campaign_type.lower().replace(' ', '_')}",
                    "package_id": None,  # Not linked to specific package
                    "discount_percent": 0.0,  # Default
                    "experiment_group": "control",  # Default
                    "response_status": "ignored",  # Default
                    "booking_id": None,  # Not linked to booking
                    "opened_at": None,
                    "clicked_at": None,
                    "campaign_id": f"campaign_{campaign_type.lower().replace(' ', '_')}",
                    "created_at": sent_at,
                    "updated_at": datetime.now().isoformat()
                }
                outreach_messages.append(message)
        
        return outreach_messages
    
    def _transform_retailSale(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform retail sale data to canonical model."""
        retail_sales = []
        
        # Try to match product sales to patients
        if 'product' not in df.columns:
            # If we don't have specific retail sale data, return empty list
            return retail_sales
        
        # Get clinic and patient mappings
        clinic_map = {c['source_clinic_id']: c['clinic_id'] 
                     for c in canonical_data.get('clinic', [])}
        patient_map = {}
        for patient in canonical_data.get('patient', []):
            name = f"{patient['first_name']} {patient['last_name']}".strip().lower()
            patient_map[name] = patient['patient_id']
        
        default_clinic_id = next(iter(clinic_map.values())) if clinic_map else str(uuid.uuid4())
        
        for _, row in df.iterrows():
            client_name = str(row.get("client_name", "")).lower()
            patient_id = patient_map.get(client_name, str(uuid.uuid4()))
            
            retail_sale = {
                "retail_sale_id": str(uuid.uuid4()),
                "source_retail_sale_id": str(row.get("sale_id", "")),
                "source_system": self.source_system,
                "patient_id": patient_id,
                "clinic_id": default_clinic_id,
                "sale_date": row.get("sale_date", datetime.now().isoformat()),
                "total_amount": float(row.get("net_retail_product_sales", 0)),
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            retail_sales.append(retail_sale)
        
        return retail_sales
    
    def _transform_retailSaleLine(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform retail sale line data to canonical model."""
        retail_sale_lines = []
        
        # Get retail sale IDs
        retail_sale_map = {rs['source_retail_sale_id']: rs['retail_sale_id'] 
                          for rs in canonical_data.get('retailSale', [])}
        
        # If we don't have any retail sales or appropriate data, return empty list
        if not retail_sale_map or 'product' not in df.columns:
            return retail_sale_lines
        
        for sale_id, retail_sale_id in retail_sale_map.items():
            # Filter the DataFrame for this sale ID
            sale_df = df[df['sale_id'] == sale_id] if 'sale_id' in df.columns else pd.DataFrame()
            
            if not sale_df.empty:
                for _, row in sale_df.iterrows():
                    retail_sale_line = {
                        "retail_sale_line_id": str(uuid.uuid4()),
                        "retail_sale_id": retail_sale_id,
                        "product_id": str(uuid.uuid4()),  # Generate a unique ID 
                        "product_name": row.get("retail_product_name", ""),
                        "product_category": row.get("product_brand_name", ""),
                        "quantity": int(row.get("quantity", 1)),
                        "unit_price": float(row.get("unit_price", 0)),
                        "discount_amount": float(row.get("discount_amount", 0)),
                        "tax_amount": float(row.get("sales_tax", 0)),
                        "created_at": datetime.now().isoformat(),
                        "updated_at": datetime.now().isoformat()
                    }
                    retail_sale_lines.append(retail_sale_line)
        
        return retail_sale_lines
    
    def _transform_voucherRedemption(self, df: pd.DataFrame, canonical_data: Dict) -> List[dict]:
        """Transform voucher redemption data."""
        vouchers = []
        
        # Get patient mappings
        patient_map = {p['source_patient_id']: p['patient_id'] 
                    for p in canonical_data.get('patient', [])}
        
        for _, row in df.iterrows():
            client_id = str(row.get("client_id", ""))
            patient_id = patient_map.get(client_id, str(uuid.uuid4()))
            
            voucher = {
                "voucher_id": str(uuid.uuid4()),
                "source_voucher_id": str(row.get("voucher_id", "")),
                "source_system": self.source_system,
                "patient_id": patient_id,
                "redemption_date": row.get("redemption_date", datetime.now().isoformat()),
                "original_value": float(row.get("original_value", 0.0)),
                "redeemed_value": float(row.get("redeemed_value", 0.0)),
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            vouchers.append(voucher)
        
        return vouchers