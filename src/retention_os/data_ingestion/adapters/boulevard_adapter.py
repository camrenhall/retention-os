# src/retention_os/data_ingestion/adapters/boulevard_adapter.py
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from datetime import datetime

from ..base_adapter import BaseCRMAdapter

class BoulevardAdapter(BaseCRMAdapter):
    """
    Adapter for Boulevard CRM data.
    """
    
    def __init__(self, config: Dict[str, Any], input_dir: str, **kwargs):
        super().__init__(config, input_dir, **kwargs)
        self.source_system = "Boulevard"
    
    # src/retention_os/data_ingestion/adapters/boulevard_adapter.py

    def standardize_schema(self) -> Dict[str, pd.DataFrame]:
        """
        Transform Boulevard source dataframes into standardized canonical format.
        
        Returns:
            Dictionary of standardized dataframes, keyed by entity name
        """
        self.logger.info("Standardizing schema for Boulevard data")
        self.logger.info(f"Available dataframes: {list(self.dataframes.keys())}")
        
        standardized_data = {}
        
        # Process clinic first as it's required for foreign keys
        if 'clinic' in self.dataframes:
            self.logger.info("Processing clinic data")
            standardized_data['clinic'] = self._process_clinic()
            if len(standardized_data['clinic']) > 0:
                self.logger.info(f"Processed {len(standardized_data['clinic'])} clinic records")
            else:
                self.logger.warning("No clinic records processed, this will affect foreign key relationships")
        else:
            self.logger.warning("No clinic data found")
        
        # Process provider
        if 'provider' in self.dataframes:
            self.logger.info("Processing provider data")
            standardized_data['provider'] = self._process_provider(standardized_data.get('clinic'))
            self.logger.info(f"Processed {len(standardized_data['provider'])} provider records")
        
        # Process service
        if 'service' in self.dataframes:
            self.logger.info("Processing service data")
            standardized_data['service'] = self._process_service(standardized_data.get('clinic'))
            self.logger.info(f"Processed {len(standardized_data['service'])} service records")
        
        # Process package
        if 'package' in self.dataframes:
            self.logger.info("Processing package data")
            standardized_data['package'] = self._process_package(standardized_data.get('clinic'))
            self.logger.info(f"Processed {len(standardized_data['package'])} package records")
        
        # Process patient
        if 'patient' in self.dataframes:
            self.logger.info("Processing patient data")
            standardized_data['patient'] = self._process_patient(standardized_data.get('clinic'))
            self.logger.info(f"Processed {len(standardized_data['patient'])} patient records")
        
        # Process appointment
        if 'appointment' in self.dataframes:
            self.logger.info("Processing appointment data")
            standardized_data['appointment'] = self._process_appointment(
                standardized_data.get('patient'),
                standardized_data.get('clinic'),
                standardized_data.get('provider')
            )
            self.logger.info(f"Processed {len(standardized_data['appointment'])} appointment records")
        
        # Process payment
        if 'payment' in self.dataframes:
            self.logger.info("Processing payment data")
            standardized_data['payment'] = self._process_payment(
                standardized_data.get('patient'),
                standardized_data.get('appointment')
            )
            self.logger.info(f"Processed {len(standardized_data['payment'])} payment records")
        
        # Process outreach message
        if 'marketing' in self.dataframes:
            self.logger.info("Processing outreach message data")
            standardized_data['outreach_message'] = self._process_outreach_message(
                standardized_data.get('patient'),
                standardized_data.get('clinic')
            )
            self.logger.info(f"Processed {len(standardized_data['outreach_message'])} outreach message records")
        
        # Log summary of processed entities
        self.logger.info(f"Standardized data contains {len(standardized_data)} entities: {list(standardized_data.keys())}")
        
        return standardized_data
    
    def _process_clinic(self) -> pd.DataFrame:
        """
        Process clinic data from Boulevard.
        
        Returns:
            Standardized clinic dataframe
        """
        df = self.dataframes['clinic']
        mapped_df = self.apply_field_mapping(df, 'clinic')
        mapped_df = self.apply_type_conversions(mapped_df, 'clinic')
        
        # Add additional fields required by canonical model
        if 'source_clinic_id' not in mapped_df.columns and 'source_id' in mapped_df.columns:
            mapped_df['source_clinic_id'] = mapped_df['source_id']
            
        mapped_df['source_system'] = self.source_system
        
        if 'timezone' not in mapped_df.columns:
            mapped_df['timezone'] = 'America/Chicago'  # Default timezone, adjust as needed
            
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'clinic_id', 'C')
        
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        # Structure address as a dictionary inside a JSON field (PostgreSQL jsonb)
        address_fields = ['street', 'city', 'state', 'postal_code', 'country']
        for field in address_fields:
            if field not in mapped_df.columns:
                mapped_df[field] = None
                
        # Ensure all required fields are present
        for field in ['name', 'source_clinic_id', 'source_system', 'timezone', 'created_at', 'updated_at']:
            if field not in mapped_df.columns:
                if field == 'name' and 'location_name' in mapped_df.columns:
                    mapped_df['name'] = mapped_df['location_name']
                else:
                    mapped_df[field] = None
        
        return mapped_df
    
    def _process_patient(self, clinic_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process patient data from Boulevard.
        
        Args:
            clinic_df: Standardized clinic dataframe for foreign key
            
        Returns:
            Standardized patient dataframe
        """
        df = self.dataframes['patient']
        mapped_df = self.apply_field_mapping(df, 'patient')
        mapped_df = self.apply_type_conversions(mapped_df, 'patient')
        
        # Add foreign key to clinic
        if clinic_df is not None and len(clinic_df) > 0:
            mapped_df['clinic_id'] = clinic_df.iloc[0]['clinic_id']
        else:
            mapped_df['clinic_id'] = None
            
        # Add source system
        mapped_df['source_system'] = self.source_system
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'patient_id', 'P')
        
        # Handle address fields
        address_fields = ['street', 'city', 'state', 'postal_code', 'country']
        for field in address_fields:
            field_name = f'address_{field}'
            boulevard_field = None
            
            # Map Boulevard-specific address fields to canonical model
            if field == 'street' and 'address_line_one' in mapped_df.columns:
                boulevard_field = 'address_line_one'
            elif field == 'city' and 'address_city' in mapped_df.columns:
                boulevard_field = 'address_city'
            elif field == 'state' and 'address_state' in mapped_df.columns:
                boulevard_field = 'address_state'
            elif field == 'postal_code' and 'address_zip' in mapped_df.columns:
                boulevard_field = 'address_zip'
                
            if boulevard_field and boulevard_field in mapped_df.columns:
                mapped_df[field_name] = mapped_df[boulevard_field]
            else:
                mapped_df[field_name] = None
        
        # Handle communication preferences
        if 'sms_marketing_enabled' in mapped_df.columns:
            mapped_df['sms_opt_in'] = mapped_df['sms_marketing_enabled'].map(
                {'true': True, 'false': False, True: True, False: False})
        else:
            mapped_df['sms_opt_in'] = False
            
        if 'email_marketing_enabled' in mapped_df.columns:
            mapped_df['email_opt_in'] = mapped_df['email_marketing_enabled'].map(
                {'true': True, 'false': False, True: True, False: False})
        else:
            mapped_df['email_opt_in'] = False
            
        mapped_df['marketing_opt_in'] = mapped_df['email_opt_in'] | mapped_df['sms_opt_in']
        
        # Add timestamps
        now = datetime.now()
        if 'created_at' not in mapped_df.columns:
            mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        return mapped_df
    
    # src/retention_os/data_ingestion/adapters/boulevard_adapter.py

    def _process_provider(self, clinic_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process provider data from Boulevard.
        
        Args:
            clinic_df: Standardized clinic dataframe for foreign key
            
        Returns:
            Standardized provider dataframe
        """
        if 'provider' not in self.dataframes:
            self.logger.warning("Provider data file not found, creating empty provider dataframe")
            # Create an empty dataframe with required columns
            provider_df = pd.DataFrame(columns=[
                'provider_id', 'source_provider_id', 'source_system', 'clinic_id',
                'name', 'specialties', 'active', 'created_at', 'updated_at'
            ])
            return provider_df
            
        df = self.dataframes['provider']
        mapped_df = self.apply_field_mapping(df, 'provider')
        mapped_df = self.apply_type_conversions(mapped_df, 'provider')
        
        # Add foreign key to clinic
        if clinic_df is not None and len(clinic_df) > 0:
            mapped_df['clinic_id'] = clinic_df.iloc[0]['clinic_id']
        else:
            mapped_df['clinic_id'] = None
            
        # Add source system
        mapped_df['source_system'] = self.source_system
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'provider_id', 'PR')
        
        # Create name field from first and last name if available
        if 'name' not in mapped_df.columns:
            if 'first_name' in mapped_df.columns and 'last_name' in mapped_df.columns:
                mapped_df['name'] = mapped_df['first_name'] + ' ' + mapped_df['last_name']
            else:
                mapped_df['name'] = None
        
        # Add specialties as a list (using role_name if available)
        if 'specialties' not in mapped_df.columns:
            if 'role_name' in mapped_df.columns:
                # Convert single role to a list
                mapped_df['specialties'] = mapped_df['role_name'].apply(lambda x: [x] if pd.notna(x) else [])
            else:
                mapped_df['specialties'] = [[] for _ in range(len(mapped_df))]
        
        # Set active flag
        if 'active' not in mapped_df.columns:
            mapped_df['active'] = True
            
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        return mapped_df
    
    # src/retention_os/data_ingestion/adapters/boulevard_adapter.py

    def _process_service(self, clinic_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process service data from Boulevard.
        
        Args:
            clinic_df: Standardized clinic dataframe for foreign key
            
        Returns:
            Standardized service dataframe
        """
        if 'service' not in self.dataframes:
            self.logger.warning("Service data file not found, creating empty service dataframe")
            service_df = pd.DataFrame(columns=[
                'service_id', 'source_service_id', 'source_system', 'clinic_id',
                'name', 'category', 'description', 'duration_minutes', 'base_price',
                'active', 'created_at', 'updated_at'
            ])
            return service_df
        
        # Use the Service Records.csv file
        df = self.dataframes['service']
        mapped_df = self.apply_field_mapping(df, 'service')
        mapped_df = self.apply_type_conversions(mapped_df, 'service')
        
        # Add foreign key to clinic
        if clinic_df is not None and len(clinic_df) > 0:
            mapped_df['clinic_id'] = clinic_df.iloc[0]['clinic_id']
        else:
            mapped_df['clinic_id'] = None
            
        # Add source system
        mapped_df['source_system'] = self.source_system
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'service_id', 'S')
        
        # Add description field if not present
        if 'description' not in mapped_df.columns:
            mapped_df['description'] = None
        
        # Add duration_minutes field if not present
        if 'duration_minutes' not in mapped_df.columns:
            mapped_df['duration_minutes'] = 60  # Default duration
        
        # Make sure active is boolean
        if 'active' in mapped_df.columns:
            mapped_df['active'] = mapped_df['active'].map({'true': True, 'True': True, 'TRUE': True, 
                                                        'false': False, 'False': False, 'FALSE': False})
        else:
            mapped_df['active'] = True
        
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        return mapped_df
    
    def _process_package(self, clinic_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process package data from Boulevard.
        
        Args:
            clinic_df: Standardized clinic dataframe for foreign key
            
        Returns:
            Standardized package dataframe
        """
        if 'package' not in self.dataframes:
            self.logger.warning("Package data file not found, creating empty package dataframe")
            # Create an empty dataframe with required columns
            package_df = pd.DataFrame(columns=[
                'package_id', 'source_package_id', 'source_system', 'clinic_id',
                'name', 'description', 'list_price', 'validity_days', 'active',
                'is_synthetic', 'created_at', 'updated_at'
            ])
            return package_df
            
        df = self.dataframes['package']
        mapped_df = self.apply_field_mapping(df, 'package')
        mapped_df = self.apply_type_conversions(mapped_df, 'package')
        
        # Add foreign key to clinic
        if clinic_df is not None and len(clinic_df) > 0:
            mapped_df['clinic_id'] = clinic_df.iloc[0]['clinic_id']
        else:
            mapped_df['clinic_id'] = None
            
        # Add source system
        mapped_df['source_system'] = self.source_system
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'package_id', 'PK')
        
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        # Add missing required fields
        for field, default in [
            ('description', None),
            ('validity_days', 365),  # Default to 1 year validity
            ('active', True),
            ('is_synthetic', False)
        ]:
            if field not in mapped_df.columns:
                mapped_df[field] = default
        
        # If price information is available, use it
        if 'gross_package_sales' in mapped_df.columns and 'list_price' not in mapped_df.columns:
            mapped_df['list_price'] = mapped_df['gross_package_sales']
        elif 'list_price' not in mapped_df.columns:
            mapped_df['list_price'] = None
        
        return mapped_df
    
    # src/retention_os/data_ingestion/adapters/boulevard_adapter.py

    def _process_appointment(self, patient_df: pd.DataFrame, clinic_df: pd.DataFrame, 
                            provider_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process appointment data from Boulevard.
        
        Args:
            patient_df: Standardized patient dataframe for foreign key
            clinic_df: Standardized clinic dataframe for foreign key
            provider_df: Standardized provider dataframe for foreign key
            
        Returns:
            Standardized appointment dataframe
        """
        if 'appointment' not in self.dataframes:
            self.logger.warning("Appointment data file not found, creating empty appointment dataframe")
            # Create an empty dataframe with required columns
            appointment_df = pd.DataFrame(columns=[
                'appointment_id', 'source_appointment_id', 'source_system', 'patient_id',
                'clinic_id', 'provider_id', 'scheduled_at', 'status', 'cancellation_reason',
                'created_at', 'updated_at'
            ])
            return appointment_df
                
        df = self.dataframes['appointment']
        mapped_df = self.apply_field_mapping(df, 'appointment')
        mapped_df = self.apply_type_conversions(mapped_df, 'appointment')
        
        # Add source system
        mapped_df['source_system'] = self.source_system
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'appointment_id', 'A')
        
        # Add clinic_id
        if clinic_df is not None and len(clinic_df) > 0:
            mapped_df['clinic_id'] = clinic_df.iloc[0]['clinic_id']
        else:
            mapped_df['clinic_id'] = None
        
        # Map patients and providers based on their source IDs
        if patient_df is not None and 'client_id' in mapped_df.columns:
            # Create a mapping from source patient ID to canonical patient ID
            patient_id_map = dict(zip(patient_df['source_patient_id'], patient_df['patient_id']))
            
            # Map the client_id to patient_id
            mapped_df['patient_id'] = mapped_df['client_id'].map(patient_id_map)
        else:
            mapped_df['patient_id'] = None
                
        if provider_df is not None and 'staff_id' in mapped_df.columns:
            # Create a mapping from source provider ID to canonical provider ID
            provider_id_map = dict(zip(provider_df['source_provider_id'], provider_df['provider_id']))
            
            # Map the staff_id to provider_id
            mapped_df['provider_id'] = mapped_df['staff_id'].map(provider_id_map)
        else:
            mapped_df['provider_id'] = None
        
        # Handle scheduled_at - FIX FOR NaT and float errors
        if 'scheduled_at' not in mapped_df.columns:
            if 'start_on' in mapped_df.columns and 'start_at' in mapped_df.columns:
                # Create an empty scheduled_at column
                mapped_df['scheduled_at'] = pd.NaT
                
                # Only combine where both values are not null
                valid_indices = mapped_df['start_on'].notna() & mapped_df['start_at'].notna()
                
                if valid_indices.any():
                    # For valid rows, combine date and time
                    valid_df = mapped_df.loc[valid_indices]
                    
                    # Convert floats to strings if needed
                    start_at_str = valid_df['start_at'].astype(str)
                    start_on_str = valid_df['start_on'].astype(str)
                    
                    # Try to parse the combined string as a datetime
                    try:
                        combined_datetime = pd.to_datetime(
                            start_on_str + ' ' + start_at_str, 
                            errors='coerce'
                        )
                        
                        # Update only the valid rows
                        mapped_df.loc[valid_indices, 'scheduled_at'] = combined_datetime
                    except Exception as e:
                        self.logger.error(f"Error combining date and time: {str(e)}")
            else:
                mapped_df['scheduled_at'] = None
        
        # Handle status
        if 'status' not in mapped_df.columns and 'appointment_state' in mapped_df.columns:
            # Map Boulevard appointment states to canonical status
            status_map = {
                'confirmed': 'completed',
                'no_show': 'no-show',
                'cancelled': 'cancelled',
                # Add more mappings as needed
            }
            mapped_df['status'] = mapped_df['appointment_state'].map(status_map)
        elif 'status' not in mapped_df.columns:
            mapped_df['status'] = None
        
        # Handle cancellation reason
        if 'cancellation_reason' not in mapped_df.columns:
            mapped_df['cancellation_reason'] = None
        
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        return mapped_df
    
    def _process_payment(self, patient_df: pd.DataFrame, appointment_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process payment data from Boulevard.
        
        Args:
            patient_df: Standardized patient dataframe for foreign key
            appointment_df: Standardized appointment dataframe for foreign key
            
        Returns:
            Standardized payment dataframe
        """
        if 'payment' not in self.dataframes:
            self.logger.warning("Payment data file not found, creating empty payment dataframe")
            # Create an empty dataframe with required columns
            payment_df = pd.DataFrame(columns=[
                'payment_id', 'source_payment_id', 'source_system', 'patient_id',
                'appointment_id', 'amount', 'method', 'paid_at', 'status',
                'created_at', 'updated_at'
            ])
            return payment_df
            
        df = self.dataframes['payment']
        mapped_df = self.apply_field_mapping(df, 'payment')
        mapped_df = self.apply_type_conversions(mapped_df, 'payment')
        
        # Add source system
        mapped_df['source_system'] = self.source_system
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'payment_id', 'PA')
        
        # Handle client_name to patient_id mapping
        if patient_df is not None and 'client_name' in mapped_df.columns:
            # Create full name field in patient_df for matching
            if 'full_name' not in patient_df.columns and 'first_name' in patient_df.columns and 'last_name' in patient_df.columns:
                patient_df['full_name'] = patient_df['first_name'] + ' ' + patient_df['last_name']
                
            # Create a mapping from full name to patient_id
            patient_name_map = dict(zip(patient_df['full_name'], patient_df['patient_id']))
            
            # Map the client_name to patient_id
            mapped_df['patient_id'] = mapped_df['client_name'].map(patient_name_map)
        else:
            mapped_df['patient_id'] = None
        
        # Appointment ID mapping is complex and would require more context
        # For now, we'll leave it as NULL
        mapped_df['appointment_id'] = None
        
        # Handle payment date
        if 'paid_at' not in mapped_df.columns and 'created_on' in mapped_df.columns:
            mapped_df['paid_at'] = pd.to_datetime(mapped_df['created_on'], errors='coerce')
        elif 'paid_at' not in mapped_df.columns:
            mapped_df['paid_at'] = None
        
        # Handle payment method
        if 'method' not in mapped_df.columns and 'payment_method' in mapped_df.columns:
            # Map Boulevard payment methods to canonical methods
            method_map = {
                'credit card': 'card',
                'credit_card': 'card',
                'cash': 'cash',
                'check': 'check',
                'gift card': 'gift_card',
                # Add more mappings as needed
            }
            mapped_df['method'] = mapped_df['payment_method'].map(method_map)
        elif 'method' not in mapped_df.columns:
            mapped_df['method'] = None
        
        # Continuing from the previous code for _process_payment method
        # Handle amount
        if 'amount' not in mapped_df.columns and 'transaction_amount' in mapped_df.columns:
            mapped_df['amount'] = mapped_df['transaction_amount']
        elif 'amount' not in mapped_df.columns:
            mapped_df['amount'] = None
        
        # Handle status (assumed completed unless there's info indicating otherwise)
        if 'status' not in mapped_df.columns:
            mapped_df['status'] = 'completed'
        
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        return mapped_df
    
    def _process_patient_package(self, patient_df: pd.DataFrame, package_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process patient package data from Boulevard.
        
        Args:
            patient_df: Standardized patient dataframe for foreign key
            package_df: Standardized package dataframe for foreign key
            
        Returns:
            Standardized patient package dataframe
        """
        # This might be extracted from multiple sources
        # For now, creating a placeholder with minimal data
        self.logger.warning("Creating placeholder patient_package dataframe")
        
        # Create an empty dataframe with required columns
        patient_package_df = pd.DataFrame(columns=[
            'patient_package_id', 'source_patient_package_id', 'source_system', 
            'patient_id', 'package_id', 'purchase_date', 'expiration_date',
            'price_paid', 'discount_amount', 'status', 'services_remaining',
            'created_at', 'updated_at'
        ])
        
        # Add source system
        patient_package_df['source_system'] = self.source_system
        
        # Add timestamps
        now = datetime.now()
        patient_package_df['created_at'] = now
        patient_package_df['updated_at'] = now
        
        return patient_package_df
    
    def _process_outreach_message(self, patient_df: pd.DataFrame, clinic_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process outreach message data from Boulevard.
        
        Args:
            patient_df: Standardized patient dataframe for foreign key
            clinic_df: Standardized clinic dataframe for foreign key
            
        Returns:
            Standardized outreach message dataframe
        """
        if 'marketing' not in self.dataframes:
            self.logger.warning("Marketing data file not found, creating empty outreach_message dataframe")
            # Create an empty dataframe with required columns
            outreach_df = pd.DataFrame(columns=[
                'message_id', 'patient_id', 'clinic_id', 'sent_at', 'channel',
                'template_id', 'package_id', 'discount_percent', 'experiment_group',
                'response_status', 'booking_id', 'opened_at', 'clicked_at',
                'campaign_id', 'created_at', 'updated_at'
            ])
            return outreach_df
            
        df = self.dataframes['marketing']
        mapped_df = self.apply_field_mapping(df, 'outreach_message')
        mapped_df = self.apply_type_conversions(mapped_df, 'outreach_message')
        
        # Generate surrogate keys
        mapped_df = self.generate_surrogate_key(mapped_df, 'message_id', 'M')
        
        # Add clinic_id
        if clinic_df is not None and len(clinic_df) > 0:
            mapped_df['clinic_id'] = clinic_df.iloc[0]['clinic_id']
        else:
            mapped_df['clinic_id'] = None
        
        # Note: For marketing data, we might not have individual patient mappings
        # but rather aggregate stats. For a proper implementation, we'd need
        # to have actual message logs rather than just campaign performance data.
        mapped_df['patient_id'] = None
        
        # Handle other required fields with default values
        defaults = {
            'sent_at': datetime.now(),
            'channel': None,
            'template_id': None,
            'package_id': None,
            'discount_percent': None,
            'experiment_group': 'historical',
            'response_status': None,
            'booking_id': None,
            'opened_at': None,
            'clicked_at': None,
            'campaign_id': None,
            'source_system': self.source_system
        }
        
        for field, default in defaults.items():
            if field not in mapped_df.columns:
                mapped_df[field] = default
        
        # Add timestamps
        now = datetime.now()
        mapped_df['created_at'] = now
        mapped_df['updated_at'] = now
        
        return mapped_df