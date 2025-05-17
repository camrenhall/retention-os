import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class EnrichmentRule:
    """Rule for enriching entity data."""
    
    def __init__(self, 
                 field: str, 
                 rule_type: str, 
                 parameters: Dict[str, Any] = None):
        """
        Initialize an enrichment rule.
        
        Args:
            field: The field to enrich
            rule_type: The type of enrichment
            parameters: Parameters for the rule
        """
        self.field = field
        self.rule_type = rule_type
        self.parameters = parameters or {}
    
    def __str__(self) -> str:
        """String representation of the rule."""
        return f"{self.rule_type} enrichment for {self.field}"

class SemanticEnricher:
    """Adds derived and calculated attributes to enhance the data model."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the semantic enricher.
        
        Args:
            config: Configuration dictionary for enrichment
        """
        self.config = config or {}
        self.metrics = self.config.get('metrics', [])
        self._enrichers = self._build_enrichers()
        
    def _enrich_provider_performance(self, 
                               entity: Dict[str, Any], 
                               related_entities: Dict[str, List[Dict[str, Any]]], 
                               params: Dict[str, Any]) -> None:
        """
        Enrich provider entity with performance metrics.
        
        Args:
            entity: Provider entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'provider_id' not in entity:
            return
        
        provider_id = entity['provider_id']
        
        # Count appointments
        appointment_count = 0
        completed_count = 0
        cancelled_count = 0
        no_show_count = 0
        
        if 'appointment' in related_entities:
            for appointment in related_entities['appointment']:
                if appointment.get('provider_id') == provider_id:
                    appointment_count += 1
                    status = appointment.get('status', '').lower()
                    if status == 'completed':
                        completed_count += 1
                    elif status == 'cancelled':
                        cancelled_count += 1
                    elif status == 'no-show':
                        no_show_count += 1
        
        # Calculate metrics
        if appointment_count > 0:
            completion_rate = completed_count / appointment_count
            cancellation_rate = cancelled_count / appointment_count
            no_show_rate = no_show_count / appointment_count
        else:
            completion_rate = 0
            cancellation_rate = 0
            no_show_rate = 0
        
        # Add metrics to entity
        entity['appointment_count'] = appointment_count
        entity['completed_count'] = completed_count
        entity['completion_rate'] = round(completion_rate, 2)
        entity['cancellation_rate'] = round(cancellation_rate, 2)
        entity['no_show_rate'] = round(no_show_rate, 2)

    def _enrich_service_popularity(self, 
                                entity: Dict[str, Any], 
                                related_entities: Dict[str, List[Dict[str, Any]]], 
                                params: Dict[str, Any]) -> None:
        """
        Enrich service entity with popularity metrics.
        
        Args:
            entity: Service entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'service_id' not in entity:
            return
        
        service_id = entity['service_id']
        
        # Count appointments for this service
        appointment_count = 0
        
        if 'appointmentLine' in related_entities:
            for line in related_entities['appointmentLine']:
                if line.get('service_id') == service_id:
                    appointment_count += 1
        
        # Add metrics to entity
        entity['appointment_count'] = appointment_count
        
        # Categorize popularity
        if appointment_count > 100:
            entity['popularity'] = 'High'
        elif appointment_count > 50:
            entity['popularity'] = 'Medium'
        elif appointment_count > 10:
            entity['popularity'] = 'Low'
        else:
            entity['popularity'] = 'Very Low'

    def _enrich_clinic_retention(self, 
                            entity: Dict[str, Any], 
                            related_entities: Dict[str, List[Dict[str, Any]]], 
                            params: Dict[str, Any]) -> None:
        """
        Enrich clinic entity with retention metrics.
        
        Args:
            entity: Clinic entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'clinic_id' not in entity:
            return
        
        clinic_id = entity['clinic_id']
        
        # Get patients for this clinic
        clinic_patients = []
        
        if 'patient' in related_entities:
            for patient in related_entities['patient']:
                if patient.get('clinic_id') == clinic_id:
                    clinic_patients.append(patient)
        
        # Count patients by segment
        total_patients = len(clinic_patients)
        active_patients = 0
        at_risk_patients = 0
        churned_patients = 0
        
        for patient in clinic_patients:
            segment = patient.get('client_segment', '').lower()
            if 'active' in segment:
                active_patients += 1
            elif 'at risk' in segment:
                at_risk_patients += 1
            elif 'churned' in segment:
                churned_patients += 1
        
        # Calculate retention rate
        if total_patients > 0:
            retention_rate = active_patients / total_patients
            churn_rate = churned_patients / total_patients
        else:
            retention_rate = 0
            churn_rate = 0
        
        # Add metrics to entity
        entity['total_patients'] = total_patients
        entity['active_patients'] = active_patients
        entity['at_risk_patients'] = at_risk_patients
        entity['churned_patients'] = churned_patients
        entity['retention_rate'] = round(retention_rate, 2)
        entity['churn_rate'] = round(churn_rate, 2)
    
    def _build_enrichers(self) -> Dict[str, callable]:
        """Build dictionary of enrichment functions."""
        return {
            'lifetime_value': self._enrich_lifetime_value,
            'days_since_last_visit': self._enrich_days_since_last_visit,
            'treatment_frequency': self._enrich_treatment_frequency,
            'preferred_provider': self._enrich_preferred_provider,
            'preferred_treatment_category': self._enrich_preferred_treatment_category,
            'client_segment': self._enrich_client_segment,
            'churn_risk': self._enrich_churn_risk,
            'voucher_history': self._enrich_voucher_history,
            'provider_performance': self._enrich_provider_performance,
            'provider_satisfaction': self._enrich_provider_satisfaction,
            'service_popularity': self._enrich_service_popularity,
            'service_conversion': self._enrich_service_conversion,
            'clinic_retention': self._enrich_clinic_retention
        }
    
    def enrich_entities(self, 
                        entity_type: str, 
                        entities: List[Dict[str, Any]], 
                        related_entities: Dict[str, List[Dict[str, Any]]], 
                        rules: List[EnrichmentRule]) -> List[Dict[str, Any]]:
        """
        Enrich entities with derived attributes.
        
        Args:
            entity_type: Type of entity to enrich
            entities: List of entity dictionaries
            related_entities: Dictionary mapping entity types to lists of related entities
            rules: List of enrichment rules
            
        Returns:
            List of enriched entity dictionaries
        """
        enriched_entities = []
        
        # Process each entity
        for entity in entities:
            enriched_entity = entity.copy()
            
            # Apply each rule
            for rule in rules:
                # Skip rule if not in configured metrics
                if rule.rule_type not in self.metrics and self.metrics:
                    continue
                
                # Get the enrichment function
                enricher = self._enrichers.get(rule.rule_type)
                if not enricher:
                    logger.warning(f"Unsupported enrichment rule type: {rule.rule_type}")
                    continue
                
                # Apply the enrichment
                try:
                    enricher(enriched_entity, related_entities, rule.parameters)
                except Exception as e:
                    logger.error(f"Error applying enrichment rule {rule.rule_type} to {entity_type}: {e}")
            
            enriched_entities.append(enriched_entity)
        
        return enriched_entities
    
    def build_relationships(self, 
                           entities: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Build relationships between entities.
        
        Args:
            entities: Dictionary mapping entity types to lists of entities
            
        Returns:
            Dictionary with updated entities including relationship data
        """
        # This is a simplified version for the MVP
        # In a full implementation, we would build more sophisticated relationships
        
        # Create indexes for faster lookups
        indexes = self._build_entity_indexes(entities)
        
        # Link appointments to patients
        if 'appointment' in entities and 'patient' in entities:
            for appointment in entities['appointment']:
                patient_id = appointment.get('patient_id')
                if patient_id and patient_id in indexes['patient']:
                    if 'appointments' not in indexes['patient'][patient_id]:
                        indexes['patient'][patient_id]['appointments'] = []
                    indexes['patient'][patient_id]['appointments'].append(appointment['appointment_id'])
        
        # Link patients to packages
        if 'patientPackage' in entities and 'patient' in entities:
            for patient_package in entities['patientPackage']:
                patient_id = patient_package.get('patient_id')
                if patient_id and patient_id in indexes['patient']:
                    if 'packages' not in indexes['patient'][patient_id]:
                        indexes['patient'][patient_id]['packages'] = []
                    indexes['patient'][patient_id]['packages'].append(patient_package['package_id'])
        
        # Convert indexes back to lists
        for entity_type, entity_dict in indexes.items():
            entities[entity_type] = list(entity_dict.values())
        
        return entities
    
    def _build_entity_indexes(self, entities: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Build indexes for entities by ID.
        
        Args:
            entities: Dictionary mapping entity types to lists of entities
            
        Returns:
            Dictionary mapping entity types to dictionaries of entities by ID
        """
        indexes = {}
        
        for entity_type, entity_list in entities.items():
            id_field = f"{entity_type}_id"
            indexes[entity_type] = {
                entity[id_field]: entity for entity in entity_list if id_field in entity
            }
        
        return indexes
    
    # Enrichment functions
    
    def _enrich_lifetime_value(self, 
                               entity: Dict[str, Any], 
                               related_entities: Dict[str, List[Dict[str, Any]]], 
                               params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with lifetime value.
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        patient_id = entity['patient_id']
        lifetime_value = 0.0
        
        # Sum payments
        if 'payment' in related_entities:
            for payment in related_entities['payment']:
                if payment.get('patient_id') == patient_id and payment.get('status') == 'completed':
                    lifetime_value += float(payment.get('amount', 0))
        
        # Add package purchases
        if 'patientPackage' in related_entities:
            for package in related_entities['patientPackage']:
                if package.get('patient_id') == patient_id:
                    lifetime_value += float(package.get('price_paid', 0))
        
        entity['lifetime_value'] = round(lifetime_value, 2)
    
    def _enrich_days_since_last_visit(self, 
                                      entity: Dict[str, Any], 
                                      related_entities: Dict[str, List[Dict[str, Any]]], 
                                      params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with days since last visit.
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        patient_id = entity['patient_id']
        last_visit = None
        
        # Find most recent appointment
        if 'appointment' in related_entities:
            for appointment in related_entities['appointment']:
                if appointment.get('patient_id') == patient_id and appointment.get('status') == 'completed':
                    appointment_date = appointment.get('scheduled_at')
                    if isinstance(appointment_date, str):
                        try:
                            appointment_date = datetime.fromisoformat(appointment_date.replace('Z', '+00:00'))
                        except ValueError:
                            continue
                    
                    if isinstance(appointment_date, datetime) and (last_visit is None or appointment_date > last_visit):
                        last_visit = appointment_date
        
        if last_visit:
            today = datetime.now()
            days = (today - last_visit).days
            entity['days_since_last_visit'] = days
        else:
            # No visits found
            entity['days_since_last_visit'] = None
    
    def _enrich_treatment_frequency(self, 
                                   entity: Dict[str, Any], 
                                   related_entities: Dict[str, List[Dict[str, Any]]], 
                                   params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with treatment frequency (average days between visits).
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        patient_id = entity['patient_id']
        appointment_dates = []
        
        # Collect appointment dates
        if 'appointment' in related_entities:
            for appointment in related_entities['appointment']:
                if appointment.get('patient_id') == patient_id and appointment.get('status') == 'completed':
                    appointment_date = appointment.get('scheduled_at')
                    if isinstance(appointment_date, str):
                        try:
                            appointment_date = datetime.fromisoformat(appointment_date.replace('Z', '+00:00'))
                            appointment_dates.append(appointment_date)
                        except ValueError:
                            continue
        
        # Calculate average days between visits
        if len(appointment_dates) >= 2:
            # Sort dates
            appointment_dates.sort()
            
            # Calculate intervals
            intervals = [(appointment_dates[i+1] - appointment_dates[i]).days 
                         for i in range(len(appointment_dates)-1)]
            
            # Calculate average
            avg_interval = sum(intervals) / len(intervals)
            entity['visit_frequency_days'] = round(avg_interval, 1)
        else:
            # Not enough visits to calculate
            entity['visit_frequency_days'] = None
    
    def _enrich_preferred_provider(self, 
                                  entity: Dict[str, Any], 
                                  related_entities: Dict[str, List[Dict[str, Any]]], 
                                  params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with preferred provider.
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        patient_id = entity['patient_id']
        provider_counts = {}
        
        # Count appointments by provider
        if 'appointment' in related_entities:
            for appointment in related_entities['appointment']:
                if appointment.get('patient_id') == patient_id and appointment.get('status') == 'completed':
                    provider_id = appointment.get('provider_id')
                    if provider_id:
                        provider_counts[provider_id] = provider_counts.get(provider_id, 0) + 1
        
        # Find most frequent provider
        if provider_counts:
            preferred_provider_id = max(provider_counts.items(), key=lambda x: x[1])[0]
            entity['preferred_provider_id'] = preferred_provider_id
        else:
            entity['preferred_provider_id'] = None
    
    def _enrich_preferred_treatment_category(self, 
                                            entity: Dict[str, Any], 
                                            related_entities: Dict[str, List[Dict[str, Any]]], 
                                            params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with preferred treatment category.
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        patient_id = entity['patient_id']
        service_counts = {}
        service_categories = {}
        
        # Build service category lookup
        if 'service' in related_entities:
            for service in related_entities['service']:
                service_id = service.get('service_id')
                category = service.get('category')
                if service_id and category:
                    service_categories[service_id] = category
        
        # Count appointments by service
        if 'appointmentLine' in related_entities and 'appointment' in related_entities:
            appointment_lookup = {
                appointment.get('appointment_id'): appointment 
                for appointment in related_entities['appointment']
            }
            
            for line in related_entities['appointmentLine']:
                appointment_id = line.get('appointment_id')
                if appointment_id in appointment_lookup:
                    appointment = appointment_lookup[appointment_id]
                    if appointment.get('patient_id') == patient_id and appointment.get('status') == 'completed':
                        service_id = line.get('service_id')
                        if service_id in service_categories:
                            category = service_categories[service_id]
                            service_counts[category] = service_counts.get(category, 0) + 1
        
        # Find most frequent category
        if service_counts:
            preferred_category = max(service_counts.items(), key=lambda x: x[1])[0]
            entity['preferred_treatment_category'] = preferred_category
        else:
            entity['preferred_treatment_category'] = None
    
    def _enrich_client_segment(self, 
                          entity: Dict[str, Any], 
                          related_entities: Dict[str, List[Dict[str, Any]]], 
                          params: Dict[str, Any]) -> None:
        """Enhanced client segmentation using more data points."""
        if 'patient_id' not in entity:
            return
        
        # Get RFM values (ensure these have been calculated first)
        days_since_last_visit = entity.get('days_since_last_visit')
        lifetime_value = entity.get('lifetime_value')
        visit_frequency = entity.get('visit_frequency_days')
        
        # Get voucher redemption info if available
        voucher_count = entity.get('voucher_redemption_count', 0)
        
        # Determine service preferences from appointment data
        service_preferences = entity.get('preferred_treatment_category')
        
        # Calculate segment with enhanced logic
        segment = 'Unknown'
        
        # Use additional data points for more nuanced segmentation
        if days_since_last_visit is not None:
            if days_since_last_visit <= 60:
                if lifetime_value and lifetime_value >= 1000:
                    segment = 'Loyal High Value'
                    if voucher_count > 2:
                        segment = 'Discount-Driven High Value'
                else:
                    segment = 'Active Regular'
                    if voucher_count > 2:
                        segment = 'Discount-Driven Regular'
            elif days_since_last_visit <= 120:
                if lifetime_value and lifetime_value >= 1000:
                    segment = 'At Risk High Value'
                else:
                    segment = 'At Risk Regular'
            else:
                if lifetime_value and lifetime_value >= 1000:
                    segment = 'Churned High Value'
                else:
                    segment = 'Churned Regular'
        
        entity['client_segment'] = segment
    
    def _enrich_churn_risk(self, 
                          entity: Dict[str, Any], 
                          related_entities: Dict[str, List[Dict[str, Any]]], 
                          params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with churn risk score.
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        # Get factors that influence churn
        days_since_last_visit = entity.get('days_since_last_visit')
        visit_frequency = entity.get('visit_frequency_days')
        
        # Skip if no visit history
        if days_since_last_visit is None:
            entity['churn_risk'] = None
            return
        
        # Simple churn risk model for MVP
        # In a full implementation, this would be a more sophisticated model
        
        # Base risk from days since last visit
        if days_since_last_visit <= 30:
            base_risk = 0.1
        elif days_since_last_visit <= 60:
            base_risk = 0.3
        elif days_since_last_visit <= 90:
            base_risk = 0.5
        elif days_since_last_visit <= 120:
            base_risk = 0.7
        else:
            base_risk = 0.9
        
        # Adjust based on visit frequency if available
        if visit_frequency is not None:
            if visit_frequency <= 14:  # Frequent visitor
                frequency_factor = 0.7  # Lower risk
            elif visit_frequency <= 30:
                frequency_factor = 0.9
            elif visit_frequency <= 60:
                frequency_factor = 1.1
            else:
                frequency_factor = 1.3  # Higher risk
            
            final_risk = base_risk * frequency_factor
        else:
            final_risk = base_risk
        
        # Cap at 0.99
        entity['churn_risk'] = min(round(final_risk, 2), 0.99)
        
        
    def _enrich_voucher_history(self, 
                           entity: Dict[str, Any], 
                           related_entities: Dict[str, List[Dict[str, Any]]], 
                           params: Dict[str, Any]) -> None:
        """
        Enrich patient entity with voucher redemption history.
        
        Args:
            entity: Patient entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'patient_id' not in entity:
            return
        
        patient_id = entity['patient_id']
        
        # Get voucher redemptions if available
        if 'voucherRedemption' in related_entities:
            patient_vouchers = [v for v in related_entities['voucherRedemption'] 
                            if v.get('patient_id') == patient_id]
            
            if patient_vouchers:
                total_voucher_value = sum(v.get('redeemed_value', 0) for v in patient_vouchers)
                entity['voucher_redemption_count'] = len(patient_vouchers)
                entity['total_voucher_value'] = total_voucher_value
                
                # Find most recent redemption
                most_recent = max(patient_vouchers, 
                                key=lambda v: v.get('redemption_date', datetime.min.isoformat()))
                entity['last_voucher_redemption_date'] = most_recent.get('redemption_date')
                
    def _enrich_provider_satisfaction(self, 
                                    entity: Dict[str, Any], 
                                    related_entities: Dict[str, List[Dict[str, Any]]], 
                                    params: Dict[str, Any]) -> None:
        """
        Enrich provider entity with patient satisfaction metrics.
        
        Args:
            entity: Provider entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'provider_id' not in entity:
            return
        
        provider_id = entity['provider_id']
        
        # For MVP, we'll use a basic satisfaction model based on cancellation rates
        # In a real implementation, this would use patient feedback or more sophisticated metrics
        
        # Get all appointments for this provider
        provider_appointments = []
        
        if 'appointment' in related_entities:
            for appointment in related_entities['appointment']:
                if appointment.get('provider_id') == provider_id:
                    provider_appointments.append(appointment)
        
        # Calculate basic satisfaction metrics
        total_appointments = len(provider_appointments)
        if total_appointments == 0:
            entity['patient_satisfaction'] = None
            return
        
        # Count completions and cancellations
        completed = sum(1 for a in provider_appointments if a.get('status') == 'completed')
        cancelled = sum(1 for a in provider_appointments if a.get('status') == 'cancelled')
        
        # Simple satisfaction metric: percentage of completed appointments
        if total_appointments > 0:
            completion_rate = completed / total_appointments
            
            # Convert to a 5-point scale
            if completion_rate >= 0.95:
                satisfaction = 5.0  # Excellent
            elif completion_rate >= 0.9:
                satisfaction = 4.5
            elif completion_rate >= 0.85:
                satisfaction = 4.0
            elif completion_rate >= 0.8:
                satisfaction = 3.5
            elif completion_rate >= 0.75:
                satisfaction = 3.0  # Average
            elif completion_rate >= 0.7:
                satisfaction = 2.5
            elif completion_rate >= 0.65:
                satisfaction = 2.0
            elif completion_rate >= 0.6:
                satisfaction = 1.5
            else:
                satisfaction = 1.0  # Poor
            
            entity['patient_satisfaction'] = round(satisfaction, 1)
        else:
            entity['patient_satisfaction'] = None

    def _enrich_service_conversion(self, 
                                entity: Dict[str, Any], 
                                related_entities: Dict[str, List[Dict[str, Any]]], 
                                params: Dict[str, Any]) -> None:
        """
        Enrich service entity with conversion metrics.
        
        Args:
            entity: Service entity to enrich
            related_entities: Dictionary of related entities
            params: Parameters for the enrichment
        """
        if 'service_id' not in entity:
            return
        
        service_id = entity['service_id']
        
        # For MVP, we'll calculate conversion as percentage of packages that include this service
        # In a real implementation, this would be more sophisticated
        
        # Count packages that include this service
        packages_with_service = 0
        total_packages = 0
        
        if 'packageComponent' in related_entities and 'package' in related_entities:
            total_packages = len(related_entities['package'])
            
            for component in related_entities['packageComponent']:
                if component.get('service_id') == service_id:
                    packages_with_service += 1
        
        # Calculate conversion rate
        if total_packages > 0:
            conversion_rate = packages_with_service / total_packages
            entity['package_inclusion_rate'] = round(conversion_rate, 2)
        else:
            entity['package_inclusion_rate'] = 0.0
        
        # Also calculate what percentage of appointments for this service came from packages
        package_appointments = 0
        total_service_appointments = 0
        
        if 'appointmentLine' in related_entities:
            for line in related_entities['appointmentLine']:
                if line.get('service_id') == service_id:
                    total_service_appointments += 1
                    if line.get('package_id') is not None:
                        package_appointments += 1
        
        # Calculate package usage rate
        if total_service_appointments > 0:
            package_usage_rate = package_appointments / total_service_appointments
            entity['package_usage_rate'] = round(package_usage_rate, 2)
        else:
            entity['package_usage_rate'] = 0.0