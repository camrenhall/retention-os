from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field
from datetime import datetime

# Define nested models

class Address(BaseModel):
    street: str = ""
    city: str = ""
    state: str = ""
    postal_code: str = ""
    country: str = ""

class ContactInfo(BaseModel):
    phone: str = ""
    email: str = ""

class CommunicationPreferences(BaseModel):
    email_opt_in: bool = False
    sms_opt_in: bool = False
    marketing_opt_in: bool = False

# Define main entity models

class Clinic(BaseModel):
    clinic_id: str
    source_clinic_id: str
    source_system: str
    name: str
    timezone: str
    address: Address
    contact_info: ContactInfo
    created_at: datetime
    updated_at: datetime

class Patient(BaseModel):
    patient_id: str
    source_patient_id: str
    source_system: str
    clinic_id: str
    first_name: str
    last_name: str
    date_of_birth: Optional[datetime] = None
    gender: str = "Unknown"
    email: str = ""
    mobile_phone: str = ""
    address: Address
    communication_preferences: CommunicationPreferences
    created_at: datetime
    updated_at: datetime

class Provider(BaseModel):
    provider_id: str
    source_provider_id: str
    source_system: str
    clinic_id: str
    name: str
    specialties: List[str] = []
    active: bool = True
    created_at: datetime
    updated_at: datetime
    services_performed: Optional[int] = None
    revenue_generated: Optional[float] = None
    utilization_percentage: Optional[float] = None
    start_date: Optional[datetime] = None
    tenure_days: Optional[int] = None

class Service(BaseModel):
    service_id: str
    source_service_id: str
    source_system: str
    clinic_id: str
    name: str
    category: str = ""
    description: str = ""
    duration_minutes: int = 60
    base_price: float = 0.0
    active: bool = True
    created_at: datetime
    updated_at: datetime
    gross_sales: Optional[float] = None
    net_sales: Optional[float] = None
    service_count: Optional[int] = None

class Package(BaseModel):
    package_id: str
    source_package_id: str
    source_system: str
    clinic_id: str
    name: str
    description: str = ""
    list_price: float = 0.0
    validity_days: int = 365
    active: bool = True
    is_synthetic: bool = False
    created_at: datetime
    updated_at: datetime

class PackageComponent(BaseModel):
    package_component_id: str
    package_id: str
    service_id: str
    quantity: int = 1
    sequence_order: int = 1
    created_at: datetime
    updated_at: datetime

class Appointment(BaseModel):
    appointment_id: str
    source_appointment_id: str
    source_system: str
    patient_id: str
    clinic_id: str
    provider_id: str
    scheduled_at: datetime
    status: str
    cancellation_reason: str = ""
    created_at: datetime
    updated_at: datetime

class AppointmentLine(BaseModel):
    appointment_line_id: str
    appointment_id: str
    service_id: str
    package_id: Optional[str] = None
    unit_price: float = 0.0
    discount_amount: float = 0.0
    discount_reason: str = ""
    tax_amount: float = 0.0
    quantity: int = 1
    notes: str = ""
    created_at: datetime
    updated_at: datetime

class Payment(BaseModel):
    payment_id: str
    source_payment_id: str
    source_system: str
    patient_id: str
    appointment_id: Optional[str] = None
    amount: float
    method: str
    paid_at: datetime
    status: str = "completed"
    created_at: datetime
    updated_at: datetime

class PatientPackage(BaseModel):
    patient_package_id: str
    source_patient_package_id: str = ""
    source_system: str
    patient_id: str
    package_id: str
    purchase_date: datetime
    expiration_date: datetime
    price_paid: float = 0.0
    discount_amount: float = 0.0
    status: str = "active"
    services_remaining: int = 0
    created_at: datetime
    updated_at: datetime

class OutreachMessage(BaseModel):
    message_id: str
    patient_id: str
    clinic_id: str
    sent_at: datetime
    channel: str
    template_id: str = ""
    package_id: Optional[str] = None
    discount_percent: float = 0.0
    experiment_group: str = ""
    response_status: str = ""
    booking_id: Optional[str] = None
    opened_at: Optional[datetime] = None
    clicked_at: Optional[datetime] = None
    campaign_id: str = ""
    created_at: datetime
    updated_at: datetime

class RetailSale(BaseModel):
    retail_sale_id: str
    source_retail_sale_id: str = ""
    source_system: str
    patient_id: str
    clinic_id: str
    sale_date: datetime
    total_amount: float = 0.0
    created_at: datetime
    updated_at: datetime

class RetailSaleLine(BaseModel):
    retail_sale_line_id: str
    retail_sale_id: str
    product_id: str
    product_name: str = ""
    product_category: str = ""
    quantity: int = 1
    unit_price: float = 0.0
    discount_amount: float = 0.0
    tax_amount: float = 0.0
    created_at: datetime
    updated_at: datetime
    
class VoucherRedemption(BaseModel):
    voucher_id: str
    source_voucher_id: str = ""
    source_system: str
    patient_id: str
    redemption_date: datetime
    original_value: float = 0.0
    redeemed_value: float = 0.0
    created_at: datetime
    updated_at: datetime

# Dictionary mapping entity types to their model classes
entity_models = {
    'clinic': Clinic,
    'patient': Patient,
    'provider': Provider,
    'service': Service,
    'package': Package,
    'packageComponent': PackageComponent,
    'appointment': Appointment,
    'appointmentLine': AppointmentLine,
    'payment': Payment,
    'patientPackage': PatientPackage,
    'outreachMessage': OutreachMessage,
    'retailSale': RetailSale,
    'retailSaleLine': RetailSaleLine,
    'voucherRedemption': VoucherRedemption
}