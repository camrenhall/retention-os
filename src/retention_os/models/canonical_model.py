"""
Canonical Data Model definitions using Pydantic models.
"""
from datetime import datetime
from typing import Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, EmailStr, Field, validator


class BaseCanonicalModel(BaseModel):
    """Base class for all canonical models."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    source_id: Optional[str] = None
    external_id: Optional[str] = None
    business_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    metadata: Dict = Field(default_factory=dict)

    class Config:
        populate_by_name = True


class Business(BaseCanonicalModel):
    """Business entity (med spa)."""
    name: str
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[EmailStr] = None
    website: Optional[str] = None
    timezone: Optional[str] = None
    

class Client(BaseCanonicalModel):
    """Client entity (customer)."""
    first_name: str
    last_name: str
    email: Optional[EmailStr] = None
    mobile_phone: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    gender: Optional[str] = None
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    email_opt_in: Optional[bool] = None
    sms_opt_in: Optional[bool] = None
    marketing_opt_in: Optional[bool] = None

    @property
    def full_name(self) -> str:
        """Get client's full name."""
        return f"{self.first_name} {self.last_name}"


class Service(BaseCanonicalModel):
    """Service entity (treatment offering)."""
    name: str
    category: Optional[str] = None
    description: Optional[str] = None
    duration_minutes: Optional[int] = None
    base_price: Optional[float] = None
    is_active: bool = True


class Package(BaseCanonicalModel):
    """Package entity (bundle of services)."""
    name: str
    description: Optional[str] = None
    list_price: Optional[float] = None
    validity_days: Optional[int] = None
    is_active: Optional[bool] = None
    is_synthetic: Optional[bool] = None


class PackageComponent(BaseCanonicalModel):
    """PackageComponent entity (services within packages)."""
    package_id: str
    service_id: str
    quantity: int = 1
    sequence_order: Optional[int] = None


class Appointment(BaseCanonicalModel):
    """Appointment entity (scheduled visit)."""
    client_id: str
    scheduled_at: datetime
    status: str  # completed, no-show, cancelled, etc.
    cancellation_reason: Optional[str] = None


class AppointmentLine(BaseCanonicalModel):
    """AppointmentLine entity (services within appointments)."""
    appointment_id: str
    service_id: str
    package_id: Optional[str] = None
    unit_price: Optional[float] = None
    discount_amount: Optional[float] = None
    discount_reason: Optional[str] = None
    tax_amount: Optional[float] = None
    quantity: Optional[int] = None
    notes: Optional[str] = None


class Payment(BaseCanonicalModel):
    """Payment entity (financial transactions)."""
    source_payment_id: Optional[str] = None
    client_id: str
    appointment_id: Optional[str] = None
    amount: float
    method: Optional[str] = None
    paid_at: datetime
    status: Optional[str] = None


class ClientPackage(BaseCanonicalModel):
    """ClientPackage entity (packages purchased by clients)."""
    client_id: str
    package_id: str
    purchase_date: datetime
    expiration_date: Optional[datetime] = None
    price_paid: Optional[float] = None
    discount_amount: Optional[float] = None
    status: Optional[str] = None
    services_remaining: Optional[int] = None


# Full Canonical Data Model
class CanonicalDataModel(BaseModel):
    """Complete Canonical Data Model containing all entity collections."""
    businesses: List[Business] = Field(default_factory=list)
    clients: List[Client] = Field(default_factory=list)
    services: List[Service] = Field(default_factory=list)
    packages: List[Package] = Field(default_factory=list)
    package_components: List[PackageComponent] = Field(default_factory=list)
    appointments: List[Appointment] = Field(default_factory=list)
    appointment_lines: List[AppointmentLine] = Field(default_factory=list)
    payments: List[Payment] = Field(default_factory=list)
    client_packages: List[ClientPackage] = Field(default_factory=list)

    process_date: datetime = Field(default_factory=datetime.now)
    source_system: str
    business_name: str
    files_processed: List[str] = Field(default_factory=list)
    entities_processed: Dict[str, int] = Field(default_factory=dict)