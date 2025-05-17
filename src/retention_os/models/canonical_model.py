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
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
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
    home_phone: Optional[str] = None
    work_phone: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    gender: Optional[str] = None
    referral_source: Optional[str] = None
    is_active: bool = True
    sms_marketing_enabled: bool = False
    email_marketing_enabled: bool = False

    @property
    def full_name(self) -> str:
        """Get client's full name."""
        return f"{self.first_name} {self.last_name}"


class Professional(BaseCanonicalModel):
    """Professional entity (staff/provider)."""
    first_name: str
    last_name: str
    email: Optional[EmailStr] = None
    mobile_phone: Optional[str] = None
    role_id: Optional[str] = None
    role_name: Optional[str] = None
    specialties: Optional[List[str]] = None
    is_active: bool = True

    @property
    def full_name(self) -> str:
        """Get professional's full name."""
        return f"{self.first_name} {self.last_name}"


class Service(BaseCanonicalModel):
    """Service entity (treatment offering)."""
    name: str
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    description: Optional[str] = None
    duration_minutes: Optional[int] = None
    base_price: Optional[float] = None
    is_active: bool = True


class Package(BaseCanonicalModel):
    """Package entity (bundle of services)."""
    name: str
    description: Optional[str] = None
    list_price: Optional[float] = None
    net_price: Optional[float] = None
    tax: Optional[float] = None
    validity_days: Optional[int] = None
    service_count: Optional[int] = None


class PackageComponent(BaseCanonicalModel):
    """PackageComponent entity (services within packages)."""
    package_id: str
    service_id: str
    quantity: int = 1
    unit_price: Optional[float] = None
    discount_percentage: Optional[float] = None


class Appointment(BaseCanonicalModel):
    """Appointment entity (scheduled visit)."""
    client_id: str
    business_id: str
    professional_id: Optional[str] = None
    scheduled_at: datetime
    end_at: Optional[datetime] = None
    status: str  # completed, no-show, cancelled, etc.
    notes: Optional[str] = None
    cancellation_reason: Optional[str] = None
    cancelled_at: Optional[datetime] = None
    booked_by: Optional[str] = None  # client, staff, etc.


class AppointmentLine(BaseCanonicalModel):
    """AppointmentLine entity (services within appointments)."""
    appointment_id: str
    service_id: str
    professional_id: Optional[str] = None
    client_package_id: Optional[str] = None
    price: Optional[float] = None
    duration_minutes: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: Optional[str] = None


class Payment(BaseCanonicalModel):
    """Payment entity (financial transactions)."""
    client_id: Optional[str] = None
    business_id: str
    amount: float
    method: Optional[str] = None  # credit card, cash, etc.
    paid_at: datetime
    order_number: Optional[str] = None
    is_refund: bool = False
    notes: Optional[str] = None


class ClientPackage(BaseCanonicalModel):
    """ClientPackage entity (packages purchased by clients)."""
    client_id: str
    package_id: str
    purchase_date: datetime
    expiration_date: Optional[datetime] = None
    services_remaining: Optional[Dict[str, int]] = None
    original_price: Optional[float] = None
    paid_price: Optional[float] = None
    status: str = "active"  # active, expired, used


class OutreachMessage(BaseCanonicalModel):
    """OutreachMessage entity (marketing communications)."""
    client_id: Optional[str] = None
    business_id: str
    type: str  # email, sms, etc.
    template_id: Optional[str] = None
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None
    sent_at: Optional[datetime] = None
    opened_at: Optional[datetime] = None
    clicked_at: Optional[datetime] = None
    status: str  # sent, failed, opened, clicked


class ProductSale(BaseCanonicalModel):
    """ProductSale entity (retail product sales)."""
    client_id: Optional[str] = None
    business_id: str
    transaction_date: datetime
    order_number: Optional[str] = None
    subtotal: float
    tax: Optional[float] = None
    total: float
    payment_id: Optional[str] = None
    status: str = "completed"


class ProductSaleLine(BaseCanonicalModel):
    """ProductSaleLine entity (individual products within sales)."""
    product_sale_id: str
    product_id: Optional[str] = None
    product_name: str
    product_brand: Optional[str] = None
    quantity: int
    unit_price: float
    discount: Optional[float] = None
    total_price: float
    tax: Optional[float] = None


# Full Canonical Data Model
class CanonicalDataModel(BaseModel):
    """Complete Canonical Data Model containing all entity collections."""
    businesses: List[Business] = Field(default_factory=list)
    clients: List[Client] = Field(default_factory=list)
    professionals: List[Professional] = Field(default_factory=list)
    services: List[Service] = Field(default_factory=list)
    packages: List[Package] = Field(default_factory=list)
    package_components: List[PackageComponent] = Field(default_factory=list)
    appointments: List[Appointment] = Field(default_factory=list)
    appointment_lines: List[AppointmentLine] = Field(default_factory=list)
    payments: List[Payment] = Field(default_factory=list)
    client_packages: List[ClientPackage] = Field(default_factory=list)
    outreach_messages: List[OutreachMessage] = Field(default_factory=list)
    product_sales: List[ProductSale] = Field(default_factory=list)
    product_sale_lines: List[ProductSaleLine] = Field(default_factory=list)

    process_date: datetime = Field(default_factory=datetime.now)
    source_system: str
    business_name: str
    files_processed: List[str] = Field(default_factory=list)
    entities_processed: Dict[str, int] = Field(default_factory=dict)