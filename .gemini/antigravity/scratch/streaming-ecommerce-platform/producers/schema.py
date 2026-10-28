"""
Data schemas for simulated e-commerce events.
Uses Pydantic v2 for data validation, typing, and JSON serialization.
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class ClickEventType(str, Enum):
    VIEW = "view"
    ADD_TO_CART = "add_to_cart"
    CHECKOUT = "checkout"


class OrderStatus(str, Enum):
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"


class PaymentMethod(str, Enum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    PAYPAL = "PAYPAL"
    UPI = "UPI"
    APPLE_PAY = "APPLE_PAY"


class PaymentStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class Customer(BaseModel):
    customer_id: str = Field(..., description="Unique customer UUID")
    name: str = Field(..., description="Full customer name")
    email: str = Field(..., description="Customer email address")
    city: str = Field(..., description="Customer residence city")
    state: str = Field(..., description="Customer residence state")
    country: str = Field(default="USA", description="Country")
    signup_date: str = Field(..., description="ISO 8601 date string of signup")


class Product(BaseModel):
    product_id: str = Field(..., description="Unique product SKU/UUID")
    name: str = Field(..., description="Product title")
    category: str = Field(..., description="Product category hierarchy")
    price: float = Field(..., gt=0, description="Unit price in USD")
    stock: int = Field(default=100, ge=0, description="Available inventory stock")


class ClickstreamEvent(BaseModel):
    event_id: str = Field(..., description="Unique click event UUID")
    session_id: str = Field(..., description="User browsing session UUID")
    customer_id: str = Field(..., description="Customer identifier")
    product_id: Optional[str] = Field(None, description="Target product SKU if viewing/acting on item")
    event_type: ClickEventType = Field(..., description="view, add_to_cart, checkout")
    page_url: str = Field(..., description="Current web/app URL")
    referrer: str = Field(..., description="Referrer source (google, direct, social, etc.)")
    device: str = Field(..., description="User device (desktop, mobile, tablet)")
    event_timestamp: str = Field(..., description="ISO 8601 event timestamp with timezone")

    @field_validator("event_timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        # Validate that string is parseable as ISO timestamp
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class OrderEvent(BaseModel):
    order_id: str = Field(..., description="Unique order UUID")
    session_id: str = Field(..., description="Originating session identifier")
    customer_id: str = Field(..., description="Purchasing customer identifier")
    product_id: str = Field(..., description="Purchased product SKU")
    quantity: int = Field(..., gt=0, description="Quantity of product purchased")
    unit_price: float = Field(..., gt=0, description="Unit price at time of purchase")
    total_amount: float = Field(..., gt=0, description="Calculated total order price (quantity * unit_price)")
    order_status: OrderStatus = Field(default=OrderStatus.COMPLETED, description="Lifecycle status")
    order_timestamp: str = Field(..., description="ISO 8601 order creation timestamp")

    @field_validator("order_timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class PaymentEvent(BaseModel):
    payment_id: str = Field(..., description="Unique payment transaction UUID")
    order_id: str = Field(..., description="Associated order identifier")
    customer_id: str = Field(..., description="Customer making payment")
    payment_method: PaymentMethod = Field(..., description="Tender method")
    payment_status: PaymentStatus = Field(..., description="Transaction settlement status")
    amount: float = Field(..., gt=0, description="Payment transaction amount in USD")
    payment_timestamp: str = Field(..., description="ISO 8601 payment settlement timestamp")

    @field_validator("payment_timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v
