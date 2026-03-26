from __future__ import annotations

from typing import Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class BuyOrderLineSchema(OptiplyBaseSchema):

    # Mandatory (enforced via get_mandatory_fields; Optional here so cache can fill them)
    subtotalValue: Optional[str] = None
    productId: Optional[int] = None
    quantity: Optional[int] = None
    buyOrderId: Optional[int] = None

    # Optional
    expectedDeliveryDate: Optional[str] = None

    @field_validator("subtotalValue", mode="before")
    @classmethod
    def coerce_subtotal_value(cls, v):
        if v is not None:
            try:
                return str(round(float(v), 2))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("productId", "buyOrderId", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("quantity", mode="before")
    @classmethod
    def coerce_quantity(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v
