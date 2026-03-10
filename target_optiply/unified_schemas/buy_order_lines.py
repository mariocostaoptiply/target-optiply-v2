from __future__ import annotations

from typing import Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class BuyOrderLineSchema(OptiplyBaseSchema):

    # Mandatory
    subtotalValue: str
    productId: int
    quantity: float
    buyOrderId: int

    # Optional
    expectedDeliveryDate: Optional[str] = None

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
    def coerce_float(cls, v):
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return v
