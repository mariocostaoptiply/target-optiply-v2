from __future__ import annotations

from typing import Optional
from pydantic import Field, field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class BuyOrderSchema(OptiplyBaseSchema):

    # Mandatory
    placed: str
    supplierId: int
    accountId: int

    # totalValue is optional here — computed from line_items in _add_additional_attributes if present
    totalValue: Optional[str] = None

    # Optional
    completed: Optional[str] = None
    expectedDeliveryDate: Optional[str] = None
    assembly: Optional[bool] = None
    line_items: Optional[str] = Field(default=None, exclude=True)  # parsed into orderLines in sink, never sent to API

    @field_validator("totalValue", mode="before")
    @classmethod
    def coerce_total_value(cls, v):
        if v is not None:
            try:
                return str(round(float(v), 2))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("supplierId", "accountId", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v
