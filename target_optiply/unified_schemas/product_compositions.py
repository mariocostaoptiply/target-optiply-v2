from __future__ import annotations

from typing import Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class ProductCompositionSchema(OptiplyBaseSchema):

    # Mandatory
    composedProductId: int
    partProductId: int
    partQuantity: int

    @field_validator("composedProductId", "partProductId", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("partQuantity", mode="before")
    @classmethod
    def coerce_min_one(cls, v):
        if v is not None:
            try:
                val = int(float(v))
                return val if val >= 1 else None
            except (ValueError, TypeError):
                return None
        return v
