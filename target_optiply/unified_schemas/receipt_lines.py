from __future__ import annotations

from typing import Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class ReceiptLineSchema(OptiplyBaseSchema):

    # Mandatory (enforced via get_mandatory_fields; Optional here so cache can fill buyOrderLineId)
    occurred: Optional[str] = None
    quantity: Optional[int] = None
    buyOrderLineId: Optional[str] = None

    # Optional
    remoteId: Optional[str] = None
    remoteDataSyncedToDate: Optional[str] = None  # always overridden by _add_additional_attributes

    @field_validator("occurred", mode="before")
    @classmethod
    def coerce_occurred(cls, v):
        if v is not None:
            if isinstance(v, str):
                return v
            try:
                return v.isoformat()
            except (AttributeError, ValueError, TypeError):
                return str(v)
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

    @field_validator("buyOrderLineId", mode="before")
    @classmethod
    def coerce_buy_order_line_id(cls, v):
        if v is not None:
            try:
                return str(int(float(v)))
            except (ValueError, TypeError):
                return str(v)
        return v
