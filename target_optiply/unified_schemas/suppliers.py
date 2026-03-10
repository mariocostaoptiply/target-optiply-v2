from __future__ import annotations

import json
from typing import List, Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class SupplierSchema(OptiplyBaseSchema):

    # Mandatory
    name: str

    # Optional
    emails: Optional[List[str]] = None
    minimumOrderValue: Optional[float] = None
    fixedCosts: Optional[float] = None
    deliveryTime: Optional[int] = None
    userReplenishmentPeriod: Optional[int] = None
    reactingToLostSales: Optional[bool] = None
    lostSalesReaction: Optional[int] = None
    lostSalesMovReaction: Optional[int] = None
    backorders: Optional[bool] = None
    backorderThreshold: Optional[int] = None
    backordersReaction: Optional[int] = None
    maxLoadCapacity: Optional[int] = None
    containerVolume: Optional[int] = None
    ignored: Optional[bool] = None
    globalLocationNumber: Optional[str] = None
    type: Optional[str] = None

    @field_validator("emails", mode="before")
    @classmethod
    def parse_emails(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except (json.JSONDecodeError, ValueError):
                return []
        return v

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v):
        if v is not None and v not in ["vendor", "producer"]:
            return "vendor"
        return v

    @field_validator("globalLocationNumber", mode="before")
    @classmethod
    def validate_gln(cls, v):
        if v is not None and len(str(v)) != 13:
            return None
        return v

    @field_validator("deliveryTime", "userReplenishmentPeriod", "lostSalesReaction",
                     "lostSalesMovReaction", "backorderThreshold", "backordersReaction",
                     "maxLoadCapacity", "containerVolume", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("minimumOrderValue", "fixedCosts", mode="before")
    @classmethod
    def coerce_float(cls, v):
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
        return v
