from __future__ import annotations

import json
from datetime import datetime
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
    remoteDataSyncedToDate: Optional[datetime] = None
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

    @field_validator("name", mode="before")
    @classmethod
    def truncate_name(cls, v):
        if v is not None:
            return str(v)[:255]
        return v

    @field_validator("emails", mode="before")
    @classmethod
    def parse_emails(cls, v):
        if v is None:
            return v
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
            # Plain email string — wrap in list
            return [v]
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

    @field_validator("deliveryTime", "userReplenishmentPeriod", mode="before")
    @classmethod
    def coerce_int_clamped(cls, v):
        if v is not None:
            try:
                val = int(float(v))
                if val < 1:
                    return None
                return min(val, 365)
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("lostSalesReaction", "lostSalesMovReaction", "backorderThreshold",
                     "backordersReaction", "maxLoadCapacity", "containerVolume", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("fixedCosts", "minimumOrderValue", mode="before")
    @classmethod
    def coerce_float_rounded(cls, v):
        if v is not None:
            try:
                return round(float(v), 2)
            except (ValueError, TypeError):
                return None
        return v
