from __future__ import annotations

from typing import Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class SupplierProductSchema(OptiplyBaseSchema):

    # Mandatory (enforced via get_mandatory_fields, kept Optional here so cache can fill them)
    name: str
    productId: Optional[str] = None
    supplierId: Optional[str] = None

    # Optional
    skuCode: Optional[str] = None
    eanCode: Optional[str] = None
    articleCode: Optional[str] = None
    price: Optional[float] = None
    minimumPurchaseQuantity: Optional[int] = None
    lotSize: Optional[int] = None
    availability: Optional[bool] = None
    availabilityDate: Optional[str] = None
    preferred: Optional[bool] = None
    deliveryTime: Optional[int] = None
    status: Optional[str] = None
    freeStock: Optional[int] = None
    weight: Optional[float] = None
    volume: Optional[float] = None

    @field_validator("productId", "supplierId", mode="before")
    @classmethod
    def coerce_id_str(cls, v):
        if v is not None:
            try:
                return str(int(float(v)))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("deliveryTime", "freeStock", mode="before")
    @classmethod
    def coerce_int(cls, v):
        if v is not None:
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price(cls, v):
        if v is not None:
            try:
                return round(float(v), 2)
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("minimumPurchaseQuantity", "lotSize", mode="before")
    @classmethod
    def coerce_min_one(cls, v):
        if v is not None:
            try:
                val = int(float(v))
                return val if val >= 1 else None
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("weight", "volume", mode="before")
    @classmethod
    def coerce_float(cls, v):
        if v is not None:
            try:
                return round(float(v), 2)
            except (ValueError, TypeError):
                return None
        return v

    @field_validator("availability", "preferred", mode="before")
    @classmethod
    def coerce_bool(cls, v):
        if isinstance(v, str):
            if v.lower() in ["true", "1", "yes"]:
                return True
            elif v.lower() in ["false", "0", "no"]:
                return False
            return None
        return v

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, v):
        if v is not None:
            s = str(v).lower()
            if s in ["enabled", "active", "true", "1"]:
                return "enabled"
            elif s in ["disabled", "inactive", "false", "0"]:
                return "disabled"
            return None
        return v
