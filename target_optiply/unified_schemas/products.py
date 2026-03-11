from __future__ import annotations

from typing import Optional
from pydantic import field_validator
from target_optiply.unified_schemas.base import OptiplyBaseSchema

_MAX_PRICE_INTEGER_DIGITS = 9  # max 999999999.99


class ProductSchema(OptiplyBaseSchema):

    # Mandatory
    name: str
    stockLevel: int
    unlimitedStock: bool = False

    # Optional
    skuCode: Optional[str] = None
    eanCode: Optional[str] = None
    articleCode: Optional[str] = None
    price: Optional[float] = None
    notBeingBought: Optional[bool] = None
    resumingPurchase: Optional[bool] = None
    status: Optional[str] = None
    assembled: Optional[bool] = None
    minimumStock: Optional[float] = None
    maximumStock: Optional[float] = None
    ignored: Optional[bool] = None
    manualServiceLevel: Optional[float] = None
    createdAtRemote: Optional[str] = None
    updatedAt: Optional[str] = None
    stockMeasurementUnit: Optional[str] = None

    @field_validator("name", mode="before")
    @classmethod
    def truncate_name(cls, v):
        if isinstance(v, str):
            return v[:255]
        return v

    @field_validator("skuCode", "eanCode", "articleCode", mode="before")
    @classmethod
    def truncate_code(cls, v):
        if isinstance(v, str):
            return v[:255]
        return v

    @field_validator("stockMeasurementUnit", mode="before")
    @classmethod
    def truncate_measurement_unit(cls, v):
        if isinstance(v, str):
            return v[:10]
        return v

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price(cls, v):
        if v is None:
            return None
        try:
            value = round(float(v), 2)
            if int(abs(value)) >= 10 ** _MAX_PRICE_INTEGER_DIGITS:
                return None
            return value
        except (ValueError, TypeError):
            return None

    @field_validator("createdAtRemote", "updatedAt", mode="before")
    @classmethod
    def parse_datetime(cls, v):
        if v is None:
            return None
        if not isinstance(v, str):
            v = str(v)
        from dateutil import parser as dtparser
        try:
            dt = dtparser.parse(v)
            if dt.tzinfo is None:
                import datetime
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return dt.isoformat()
        except (ValueError, TypeError):
            return None
