from __future__ import annotations

from typing import Optional
from pydantic import Field
from target_optiply.unified_schemas.base import OptiplyBaseSchema


class SellOrderSchema(OptiplyBaseSchema):

    # Mandatory
    placed: str

    # totalValue optional — computed from line_items if present
    totalValue: Optional[str] = None

    # Optional
    completed: Optional[str] = None
    line_items: Optional[str] = Field(default=None, exclude=True)  # parsed into orderLines in sink, never sent to API
