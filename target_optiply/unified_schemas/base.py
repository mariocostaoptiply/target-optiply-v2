from __future__ import annotations

from pydantic import BaseModel, ConfigDict, model_validator


class OptiplyBaseSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def clean_strings(cls, values):
        if isinstance(values, dict):
            return {
                k: v.replace("\r", "").replace("\n", "").strip()
                if isinstance(v, str) else v
                for k, v in values.items()
            }
        return values
