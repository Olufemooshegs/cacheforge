from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ProductRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    category: str
    price_cents: int
    description: str
    updated_at: datetime | None = None


class ProductUpdate(BaseModel):
    name: str | None = None
    category: str | None = None
    price_cents: int | None = Field(default=None, ge=0)
    description: str | None = None
