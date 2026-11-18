"""REST request models: string keys and string values."""

from typing import Literal

from pydantic import BaseModel, Field

from common.validation import MAX_KEY_CHARS, MAX_TEXT_CHARS


class ValueRequest(BaseModel):
    value: str = Field(max_length=MAX_TEXT_CHARS)


class KeyValueRequest(ValueRequest):
    key: str = Field(min_length=1, max_length=MAX_KEY_CHARS)


class ReplicateRequest(ValueRequest):
    version: int = Field(ge=1)
    deleted: bool = False


class FaultRequest(BaseModel):
    mode: Literal["none", "down", "slow"]
    delay_s: float = Field(default=3, ge=0, le=10)
