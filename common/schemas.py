"""Pydantic models shared by Node A and Node B."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

FaultMode = Literal["none", "down", "slow"]


# ---------------------------------------------------------------------------
# Node B
# ---------------------------------------------------------------------------


class ReplicateRequest(BaseModel):
    key: str = Field(min_length=1)
    version: int = Field(ge=1)
    full_text: str


class ReplicateResponse(BaseModel):
    key: str
    version: int
    stub: str
    full_tokens: int
    sha256: str
    stale_write: bool = False


class ArchiveResponse(BaseModel):
    key: str
    version: int
    full_text: str
    full_tokens: int
    stub: str
    sha256: str


class FaultRequest(BaseModel):
    mode: FaultMode
    delay_s: float = 0.0


class FaultResponse(BaseModel):
    mode: FaultMode
    delay_s: float


class NodeBHealth(BaseModel):
    ok: bool
    node: Literal["B"]


# ---------------------------------------------------------------------------
# Node A
# ---------------------------------------------------------------------------


class PutRequest(BaseModel):
    text: str
    # Optional eviction policy; omitted fields keep the key's current setting.
    pinned: bool | None = None
    priority: int | None = Field(default=None, ge=-100, le=100)


class PolicyRequest(BaseModel):
    pinned: bool | None = None
    priority: int | None = Field(default=None, ge=-100, le=100)


class PutResponse(BaseModel):
    key: str
    version: int
    consistency_status: str
    residence: str
    active_tokens: int
    full_tokens: int
    active_window_tokens: int
    over_budget: bool


class ContextViewResponse(BaseModel):
    key: str
    version: int
    residence: str
    consistency_status: str
    acked_version: int
    active_tokens: int
    full_tokens: int
    content: str
    is_stub: bool
    replication_error: str | None = None


class HydrateResponse(BaseModel):
    key: str
    version: int
    full_text: str
    full_tokens: int
    source: Literal["L1", "L1_SPILL", "L2_PAGE_FAULT"]


class KeyStatus(BaseModel):
    version: int
    residence: str
    consistency_status: str
    active_tokens_in_node_a: int
    full_tokens_on_node_b: int | None
    eviction_allowed: bool
    replication_error: str | None
    pinned: bool = False
    priority: int = 0
    # None when /context/status is called with include_text=false.
    prompt_view: str | None


class StatusResponse(BaseModel):
    active_window_tokens: int
    max_window_tokens: int
    over_budget: bool
    tokens_saved_by_replication: int
    tokens_spilled_locally: int = 0
    page_faults: int
    pending_deletes: int = 0
    omitted_from_prompt: int = 0
    index_tokens: int = 0
    keys: dict[str, KeyStatus]


class PromptResponse(BaseModel):
    prompt: str
    tokens: int


class NodeAHealth(BaseModel):
    ok: bool
    node: Literal["A"]
    node_b_reachable: bool
