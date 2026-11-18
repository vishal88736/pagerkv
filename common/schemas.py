"""Pydantic models shared by Node A and Node B."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from common.validation import MAX_KEY_CHARS, MAX_TEXT_CHARS, validate_key

FaultMode = Literal["none", "down", "slow"]


# ---------------------------------------------------------------------------
# Node B
# ---------------------------------------------------------------------------


class ReplicateRequest(BaseModel):
    key: str = Field(min_length=1, max_length=MAX_KEY_CHARS)
    version: int = Field(ge=1)
    full_text: str = Field(max_length=MAX_TEXT_CHARS)

    @field_validator("key")
    @classmethod
    def _valid_key(cls, value: str) -> str:
        validate_key(value)
        return value


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
    delay_s: float = Field(default=0.0, ge=0, le=10)


class FaultResponse(BaseModel):
    mode: FaultMode
    delay_s: float


class NodeBHealth(BaseModel):
    ok: bool
    node: Literal["B"]
    fault_mode: FaultMode = "none"
    ready: bool = True


# ---------------------------------------------------------------------------
# Node A
# ---------------------------------------------------------------------------


class PutRequest(BaseModel):
    text: str = Field(max_length=MAX_TEXT_CHARS)
    # Optional eviction policy; omitted fields keep the key's current setting.
    pinned: bool | None = None
    priority: int | None = Field(default=None, ge=-100, le=100)


class KvPutRequest(BaseModel):
    """Body for the plain key-value facade (``PUT /kv/{key}``)."""

    value: str = Field(max_length=MAX_TEXT_CHARS)
    pinned: bool | None = None
    priority: int | None = Field(default=None, ge=-100, le=100)


class KeyValueRequest(KvPutRequest):
    key: str = Field(min_length=1, max_length=MAX_KEY_CHARS)


class KvPutResponse(BaseModel):
    key: str
    version: int
    replicated: bool
    consistency_status: str
    replication_error: str | None = None


class KvValueResponse(BaseModel):
    key: str
    version: int
    value: str
    source: Literal["L1", "L1_SPILL", "L2_PAGE_FAULT"]


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
    acked_version: int = 0
    # None when /context/status is called with include_text=false.
    prompt_view: str | None


class StatusResponse(BaseModel):
    last_replication: dict[str, Any] | None = None
    local_value_count: int = 0
    active_window_tokens: int
    max_window_tokens: int
    over_budget: bool
    tokens_saved_by_replication: int
    tokens_spilled_locally: int = 0
    page_faults: int
    pending_deletes: int = 0
    omitted_from_prompt: int = 0
    index_tokens: int = 0
    consistency: Literal["consistent", "degraded"] = "consistent"
    replication_attempts: int = 0
    replication_successes: int = 0
    replication_failures: int = 0
    last_replication_error: str | None = None
    keys: dict[str, KeyStatus]


class KeyCounts(BaseModel):
    total: int
    synced: int
    dirty: int
    pending: int
    resident: int
    paged: int
    spilled: int


class ReplicationCounts(BaseModel):
    attempts: int
    succeeded: int
    failed: int
    last_error: str | None = None


class ReplicationStatusResponse(BaseModel):
    last_replication: dict[str, Any] | None = None
    comparison_status: Literal["in_sync", "pending", "diverged", "unknown"] = "unknown"
    comparison_basis: str = "Live key, version and full-value comparison"
    differing_keys: list[str] = Field(default_factory=list)
    local_value_count: int = 0
    node: Literal["A"]
    node_b_url: str
    node_b_reachable: bool
    node_b_ready: bool
    consistency: Literal["consistent", "degraded"]
    keys: KeyCounts
    replication: ReplicationCounts
    pending_deletes: int
    active_window_tokens: int
    max_window_tokens: int
    over_budget: bool


class PromptResponse(BaseModel):
    prompt: str
    tokens: int


class NodeAHealth(BaseModel):
    ok: bool
    node: Literal["A"]
    node_b_reachable: bool
    node_b_ready: bool = False
