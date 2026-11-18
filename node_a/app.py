"""FastAPI app for Node A (L1): the bounded active context window."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
)

from common import metrics
from common.admin import TOKEN_HEADER, require_admin
from common.schemas import (
    FaultRequest,
    KeyValueRequest,
    KvPutRequest,
    KvPutResponse,
    KvValueResponse,
    NodeAHealth,
    PolicyRequest,
    PromptResponse,
    PutRequest,
    PutResponse,
    ReplicationStatusResponse,
    StatusResponse,
)
from common.validation import validate_key
from node_a import config
from node_a.replicator import Replicator, retry_loop
from node_a.state import (
    HydrateUnavailable,
    HydrateVersionMismatch,
    KeyNotFound,
    NodeAState,
)

logger = logging.getLogger("pagerkv.node_a")

ROOT = Path(__file__).resolve().parents[1]
UI_FILE = ROOT / "ui" / "index.html"
BENCH_CSV = ROOT / "bench" / "results.csv"

# Names served by static GET /context/<name> routes; a key with one of these
# names could be written but never read back, so writes are rejected.
RESERVED_KEYS = frozenset({"status", "prompt", "search"})


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    client = httpx.AsyncClient(timeout=httpx.Timeout(config.REPLICATION_TIMEOUT_S))
    replicator = Replicator(
        client=client,
        base_url=config.NODE_B_URL,
        timeout_s=config.REPLICATION_TIMEOUT_S,
    )
    state = NodeAState(
        max_window_tokens=config.MAX_WINDOW_TOKENS,
        state_file=config.NODE_A_STATE_FILE,
        index_budget_tokens=config.INDEX_BUDGET_TOKENS,
        spill_dirty=config.SPILL_DIRTY,
    )

    await state.recover_local_values(replicator)
    app.state.node_state = state
    app.state.replicator = replicator

    retry_task = asyncio.create_task(retry_loop(state, replicator, config.RETRY_INTERVAL_S))
    logger.info(
        "Node A ready: max_window=%s, node_b=%s, retry_interval=%ss",
        config.MAX_WINDOW_TOKENS,
        config.NODE_B_URL,
        config.RETRY_INTERVAL_S,
    )
    try:
        yield
    finally:
        retry_task.cancel()
        with suppress(asyncio.CancelledError):
            await retry_task
        await client.aclose()


app = FastAPI(
    title="PagerKV Node A (L1 active window)",
    version="1.0.0",
    lifespan=lifespan,
)


def _state(request: Request) -> NodeAState:
    return request.app.state.node_state


def _replicator(request: Request) -> Replicator:
    return request.app.state.replicator


def _key_or_422(key: str) -> str:
    try:
        validate_key(key)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return key


@app.get("/health", response_model=NodeAHealth)
async def health(request: Request) -> NodeAHealth:
    replicator = _replicator(request)
    reachable = await replicator.health()
    ready = await replicator.ready() if reachable else False
    return NodeAHealth(
        ok=True,
        node="A",
        node_b_reachable=reachable,
        node_b_ready=ready,
    )


# ---------------------------------------------------------------------------
# Dashboard UI and Node B fault-injection proxy (same origin, so no CORS).
# ---------------------------------------------------------------------------


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/ui")


@app.get("/ui", include_in_schema=False)
async def ui() -> FileResponse:
    return FileResponse(UI_FILE, media_type="text/html")


@app.get("/ui/bench.csv", include_in_schema=False)
async def ui_bench_csv() -> FileResponse:
    """Benchmark results for the dashboard's replay view."""
    if not BENCH_CSV.exists():
        raise HTTPException(status_code=404, detail="run bench/run_benchmark.py first")
    return FileResponse(BENCH_CSV, media_type="text/csv")


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics(request: Request) -> str:
    status = await _state(request).status(include_text=False)
    rows = status["keys"].values()

    def count(field: str, value: str) -> int:
        return sum(1 for row in rows if row[field] == value)

    return metrics.render(
        [
            (
                "pagerkv_a_window_tokens",
                "gauge",
                "Tokens in the rendered prompt.",
                status["active_window_tokens"],
            ),
            (
                "pagerkv_a_window_budget_tokens",
                "gauge",
                "MAX_WINDOW_TOKENS.",
                status["max_window_tokens"],
            ),
            (
                "pagerkv_a_over_budget",
                "gauge",
                "1 when the prompt is over budget.",
                status["over_budget"],
            ),
            (
                "pagerkv_a_page_faults_total",
                "counter",
                "Hydrations served from L2 or the local spill.",
                status["page_faults"],
            ),
            (
                "pagerkv_a_tokens_saved",
                "gauge",
                "Tokens saved by paging to Node B.",
                status["tokens_saved_by_replication"],
            ),
            (
                "pagerkv_a_tokens_spilled",
                "gauge",
                "Tokens moved to the local spill.",
                status["tokens_spilled_locally"],
            ),
            ("pagerkv_a_keys", "gauge", "Keys held by Node A.", len(status["keys"])),
            (
                "pagerkv_a_keys_resident",
                "gauge",
                "Keys resident in full.",
                count("residence", "RESIDENT_FULL"),
            ),
            (
                "pagerkv_a_keys_paged",
                "gauge",
                "Keys paged to Node B.",
                count("residence", "PAGED_TO_NODE_B"),
            ),
            (
                "pagerkv_a_keys_spilled",
                "gauge",
                "Dirty keys spilled to local disk.",
                count("residence", "SPILLED_LOCAL"),
            ),
            (
                "pagerkv_a_keys_dirty",
                "gauge",
                "Keys not yet acked by Node B.",
                count("consistency_status", "DIRTY_UNREPLICATED"),
            ),
            (
                "pagerkv_a_pending_deletes",
                "gauge",
                "Deletes not yet applied on Node B.",
                status["pending_deletes"],
            ),
            (
                "pagerkv_a_replication_attempts_total",
                "counter",
                "Node A -> Node B replication attempts.",
                status["replication_attempts"],
            ),
            (
                "pagerkv_a_replication_successes_total",
                "counter",
                "Replication attempts acknowledged by Node B.",
                status["replication_successes"],
            ),
            (
                "pagerkv_a_replication_failures_total",
                "counter",
                "Replication attempts that failed or were rejected.",
                status["replication_failures"],
            ),
        ]
    )


@app.get("/replication/status", response_model=ReplicationStatusResponse)
async def replication_status(request: Request) -> dict:
    """One aggregate answer to "is the key-value store consistent?"."""
    replicator = _replicator(request)
    reachable = await replicator.health()
    ready = await replicator.ready() if reachable else False
    state = _state(request)
    async with state._lock:
        status = state._status_unlocked(include_text=False)
        rows = list(status["keys"].values())
        counts = {
            "total": len(rows),
            "synced": sum(1 for row in rows if row["consistency_status"] == "SYNCED_ARCHIVED"),
            "dirty": sum(1 for row in rows if row["consistency_status"] == "DIRTY_UNREPLICATED"),
            "pending": sum(1 for row in rows if row["consistency_status"] == "PENDING_REPLICATION"),
            "resident": sum(1 for row in rows if row["residence"] == "RESIDENT_FULL"),
            "paged": sum(1 for row in rows if row["residence"] == "PAGED_TO_NODE_B"),
            "spilled": sum(1 for row in rows if row["residence"] == "SPILLED_LOCAL"),
        }
        remote = await replicator.snapshot()
        local = {
            key: {"value": state.full_values.get(key), "version": entry.version}
            for key, entry in state.entries.items()
        }
        differing = (
            []
            if remote is None
            else sorted(
                key
                for key in set(local) | set(remote)
                if key not in local
                or key not in remote
                or local[key]["value"] != remote[key].get("value")
                or local[key]["version"] != remote[key].get("version")
            )
        )
        unavailable_local = any(row["value"] is None for row in local.values())
        pending_work = any(
            entry.acked_version != entry.version for entry in state.entries.values()
        ) or bool(state.tombstones)
        comparison = (
            "unknown"
            if remote is None or unavailable_local
            else "pending"
            if pending_work
            else "diverged"
            if differing
            else "in_sync"
        )
    consistent = comparison == "in_sync"
    return {
        "last_replication": status["last_replication"],
        "comparison_status": comparison,
        "differing_keys": differing,
        "local_value_count": status["local_value_count"],
        "node": "A",
        "node_b_url": config.NODE_B_URL,
        "node_b_reachable": reachable,
        "node_b_ready": ready,
        "consistency": "consistent" if consistent else "degraded",
        "keys": counts,
        "replication": {
            "attempts": status["replication_attempts"],
            "succeeded": status["replication_successes"],
            "failed": status["replication_failures"],
            "last_error": status["last_replication_error"],
        },
        "pending_deletes": status["pending_deletes"],
        "active_window_tokens": status["active_window_tokens"],
        "max_window_tokens": status["max_window_tokens"],
        "over_budget": status["over_budget"],
    }


@app.get("/replication/key/{key:path}")
async def inspect_replicated_key(key: str, request: Request) -> dict:
    """Compare one real full-value copy without hydrating or modifying context."""
    _key_or_422(key)
    state = _state(request)
    async with state._lock:
        entry = state.entries.get(key)
        local = (
            None
            if entry is None
            else {
                "key": key,
                "version": entry.version,
                "value": state.full_values.get(key),
                "residence": entry.residence,
                "acked_version": entry.acked_version,
                "consistency_status": entry.consistency_status,
            }
        )
        fetched = await _replicator(request).fetch(key)
        remote = (
            {"key": key, "version": fetched.version, "value": fetched.full_text}
            if fetched.ok
            else None
        )
        unavailable = not fetched.ok and not fetched.not_found
        pending = entry is not None and entry.acked_version != entry.version
        matches = (
            local is not None
            and remote is not None
            and local["value"] == remote["value"]
            and local["version"] == remote["version"]
        )
        comparison = (
            "unknown"
            if unavailable or (local is not None and local["value"] is None)
            else "pending"
            if pending
            else "in_sync"
            if matches or (local is None and remote is None)
            else "diverged"
        )
        return {
            "key": key,
            "node_a": local,
            "node_b": remote,
            "node_b_state": "available"
            if fetched.ok
            else "missing"
            if fetched.not_found
            else "unavailable",
            "comparison_status": comparison,
            "error": fetched.error if unavailable else None,
        }


async def _proxy_fault(request: Request, method: str, body: dict | None = None):
    replicator = _replicator(request)
    headers = {}
    if TOKEN_HEADER in request.headers:
        headers[TOKEN_HEADER] = request.headers[TOKEN_HEADER]
    try:
        response = await replicator.client.request(
            method,
            f"{replicator.base_url}/admin/fault",
            json=body,
            headers=headers,
            timeout=min(2.0, replicator.timeout_s),
        )
    except httpx.HTTPError:
        return JSONResponse(status_code=503, content={"error": "Node B unreachable"})
    try:
        payload = response.json()
    except ValueError:
        payload = {"error": "Node B returned invalid JSON"}
    return JSONResponse(status_code=response.status_code, content=payload)


@app.get("/admin/node_b/fault", dependencies=[Depends(require_admin)])
async def get_node_b_fault(request: Request):
    return await _proxy_fault(request, "GET")


@app.post("/admin/node_b/fault", dependencies=[Depends(require_admin)])
async def set_node_b_fault(body: FaultRequest, request: Request):
    return await _proxy_fault(request, "POST", body.model_dump())


# ---------------------------------------------------------------------------
# Plain key-value facade for the assignment-style API.  It returns values, not
# prompt stubs: a GET hydrates from the local spill or Node B when necessary.
# ---------------------------------------------------------------------------


@app.put("/key", response_model=KvPutResponse)
async def put_named_key(body: KeyValueRequest, request: Request):
    return await kv_put(body.key, body, request)


@app.get("/keys")
async def list_values(request: Request):
    state = _state(request)
    async with state._lock:
        return {
            "node": "A",
            "keys": {
                key: {
                    "key": key,
                    "value": state.full_values.get(key),
                    "version": entry.version,
                    "local_value_available": key in state.full_values,
                }
                for key, entry in state.entries.items()
            },
        }


@app.get("/key/{key:path}")
async def get_value(key: str, request: Request):
    _key_or_422(key)
    try:
        return await _state(request).read_value(key, _replicator(request))
    except KeyNotFound:
        raise HTTPException(status_code=404, detail="Key not found") from None
    except HydrateUnavailable as exc:
        return JSONResponse(status_code=503, content={"error": exc.error})
    except HydrateVersionMismatch as exc:
        return JSONResponse(status_code=409, content={"error": str(exc)})


@app.delete("/key/{key:path}", status_code=204)
async def delete_value(key: str, request: Request):
    return await kv_delete(key, request)


@app.put("/key/{key:path}", response_model=KvPutResponse)
@app.put("/kv/{key:path}", response_model=KvPutResponse)
async def kv_put(key: str, body: KvPutRequest, request: Request) -> dict:
    _key_or_422(key)
    try:
        summary = await _state(request).put(
            key,
            body.value,
            _replicator(request),
            pinned=body.pinned,
            priority=body.priority,
        )
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"key {key!r} disappeared") from None
    return {
        "key": summary["key"],
        "version": summary["version"],
        "replicated": summary["consistency_status"] == "SYNCED_ARCHIVED",
        "consistency_status": summary["consistency_status"],
        "replication_error": summary.get("replication_error"),
    }


@app.get("/kv/{key:path}", response_model=KvValueResponse)
async def kv_get(key: str, request: Request) -> dict | JSONResponse:
    _key_or_422(key)
    try:
        hydrated = await _state(request).hydrate(key, _replicator(request))
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"no such key {key!r}") from None
    except HydrateUnavailable as exc:
        return JSONResponse(
            status_code=503,
            content={"error": exc.error, "stub": exc.stub},
        )
    except HydrateVersionMismatch as exc:
        return JSONResponse(status_code=409, content={"error": str(exc)})
    return {
        "key": hydrated["key"],
        "version": hydrated["version"],
        "value": hydrated["full_text"],
        "source": hydrated["source"],
    }


@app.delete("/kv/{key:path}", status_code=204)
async def kv_delete(key: str, request: Request) -> Response:
    _key_or_422(key)
    await _state(request).delete(key, _replicator(request))
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Static context routes MUST be registered before /context/{key:path}.
# ---------------------------------------------------------------------------


@app.get("/context/status", response_model=StatusResponse)
async def context_status(request: Request, include_text: bool = True) -> dict:
    """``include_text=false`` omits every key's ``prompt_view`` (much smaller)."""
    return await _state(request).status(include_text=include_text)


@app.get("/context/prompt", response_model=PromptResponse)
async def context_prompt(request: Request) -> dict:
    return await _state(request).prompt()


@app.get("/context/search", response_model=None)
async def context_search(
    request: Request,
    q: str = "",
    limit: int = Query(default=5, ge=1, le=50),
) -> dict | JSONResponse:
    state = _state(request)
    replicator = _replicator(request)
    remote = await replicator.search(q, limit)
    # Without Node B's full-text index, fall back to local full texts too.
    local = await state.local_search(q, include_text=remote is None)
    if remote is None:
        if local:
            return {"results": local[:limit]}
        return JSONResponse(status_code=503, content={"error": "Node B unavailable"})
    by_key: dict[str, dict] = {}
    for item in remote:
        by_key[item["key"]] = dict(item)
    for item in local:
        existing = by_key.get(item["key"])
        if existing is None or item["score"] > existing.get("score", 0):
            by_key[item["key"]] = dict(item)
        else:
            existing["score"] = max(existing.get("score", 0), item["score"])
    merged = sorted(by_key.values(), key=lambda r: (-r.get("score", 0), r["key"]))
    return {"results": merged[:limit]}


@app.put("/context/{key:path}", response_model=PutResponse)
async def put_context(key: str, body: PutRequest, request: Request) -> dict:
    _key_or_422(key)
    if key in RESERVED_KEYS:
        raise HTTPException(
            status_code=422,
            detail=f"key {key!r} is reserved (conflicts with GET /context/{key})",
        )
    try:
        return await _state(request).put(
            key,
            body.text,
            _replicator(request),
            pinned=body.pinned,
            priority=body.priority,
        )
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"key {key!r} disappeared") from None


@app.get("/context/{key:path}")
async def get_context(key: str, request: Request, hydrate: bool = False, peek: bool = False):
    """``peek=true`` reads without counting as an access (dashboards, tools)."""
    _key_or_422(key)
    state = _state(request)
    if not hydrate:
        view = await state.get_view(key, touch=not peek)
        if view is None:
            raise HTTPException(status_code=404, detail=f"no such key {key!r}")
        return view

    try:
        return await state.hydrate(key, _replicator(request))
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"no such key {key!r}") from None
    except HydrateUnavailable as exc:
        return JSONResponse(
            status_code=503,
            content={"error": exc.error, "stub": exc.stub},
        )
    except HydrateVersionMismatch as exc:
        return JSONResponse(
            status_code=409,
            content={"error": str(exc)},
        )


@app.patch("/context/{key:path}", response_model=PutResponse)
async def patch_context(key: str, body: PolicyRequest, request: Request) -> dict:
    """Pin / unpin a key or change its eviction priority."""
    _key_or_422(key)
    try:
        return await _state(request).set_policy(key, body.pinned, body.priority)
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"no such key {key!r}") from None


@app.delete("/context/{key:path}", status_code=204)
async def delete_context(key: str, request: Request) -> Response:
    """Delete locally; Node B's copy is deleted now or by the retry loop."""
    _key_or_422(key)
    await _state(request).delete(key, _replicator(request))
    return Response(status_code=204)
