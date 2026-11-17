"""FastAPI app for Node A (L1): the bounded active context window."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
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
    PolicyRequest,
    NodeAHealth,
    PromptResponse,
    PutRequest,
    PutResponse,
    StatusResponse,
)
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

    app.state.node_state = state
    app.state.replicator = replicator

    retry_task = asyncio.create_task(
        retry_loop(state, replicator, config.RETRY_INTERVAL_S)
    )
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


@app.get("/health", response_model=NodeAHealth)
async def health(request: Request) -> NodeAHealth:
    reachable = await _replicator(request).health()
    return NodeAHealth(ok=True, node="A", node_b_reachable=reachable)


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
            ("pagerkv_a_window_tokens", "gauge", "Tokens in the rendered prompt.", status["active_window_tokens"]),
            ("pagerkv_a_window_budget_tokens", "gauge", "MAX_WINDOW_TOKENS.", status["max_window_tokens"]),
            ("pagerkv_a_over_budget", "gauge", "1 when the prompt is over budget.", status["over_budget"]),
            ("pagerkv_a_page_faults_total", "counter", "Hydrations served from L2 or the local spill.", status["page_faults"]),
            ("pagerkv_a_tokens_saved", "gauge", "Tokens saved by paging to Node B.", status["tokens_saved_by_replication"]),
            ("pagerkv_a_tokens_spilled", "gauge", "Tokens moved to the local spill.", status["tokens_spilled_locally"]),
            ("pagerkv_a_keys", "gauge", "Keys held by Node A.", len(status["keys"])),
            ("pagerkv_a_keys_resident", "gauge", "Keys resident in full.", count("residence", "RESIDENT_FULL")),
            ("pagerkv_a_keys_paged", "gauge", "Keys paged to Node B.", count("residence", "PAGED_TO_NODE_B")),
            ("pagerkv_a_keys_spilled", "gauge", "Dirty keys spilled to local disk.", count("residence", "SPILLED_LOCAL")),
            ("pagerkv_a_keys_dirty", "gauge", "Keys not yet acked by Node B.", count("consistency_status", "DIRTY_UNREPLICATED")),
            ("pagerkv_a_pending_deletes", "gauge", "Deletes not yet applied on Node B.", status["pending_deletes"]),
        ]
    )


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
    return JSONResponse(status_code=response.status_code, content=response.json())


@app.get("/admin/node_b/fault", dependencies=[Depends(require_admin)])
async def get_node_b_fault(request: Request):
    return await _proxy_fault(request, "GET")


@app.post("/admin/node_b/fault", dependencies=[Depends(require_admin)])
async def set_node_b_fault(body: FaultRequest, request: Request):
    return await _proxy_fault(request, "POST", body.model_dump())


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


@app.get("/context/search")
async def context_search(
    request: Request, q: str = "", limit: int = 5
) -> dict:
    state = _state(request)
    replicator = _replicator(request)
    remote = await replicator.search(q, limit)
    # Without Node B's full-text index, fall back to local full texts too.
    local = await state.local_search(q, include_text=remote is None)
    if remote is None:
        if local:
            return {"results": local[:limit]}
        return JSONResponse(
            status_code=503, content={"error": "Node B unavailable"}
        )
    by_key: dict[str, dict] = {}
    for item in remote:
        by_key[item["key"]] = dict(item)
    for item in local:
        existing = by_key.get(item["key"])
        if existing is None or item["score"] > existing.get("score", 0):
            by_key[item["key"]] = dict(item)
        else:
            existing["score"] = max(existing.get("score", 0), item["score"])
    merged = sorted(
        by_key.values(), key=lambda r: (-r.get("score", 0), r["key"])
    )
    return {"results": merged[:limit]}


@app.put("/context/{key:path}", response_model=PutResponse)
async def put_context(key: str, body: PutRequest, request: Request) -> dict:
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
        raise HTTPException(status_code=404, detail=f"key {key!r} disappeared")


@app.get("/context/{key:path}")
async def get_context(key: str, request: Request, hydrate: bool = False):
    state = _state(request)
    if not hydrate:
        view = await state.get_view(key)
        if view is None:
            raise HTTPException(status_code=404, detail=f"no such key {key!r}")
        return view

    try:
        return await state.hydrate(key, _replicator(request))
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"no such key {key!r}")
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
    try:
        return await _state(request).set_policy(key, body.pinned, body.priority)
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"no such key {key!r}")


@app.delete("/context/{key:path}", status_code=204)
async def delete_context(key: str, request: Request) -> Response:
    """Delete locally; Node B's copy is deleted now or by the retry loop."""
    await _state(request).delete(key, _replicator(request))
    return Response(status_code=204)
