"""FastAPI app for Node A (L1): the bounded active context window."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from common.schemas import (
    FaultRequest,
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

UI_FILE = Path(__file__).resolve().parents[1] / "ui" / "index.html"


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


async def _proxy_fault(request: Request, method: str, body: dict | None = None):
    replicator = _replicator(request)
    try:
        response = await replicator.client.request(
            method,
            f"{replicator.base_url}/admin/fault",
            json=body,
            timeout=min(2.0, replicator.timeout_s),
        )
    except httpx.HTTPError:
        return JSONResponse(status_code=503, content={"error": "Node B unreachable"})
    return JSONResponse(status_code=response.status_code, content=response.json())


@app.get("/admin/node_b/fault")
async def get_node_b_fault(request: Request):
    return await _proxy_fault(request, "GET")


@app.post("/admin/node_b/fault")
async def set_node_b_fault(body: FaultRequest, request: Request):
    return await _proxy_fault(request, "POST", body.model_dump())


# ---------------------------------------------------------------------------
# Static context routes MUST be registered before /context/{key}.
# ---------------------------------------------------------------------------


@app.get("/context/status", response_model=StatusResponse)
async def context_status(request: Request) -> dict:
    return await _state(request).status()


@app.get("/context/prompt", response_model=PromptResponse)
async def context_prompt(request: Request) -> dict:
    return await _state(request).prompt()


@app.get("/context/search")
async def context_search(
    request: Request, q: str = "", limit: int = 5
) -> dict:
    state = _state(request)
    replicator = _replicator(request)
    local = await state.local_search(q)
    remote = await replicator.search(q, limit)
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


@app.put("/context/{key}", response_model=PutResponse)
async def put_context(key: str, body: PutRequest, request: Request) -> dict:
    try:
        return await _state(request).put(
            key, body.text, _replicator(request)
        )
    except KeyNotFound:
        raise HTTPException(status_code=404, detail=f"key {key!r} disappeared")


@app.get("/context/{key}")
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


@app.delete("/context/{key}", status_code=204)
async def delete_context(key: str, request: Request) -> Response:
    await _state(request).delete(key)
    return Response(status_code=204)
