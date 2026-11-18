"""REST primary node for a replicated string key-value store."""

import asyncio
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from common.admin import TOKEN_HEADER, require_admin
from common.schemas import FaultRequest, KeyValueRequest, ValueRequest
from common.store import Store
from common.validation import validate_key
from node_a import config
from node_a.replicator import Replicator
from node_a.state import NodeAState


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(timeout=config.REPLICATION_TIMEOUT_S) as client:
        state = NodeAState(Store(config.NODE_A_DB), Replicator(client, config.NODE_B_URL))
        app.state.node_state = state
        task = asyncio.create_task(state.retry_loop(config.RETRY_INTERVAL_S))
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            state.store.close()


app = FastAPI(title="PagerKV · REST replicated key-value store", lifespan=lifespan)


def state() -> NodeAState:
    return app.state.node_state


def check_key(key: str) -> None:
    try:
        validate_key(key)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


def write_response(row: dict) -> dict:
    replicated = row["acked_version"] == row["version"]
    return {
        **row,
        "replicated": replicated,
        "consistency_status": "synced" if replicated else "pending",
        "node": "A",
    }


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/ui")


@app.get("/ui", include_in_schema=False)
async def ui():
    return FileResponse(Path(__file__).resolve().parents[1] / "ui/index.html")


@app.get("/health")
async def health():
    return {"node": "A", "ok": True}


@app.get("/keys")
async def keys():
    return {
        "node": "A",
        "keys": {k: write_response(v) for k, v in state().store.rows().items() if not v["deleted"]},
    }


@app.put("/key")
async def put_named_key(body: KeyValueRequest):
    """Literal assignment route: PUT /key with a key and value in the body."""
    return await put_key(body.key, ValueRequest(value=body.value))


@app.put("/kv/{key:path}", include_in_schema=False)
@app.put("/key/{key:path}")
async def put_key(key: str, body: ValueRequest):
    check_key(key)
    return write_response(await state().write(key, body.value))


@app.get("/kv/{key:path}", include_in_schema=False)
@app.get("/key/{key:path}")
async def get_key(key: str):
    check_key(key)
    row = state().store.get(key)
    if not row or row["deleted"]:
        raise HTTPException(404, "Key not found")
    return write_response(row)


@app.delete("/kv/{key:path}", include_in_schema=False)
@app.delete("/key/{key:path}")
async def delete_key(key: str):
    check_key(key)
    # Tombstones make deletion retryable and stop delayed writes resurrecting keys.
    row = await state().write(key, "", deleted=True)
    return JSONResponse(
        status_code=200 if row["acked_version"] == row["version"] else 202,
        content=write_response(row),
    )


@app.get("/replication/status")
async def replication_status():
    # Hold the writer lock through comparison so a local write cannot race the snapshot.
    async with state().lock:
        local = state().store.rows()
        remote = None
        error = None
        try:
            remote = await state().replicator.snapshot()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        pending = sum(row["acked_version"] != row["version"] for row in local.values())
        matches = None
        if remote is not None:
            matches = set(local) == set(remote) and all(
                all(
                    local[k][field] == remote[k][field] for field in ("value", "version", "deleted")
                )
                for k in local
            )
        consistency = (
            "unknown"
            if remote is None
            else "in_sync"
            if matches and not pending
            else "pending"
            if pending
            else "diverged"
        )
        return {
            "consistency": consistency,
            "node_b_reachable": remote is not None,
            "comparison": "Live comparison of values, versions and deletion records",
            "pending": pending,
            "replication": state().store.counters(),
            "error": error,
            "node_a": local,
            "node_b": remote,
        }


@app.get("/admin/node_b/fault", dependencies=[Depends(require_admin)])
async def get_fault(request: Request):
    return await proxy_fault(request, "GET")


@app.post("/admin/node_b/fault", dependencies=[Depends(require_admin)])
async def set_fault(body: FaultRequest, request: Request):
    return await proxy_fault(request, "POST", body.model_dump())


async def proxy_fault(request: Request, method: str, body: dict | None = None):
    headers = (
        {TOKEN_HEADER: request.headers[TOKEN_HEADER]} if TOKEN_HEADER in request.headers else {}
    )
    try:
        response = await state().replicator.client.request(
            method, f"{config.NODE_B_URL}/admin/fault", json=body, headers=headers
        )
        return JSONResponse(status_code=response.status_code, content=response.json())
    except (httpx.HTTPError, ValueError):
        return JSONResponse(status_code=503, content={"error": "Node B unavailable"})
