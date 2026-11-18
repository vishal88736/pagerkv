"""Node B stores complete replicas and simulates slow or failed requests."""

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from common.admin import require_admin
from common.schemas import FaultRequest, ReplicateRequest
from common.store import Store
from common.validation import validate_key

FAULT = FaultRequest(mode="none", delay_s=3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = Store(os.environ.get("NODE_B_DB", "kv_node_b.db"))
    yield
    app.state.store.close()


app = FastAPI(title="PagerKV · Node B replica", lifespan=lifespan)


def check_key(key: str) -> None:
    try:
        validate_key(key)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


async def fault_guard() -> None:
    if FAULT.mode == "down":
        raise HTTPException(503, "Node B unavailable (simulated failure)")
    if FAULT.mode == "slow":
        await asyncio.sleep(FAULT.delay_s)


@app.get("/health")
async def health():
    return {"node": "B", "ok": True, "ready": FAULT.mode != "down", "fault_mode": FAULT.mode}


@app.get("/admin/fault", dependencies=[Depends(require_admin)])
async def get_fault():
    return FAULT


@app.post("/admin/fault", dependencies=[Depends(require_admin)])
async def set_fault(body: FaultRequest):
    FAULT.mode = body.mode
    FAULT.delay_s = body.delay_s
    return FAULT


@app.get("/keys", dependencies=[Depends(fault_guard)])
async def keys():
    return {"node": "B", "keys": app.state.store.rows()}


@app.get("/key/{key:path}", dependencies=[Depends(fault_guard)])
async def get_key(key: str):
    check_key(key)
    row = app.state.store.get(key)
    if not row or row["deleted"]:
        raise HTTPException(404, "Key not found")
    return {"key": key, "value": row["value"], "version": row["version"], "node": "B"}


@app.put("/replicate/{key:path}", dependencies=[Depends(fault_guard)])
async def replicate(key: str, body: ReplicateRequest):
    check_key(key)
    previous = app.state.store.get(key)
    if previous and (
        previous["version"] > body.version
        or (
            previous["version"] == body.version
            and (previous["value"] != body.value or bool(previous["deleted"]) != body.deleted)
        )
    ):
        return JSONResponse(
            status_code=409, content={"error": "Version conflict", "version": previous["version"]}
        )
    return app.state.store.save(key, body.value, body.version, body.deleted)
