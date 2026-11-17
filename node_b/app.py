"""FastAPI app for Node B (L2): the lossless archive.

Fault injection makes the dirty-bit guarantees of Node A testable:
  mode "none"  -> normal behaviour
  mode "down"  -> every data endpoint returns HTTP 503 (health/admin stay up)
  mode "slow"  -> every data endpoint sleeps delay_s seconds first
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Query

from common.schemas import (
    ArchiveResponse,
    FaultRequest,
    FaultResponse,
    ReplicateRequest,
    ReplicateResponse,
)
from node_b import archive

logger = logging.getLogger("pagerkv.node_b")

# Global fault-injection switch, intentionally simple.
FAULT: dict[str, object] = {"mode": "none", "delay_s": 0.0}


async def fault_guard() -> None:
    """Dependency applied to every data endpoint of Node B."""
    mode = FAULT["mode"]
    if mode == "down":
        raise HTTPException(
            status_code=503, detail="Node B is down (fault injection)"
        )
    if mode == "slow":
        delay = float(FAULT["delay_s"])  # type: ignore[arg-type]
        if delay > 0:
            await asyncio.sleep(delay)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    archive.init_db()
    logger.info("Node B archive ready at %s", archive.db_path())
    yield


app = FastAPI(
    title="PagerKV Node B (L2 archive)",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, object]:
    return {"ok": True, "node": "B"}


@app.post("/admin/fault", response_model=FaultResponse)
async def set_fault(request: FaultRequest) -> FaultResponse:
    FAULT["mode"] = request.mode
    FAULT["delay_s"] = float(request.delay_s)
    logger.warning("Fault mode set to %s (delay=%ss)", request.mode, request.delay_s)
    return FaultResponse(mode=request.mode, delay_s=float(request.delay_s))


@app.post(
    "/replicate",
    response_model=ReplicateResponse,
    dependencies=[Depends(fault_guard)],
)
async def replicate(request: ReplicateRequest) -> ReplicateResponse:
    row = archive.save(request.key, request.version, request.full_text)
    return ReplicateResponse(
        key=row["key"],
        version=row["version"],
        stub=row["stub"],
        full_tokens=row["full_tokens"],
        sha256=row["sha256"],
        stale_write=bool(row.get("stale_write", False)),
    )


@app.get(
    "/search",
    dependencies=[Depends(fault_guard)],
)
async def search(q: str = Query(default=""), limit: int = Query(default=5, ge=1, le=50)):
    return {"results": archive.search(q, limit)}


@app.get(
    "/archive/{key:path}",
    response_model=ArchiveResponse,
    dependencies=[Depends(fault_guard)],
)
async def get_archive(key: str) -> ArchiveResponse:
    row = archive.get(key)
    if row is None:
        raise HTTPException(status_code=404, detail=f"no archived key {key!r}")
    return ArchiveResponse(
        key=row["key"],
        version=row["version"],
        full_text=row["full_text"],
        full_tokens=row["full_tokens"],
        stub=row["stub"],
        sha256=row["sha256"],
    )
