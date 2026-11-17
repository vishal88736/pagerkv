"""HTTP calls from Node A to Node B, plus the background dirty-key retry loop.

Every network failure is converted into a result object.  The replicator never
raises into a route: a broken Node B must degrade to "dirty, keep the data",
not turn a PUT into a 500.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

if TYPE_CHECKING:  # pragma: no cover - typing only
    from node_a.state import NodeAState

logger = logging.getLogger("pagerkv.node_a.replicator")


@dataclass
class ReplicationResult:
    ok: bool
    version: int | None = None
    stub: str | None = None
    full_tokens: int | None = None
    sha256: str | None = None
    error: str | None = None
    # Set when Node B already holds a newer version (``remote_version``).
    stale: bool = False
    remote_version: int | None = None


@dataclass
class FetchResult:
    ok: bool
    version: int | None = None
    full_text: str | None = None
    full_tokens: int | None = None
    stub: str | None = None
    error: str | None = None
    not_found: bool = False


class Replicator:
    """Thin async wrapper around the Node B API using one shared client."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        timeout_s: float,
    ) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s

    async def replicate(self, key: str, version: int, text: str) -> ReplicationResult:
        try:
            response = await self.client.post(
                f"{self.base_url}/replicate",
                json={"key": key, "version": version, "full_text": text},
                timeout=self.timeout_s,
            )
        except httpx.TimeoutException:
            return ReplicationResult(
                ok=False, error=f"Node B timeout after {self.timeout_s:g}s"
            )
        except httpx.HTTPError as exc:
            return ReplicationResult(ok=False, error=f"Node B unreachable: {exc}")
        except Exception as exc:  # pragma: no cover - defensive
            return ReplicationResult(ok=False, error=f"replication failed: {exc}")

        if response.status_code != 200:
            return ReplicationResult(
                ok=False, error=f"Node B HTTP {response.status_code}"
            )

        try:
            payload = response.json()
        except ValueError:
            return ReplicationResult(ok=False, error="Node B returned invalid JSON")

        if not isinstance(payload, dict):
            return ReplicationResult(ok=False, error="Node B returned bad payload")
        if payload.get("stale_write"):
            remote = payload.get("version")
            return ReplicationResult(
                ok=False,
                error=f"Node B rejected write as stale (it holds v{remote})",
                stale=True,
                remote_version=remote if isinstance(remote, int) else None,
            )
        if payload.get("version") != version:
            return ReplicationResult(
                ok=False, error="Node B ack version mismatch"
            )
        stub = payload.get("stub")
        if not isinstance(stub, str) or not stub:
            return ReplicationResult(ok=False, error="Node B ack missing stub")

        return ReplicationResult(
            ok=True,
            version=version,
            stub=stub,
            full_tokens=payload.get("full_tokens"),
            sha256=payload.get("sha256"),
        )

    async def fetch(self, key: str) -> FetchResult:
        try:
            response = await self.client.get(
                f"{self.base_url}/archive/{quote(key, safe='')}",
                timeout=self.timeout_s,
            )
        except httpx.TimeoutException:
            return FetchResult(
                ok=False, error=f"Node B timeout after {self.timeout_s:g}s"
            )
        except httpx.HTTPError as exc:
            return FetchResult(ok=False, error=f"Node B unreachable: {exc}")
        except Exception as exc:  # pragma: no cover - defensive
            return FetchResult(ok=False, error=f"hydrate failed: {exc}")

        if response.status_code == 404:
            return FetchResult(
                ok=False, error="Node B has no archived copy", not_found=True
            )
        if response.status_code != 200:
            return FetchResult(
                ok=False, error=f"Node B HTTP {response.status_code}"
            )

        try:
            payload = response.json()
        except ValueError:
            return FetchResult(ok=False, error="Node B returned invalid JSON")
        if not isinstance(payload, dict):
            return FetchResult(ok=False, error="Node B returned bad payload")

        full_text = payload.get("full_text")
        version = payload.get("version")
        if not isinstance(full_text, str) or not isinstance(version, int):
            return FetchResult(ok=False, error="Node B archive payload incomplete")

        return FetchResult(
            ok=True,
            version=version,
            full_text=full_text,
            full_tokens=payload.get("full_tokens"),
            stub=payload.get("stub"),
        )

    async def delete(self, key: str, max_version: int) -> bool:
        """Delete ``key`` on Node B up to ``max_version``.  True once settled.

        "Settled" means Node B no longer holds a version <= ``max_version``:
        it deleted it, never had it, or holds a newer version.
        """
        try:
            response = await self.client.delete(
                f"{self.base_url}/archive/{quote(key, safe='')}",
                params={"version": max_version},
                timeout=self.timeout_s,
            )
        except Exception:
            return False
        return response.status_code == 200

    async def search(self, q: str, limit: int = 5) -> list[dict[str, Any]] | None:
        """Proxy to Node B /search.  Returns None on any error."""
        try:
            response = await self.client.get(
                f"{self.base_url}/search",
                params={"q": q, "limit": limit},
                timeout=self.timeout_s,
            )
        except httpx.TimeoutException:
            return None
        except httpx.HTTPError:
            return None
        except Exception:  # pragma: no cover - defensive
            return None
        if response.status_code != 200:
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        if not isinstance(payload, dict):
            return None
        results = payload.get("results")
        if not isinstance(results, list):
            return None
        return results

    async def health(self) -> bool:
        try:
            response = await self.client.get(
                f"{self.base_url}/health",
                timeout=min(1.0, self.timeout_s),
            )
        except Exception:
            return False
        if response.status_code != 200:
            return False
        try:
            payload = response.json()
        except ValueError:
            return False
        return bool(isinstance(payload, dict) and payload.get("ok") is True)


async def retry_loop(
    state: "NodeAState", replicator: Replicator, interval_s: float
) -> None:
    """Retry dirty keys forever; one bad iteration must never kill the loop."""
    while True:
        try:
            await asyncio.sleep(interval_s)
            await state.retry_dirty(replicator)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("background replication loop iteration failed")
