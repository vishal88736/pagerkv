"""Access guard for fault-injection / admin endpoints on both nodes.

* If ``PAGERKV_ADMIN_TOKEN`` is set, a request must send the same value in the
  ``X-Admin-Token`` header (compared in constant time).
* Otherwise only loopback clients (127.0.0.1, ::1) are allowed, so a node bound
  to 0.0.0.0 cannot be switched off by anyone on the network.
"""

from __future__ import annotations

import hmac
import ipaddress
import os
from typing import Any

from fastapi import HTTPException, Request

TOKEN_ENV = "PAGERKV_ADMIN_TOKEN"
TOKEN_HEADER = "X-Admin-Token"


def _is_loopback(host: str | None) -> bool:
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host == "localhost"


def admin_allowed(request: Any) -> bool:
    """True when ``request`` may use admin endpoints (see module docstring)."""
    token = os.environ.get(TOKEN_ENV, "")
    if token:
        supplied = request.headers.get(TOKEN_HEADER, "")
        return hmac.compare_digest(supplied.encode(), token.encode())
    client = getattr(request, "client", None)
    return _is_loopback(getattr(client, "host", None))


async def require_admin(request: Request) -> None:
    """FastAPI dependency form of :func:`admin_allowed`."""
    if not admin_allowed(request):
        raise HTTPException(
            status_code=403,
            detail=(
                f"admin endpoints need a loopback client or a valid {TOKEN_HEADER} "
                f"header (set {TOKEN_ENV})"
            ),
        )
