"""Real HTTP replication; network failures leave the local write available."""

from urllib.parse import quote

import httpx


class Replicator:
    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self.client = client
        self.base_url = base_url

    async def send(self, row: dict) -> dict:
        response = await self.client.put(
            f"{self.base_url}/replicate/{quote(row['key'], safe='')}",
            json={k: row[k] for k in ("value", "version", "deleted")},
        )
        if response.status_code == 409:
            payload = response.json()
            version = payload.get("version")
            if isinstance(version, int) and version >= row["version"]:
                return {"conflict": version}
        response.raise_for_status()
        payload = response.json()
        if (
            payload.get("key") != row["key"]
            or payload.get("version") != row["version"]
            or payload.get("value") != row["value"]
            or bool(payload.get("deleted")) != bool(row["deleted"])
        ):
            raise ValueError("Node B acknowledgment did not match the requested write")
        return payload

    async def snapshot(self) -> dict:
        response = await self.client.get(f"{self.base_url}/keys")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload.get("keys"), dict):
            raise ValueError("Invalid Node B snapshot")
        return payload["keys"]
