"""Single-writer primary: keep full values, acknowledge versions, retry failures."""

import asyncio

from common.store import Store
from node_a.replicator import Replicator


class NodeAState:
    def __init__(self, store: Store, replicator: Replicator):
        self.store = store
        self.replicator = replicator
        # One process, one ordered writer, including retry and delete operations.
        self.lock = asyncio.Lock()

    async def replicate(self, row: dict) -> dict:
        for _ in range(2):
            self.store.increment("attempts")
            try:
                result = await self.replicator.send(row)
                if "conflict" in result:
                    self.store.increment("failed")
                    row = self.store.save(
                        row["key"], row["value"], result["conflict"] + 1, bool(row["deleted"])
                    )
                    self.store.result(row["key"], row["version"], "Version conflict; retry pending")
                    continue
                self.store.result(row["key"], row["version"], None)
                self.store.increment("succeeded")
                break
            except Exception as exc:
                self.store.result(row["key"], row["version"], f"{type(exc).__name__}: {exc}")
                self.store.increment("failed")
                break
        return self.store.get(row["key"]) or row

    async def write(self, key: str, value: str, deleted: bool = False) -> dict:
        async with self.lock:
            previous = self.store.get(key)
            version = previous["version"] + 1 if previous else 1
            row = self.store.save(key, value, version, deleted)
            return await self.replicate(row)

    async def retry(self) -> None:
        async with self.lock:
            for row in self.store.rows().values():
                if row["acked_version"] != row["version"]:
                    await self.replicate(row)

    async def retry_loop(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            await self.retry()
