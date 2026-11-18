"""A mismatched HTTP acknowledgment must never mark a local value replicated."""

import asyncio

import httpx

from common.store import Store
from node_a.replicator import Replicator
from node_a.state import NodeAState


def test_mismatched_ack_is_failure(tmp_path):
    async def run():
        def wrong_ack(request):
            return httpx.Response(
                200, json={"key": "sample", "value": "wrong", "version": 1, "deleted": False}
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(wrong_ack)) as client:
            store = Store(str(tmp_path / "a.db"))
            try:
                state = NodeAState(store, Replicator(client, "http://node-b"))
                result = await state.write("sample", "correct")
                assert result["value"] == "correct" and result["acked_version"] == 0
                assert result["error"]
                assert store.counters() == {"attempts": 1, "succeeded": 0, "failed": 1}
            finally:
                store.close()

    asyncio.run(run())
