from __future__ import annotations

from pathlib import Path

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

STREAM = "batch:items"  # work: entries stay until acked (so XLEN = queued + in flight)
DELAYED = "batch:delayed"  # ZSET: item id scored by due time (retries, backpressure)
DLQ = "batch:dlq"  # dead letters, kept for inspection / manual replay
GROUP = "workers"
_DIR = Path(__file__).parent


class BatchQueue:
    """Thin wrapper over Redis Streams semantics. Delivery is AT-LEAST-ONCE: a message stays in
    the consumer group's pending list until acked, and is reclaimed if its consumer dies.
    Safety against duplicates is the worker's job (idempotent completion in Postgres)."""

    def __init__(self, r: aioredis.Redis):
        self._r = r
        self._move_due = r.register_script((_DIR / "lua_move_due.lua").read_text())
        self._delay = r.register_script((_DIR / "lua_delay.lua").read_text())

    async def ensure_group(self) -> None:
        try:
            # id "0": a fresh group still consumes entries that were added before it existed
            await self._r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def enqueue(self, item_ids: list[str]) -> None:
        async with self._r.pipeline(transaction=False) as p:
            for i in item_ids:
                p.xadd(STREAM, {"item": i})
            await p.execute()

    @staticmethod
    def _parse(entries) -> list[tuple[str, str]]:
        out = []
        for msg_id, fields in entries:
            raw = fields.get(b"item") or fields.get("item")
            msg_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            out.append((msg_id, raw.decode() if isinstance(raw, bytes) else raw))
        return out

    async def read(self, consumer: str, count: int, block_ms: int) -> list[tuple[str, str]]:
        res = await self._r.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=count, block=block_ms)
        return self._parse(res[0][1]) if res else []

    async def reclaim(self, consumer: str, min_idle_ms: int, count: int) -> list[tuple[str, str]]:
        """Take over messages whose consumer died (idle in the pending list too long)."""
        res = await self._r.xautoclaim(
            STREAM, GROUP, consumer, min_idle_time=min_idle_ms, start_id="0-0", count=count
        )
        return self._parse(res[1])

    async def ack(self, msg_id: str) -> None:
        async with self._r.pipeline(transaction=False) as p:
            p.xack(STREAM, GROUP, msg_id)
            p.xdel(STREAM, msg_id)  # acked entries would otherwise pile up in memory
            await p.execute()

    async def delay(self, item_id: str, delay_s: float) -> None:
        await self._delay(keys=[DELAYED], args=[item_id, int(delay_s * 1000)])

    async def move_due(self, limit: int = 200) -> int:
        return int(await self._move_due(keys=[DELAYED, STREAM], args=[limit]))

    async def dead_letter(self, item_id: str, batch_id: str, error: str) -> None:
        await self._r.xadd(DLQ, {"item": item_id, "batch": batch_id, "error": error[:500]})

    async def depths(self) -> tuple[int, int, int]:
        async with self._r.pipeline(transaction=False) as p:
            p.xlen(STREAM)
            p.zcard(DELAYED)
            p.xlen(DLQ)
            queued, delayed, dlq = await p.execute()
        return int(queued), int(delayed), int(dlq)
