from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import UsageEvent

log = logging.getLogger(__name__)


class UsageRecorder:
    """Writes usage events off the request path: queue -> batched INSERTs.

    The request never waits on Postgres. If the queue is full we drop and count rather than
    apply backpressure to user traffic (usage is metering, not the product).
    """

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        max_queue: int = 10_000,
        batch_size: int = 100,
        linger_s: float = 0.2,
    ):
        self._sm = sessionmaker
        self._q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(max_queue)
        self._batch, self._linger = batch_size, linger_s
        self._task: asyncio.Task | None = None
        self.dropped = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="usage-recorder")

    async def stop(self) -> None:
        await self.flush()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def record(self, event: dict[str, Any]) -> None:
        try:
            self._q.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            log.error("usage queue full, dropped event (total dropped=%d)", self.dropped)

    async def flush(self) -> None:
        await self._q.join()

    async def _run(self) -> None:
        while True:
            batch = [await self._q.get()]
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(self._linger):
                    while len(batch) < self._batch:
                        batch.append(await self._q.get())
            try:
                async with self._sm() as s:
                    s.add_all(UsageEvent(**e) for e in batch)
                    await s.commit()
            except Exception:
                log.exception("failed to persist %d usage events", len(batch))
            finally:
                for _ in batch:
                    self._q.task_done()
