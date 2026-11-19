"""Batch worker: Redis Streams consumer with retries, DLQ, crash recovery and reconciliation.

Run standalone:  uv run python -m app.batch.worker

Guarantees (see docs/PLAN.md section 5):
  * at-least-once delivery  - a message is only acked after its outcome is durable in Postgres;
    a dead worker's messages are reclaimed (XAUTOCLAIM) after the visibility timeout.
  * effectively-once RESULTS - completion is a conditional UPDATE (status still not terminal), so
    duplicate deliveries cannot double-write or double-count.
  * the upstream call itself may be repeated for work that was in flight when a worker died.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
import uuid
from dataclasses import dataclass
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy import select, text, update

from app import metrics as m
from app.accounting import Accounting
from app.api.deps import AuthContext
from app.batch.queue import BatchQueue
from app.batch.retry import backoff_s
from app.core.config import settings
from app.db.models import ApiKey, Batch, BatchItem, Tenant
from app.limits.estimate import estimate_tokens
from app.providers.base import ProviderError
from app.routing.router import AllProvidersFailed, NoRoute
from app.runtime import Runtime

log = logging.getLogger("batch.worker")

_DONE_SQL = """
UPDATE batches SET
  {col} = {col} + 1,
  status = CASE WHEN completed + failed + 1 >= total
                THEN CASE WHEN {fail_after} = 0 THEN 'completed' ELSE 'completed_with_errors' END
                ELSE status END,
  finished_at = CASE WHEN completed + failed + 1 >= total THEN now() ELSE finished_at END
WHERE id = :b
"""
# `fail_after` = failures once this item is counted, so a failed item flips status correctly
_BUMP = {
    "completed": _DONE_SQL.format(col="completed", fail_after="failed"),
    "failed": _DONE_SQL.format(col="failed", fail_after="failed + 1"),
}


@dataclass
class _Claim:
    item_id: uuid.UUID
    batch_id: uuid.UUID
    attempts: int
    max_attempts: int
    body: dict[str, Any]
    auth: AuthContext | None


class Worker:
    def __init__(
        self,
        rt: Any,
        consumer: str | None = None,
        *,
        concurrency: int | None = None,
        visibility_timeout_s: float | None = None,
        backoff_base_s: float | None = None,
        backoff_cap_s: float | None = None,
        reconcile_after_s: float | None = None,
        batch_share: float | None = None,
        unavailable_delay_s: float = 5.0,
        block_ms: int = 500,
        delayed_every_s: float = 0.25,
        reclaim_every_s: float = 2.0,
        reconcile_every_s: float | None = None,
    ):
        self.rt = rt
        self.q = BatchQueue(rt.redis)
        self.consumer = consumer or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency or settings.batch_concurrency
        self.visibility_s = visibility_timeout_s or settings.batch_visibility_timeout_s
        self.backoff_base = (
            backoff_base_s if backoff_base_s is not None else settings.batch_backoff_base_s
        )
        self.backoff_cap = (
            backoff_cap_s if backoff_cap_s is not None else settings.batch_backoff_cap_s
        )
        self.reconcile_after_s = (
            reconcile_after_s if reconcile_after_s is not None else settings.batch_reconcile_after_s
        )
        self.share = batch_share if batch_share is not None else settings.batch_rate_share
        self.unavailable_delay_s = unavailable_delay_s
        self.block_ms = block_ms
        self.delayed_every_s = delayed_every_s
        self.reclaim_every_s = reclaim_every_s
        self.reconcile_every_s = (
            reconcile_every_s if reconcile_every_s is not None else settings.batch_reconcile_every_s
        )
        self._inflight: set[asyncio.Task] = set()

    # ---- lifecycle -------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event) -> None:
        await self.q.ensure_group()
        log.info("worker %s up (concurrency=%d)", self.consumer, self.concurrency)
        await asyncio.gather(
            self._guard(self._read_loop, stop),
            self._guard(self._delayed_loop, stop),
            self._guard(self._reclaim_loop, stop),
            self._guard(self._reconcile_loop, stop),
        )
        if self._inflight:  # graceful: finish what we started, then exit
            await asyncio.gather(*self._inflight, return_exceptions=True)
        log.info("worker %s stopped", self.consumer)

    async def _guard(self, loop_fn, stop: asyncio.Event) -> None:
        """A transient Redis/DB error must not kill a loop forever; back off and continue."""
        while not stop.is_set():
            try:
                await loop_fn(stop)
                return
            except (RedisError, OSError) as e:
                log.warning("%s: infrastructure error, retrying: %r", loop_fn.__name__, e)
                if "NOGROUP" in str(e):  # Redis lost its data (flush/failover): recreate the group
                    with contextlib.suppress(RedisError, OSError):
                        await self.q.ensure_group()
                await self._sleep(stop, 1.0)
            except Exception:
                log.exception("%s crashed, restarting", loop_fn.__name__)
                await self._sleep(stop, 1.0)

    @staticmethod
    async def _sleep(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), seconds)

    def _free(self) -> int:
        return self.concurrency - len(self._inflight)

    def _spawn(self, msg_id: str, item_id: str) -> None:
        t = asyncio.create_task(self._handle(msg_id, item_id))
        self._inflight.add(t)
        t.add_done_callback(self._inflight.discard)

    # ---- loops -----------------------------------------------------------------------------
    async def _read_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            free = self._free()
            if free <= 0:
                await asyncio.sleep(0.01)
                continue
            for msg_id, item_id in await self.q.read(self.consumer, free, self.block_ms):
                self._spawn(msg_id, item_id)

    async def _delayed_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.q.move_due()
            await self._sleep(stop, self.delayed_every_s)

    async def _reclaim_loop(self, stop: asyncio.Event) -> None:
        """Adopt messages from consumers that died holding them."""
        while not stop.is_set():
            free = self._free()
            if free > 0:
                for msg_id, item_id in await self.q.reclaim(
                    self.consumer, int(self.visibility_s * 1000), free
                ):
                    log.warning("reclaimed %s from a dead/slow consumer", item_id)
                    self._spawn(msg_id, item_id)
            await self._sleep(stop, self.reclaim_every_s)

    async def _reconcile_loop(self, stop: asyncio.Event) -> None:
        """Postgres is the source of truth. Re-enqueue unfinished items Redis has forgotten
        (crash between insert and enqueue, or Redis lost its data). Duplicates are harmless."""
        while not stop.is_set():
            await self.reconcile_once()
            await self._sleep(stop, self.reconcile_every_s)

    async def reconcile_once(self) -> int:
        async with self.rt.sessionmaker() as s, s.begin():
            rows = (
                (
                    await s.execute(
                        text(
                            """
                        SELECT id FROM batch_items
                        WHERE status IN ('pending', 'in_progress')
                          AND coalesce(enqueued_at, created_at) < now() - make_interval(secs => :age)
                        ORDER BY coalesce(enqueued_at, created_at)
                        LIMIT 500
                        FOR UPDATE SKIP LOCKED
                        """
                        ),
                        {"age": self.reconcile_after_s},
                    )
                )
                .scalars()
                .all()
            )
            if not rows:
                return 0
            await s.execute(
                update(BatchItem).where(BatchItem.id.in_(rows)).values(enqueued_at=text("now()"))
            )
        await self.q.enqueue([str(r) for r in rows])
        log.warning("reconciler re-enqueued %d item(s) missing from Redis", len(rows))
        return len(rows)

    # ---- one message -----------------------------------------------------------------------
    async def _handle(self, msg_id: str, item_id: str) -> None:
        try:
            await self.process(uuid.UUID(item_id))
        except Exception:
            # Not acked: it stays pending and is reclaimed after the visibility timeout.
            log.exception("processing %s failed unexpectedly; leaving unacked", item_id)
            return
        await self.q.ack(msg_id)

    async def _claim(self, item_id: uuid.UUID) -> _Claim | None:
        """Mark one attempt started. None => nothing to do (gone, or already terminal)."""
        async with self.rt.sessionmaker() as s, s.begin():
            item = await s.get(BatchItem, item_id, with_for_update=True)
            if item is None or item.status in ("done", "failed"):
                m.BATCH_ITEMS.labels("duplicate").inc()
                return None
            batch = await s.get(Batch, item.batch_id)
            row = (
                await s.execute(
                    select(ApiKey, Tenant.monthly_token_budget)
                    .join(Tenant, Tenant.id == ApiKey.tenant_id)
                    .where(ApiKey.id == batch.api_key_id)
                )
            ).one_or_none()
            auth = None
            if row and row[0].revoked_at is None:
                k, budget = row
                auth = AuthContext(k.tenant_id, k.id, k.rpm_limit, k.tpm_limit, budget)
            item.attempts += 1
            item.status = "in_progress"
            return _Claim(
                item.id,
                item.batch_id,
                item.attempts,
                batch.max_attempts,
                item.request_json["body"],
                auth,
            )

    async def process(self, item_id: uuid.UUID) -> None:
        c = await self._claim(item_id)
        if c is None:
            return
        if c.auth is None:
            return await self._fail(c, "api key revoked or deleted")
        if c.attempts > c.max_attempts:  # crash-looping "poison" item: stop retrying it forever
            return await self._fail(c, f"exceeded {c.max_attempts} attempts (worker crashes?)")

        auth = c.auth
        key_id = f"{auth.api_key_id}:batch"  # own bucket: batch can't starve interactive traffic
        rpm = max(1, int(auth.rpm_limit * self.share))
        tpm = max(1, int(auth.tpm_limit * self.share))
        est = estimate_tokens(c.body)

        d = await self.rt.limiter.consume(key_id, rpm, tpm, est)
        if d.too_large:
            return await self._fail(c, "estimated tokens exceed the key's batch token limit")
        if not d.allowed:  # backpressure, not a failure: wait it out without burning an attempt
            await self._defer(c, d.retry_after_s)
            return

        b = await self.rt.budget.reserve(auth.tenant_id, auth.monthly_token_budget, est)
        acct = Accounting(
            self.rt,
            auth,
            f"batch-{c.item_id.hex[:12]}",
            c.body["model"],
            est,
            _now(),
            bucket_key=key_id,
            bucket_tpm=tpm,
        )
        if not b.allowed:
            await self.rt.limiter.reconcile(key_id, tpm, est, 0)
            return await self._fail(c, "monthly token budget exhausted")

        try:
            rr = await self.rt.router.chat(c.body["model"], c.body)
        except NoRoute:
            await acct.release()
            return await self._fail(c, f"unknown model '{c.body['model']}'")
        except ProviderError as e:  # non-retryable upstream rejection (4xx): retrying can't help
            await acct.release()
            return await self._fail(c, f"upstream rejected request: {e.message[:200]}")
        except AllProvidersFailed as e:
            await acct.release()
            if e.all_skipped:  # every circuit is open: an outage, not this item's fault
                return await self._defer(c, self.unavailable_delay_s)  # don't burn an attempt
            return await self._retry_or_dead_letter(c, f"all providers failed: {e.attempts}")
        except Exception as e:
            await acct.release()
            log.exception("unexpected error calling upstream for %s", c.item_id)
            return await self._retry_or_dead_letter(c, f"internal error: {e!r}")

        await acct.settle(rr.target, rr.result.get("usage"))
        await self._complete(c, rr.result)

    # ---- outcomes (each is one durable Postgres write, THEN any Redis side effect) -----------
    async def _complete(self, c: _Claim, response: dict[str, Any]) -> None:
        async with self.rt.sessionmaker() as s, s.begin():
            won = (
                await s.execute(
                    update(BatchItem)
                    .where(
                        BatchItem.id == c.item_id, BatchItem.status.in_(("pending", "in_progress"))
                    )
                    .values(status="done", response_json=response, last_error=None)
                    .returning(BatchItem.id)
                )
            ).first()
            if won:  # only the writer that flipped the status updates the batch counters
                await s.execute(text(_BUMP["completed"]), {"b": c.batch_id})
        m.BATCH_ITEMS.labels("done" if won else "duplicate").inc()

    async def _fail(self, c: _Claim, error: str) -> None:
        async with self.rt.sessionmaker() as s, s.begin():
            won = (
                await s.execute(
                    update(BatchItem)
                    .where(
                        BatchItem.id == c.item_id, BatchItem.status.in_(("pending", "in_progress"))
                    )
                    .values(status="failed", last_error=error)
                    .returning(BatchItem.id)
                )
            ).first()
            if won:
                await s.execute(text(_BUMP["failed"]), {"b": c.batch_id})
        if won:
            await self.q.dead_letter(str(c.item_id), str(c.batch_id), error)
            m.BATCH_ITEMS.labels("failed").inc()

    async def _retry_or_dead_letter(self, c: _Claim, error: str) -> None:
        if c.attempts >= c.max_attempts:
            return await self._fail(c, f"gave up after {c.attempts} attempts: {error}")
        delay = backoff_s(c.attempts, self.backoff_base, self.backoff_cap)
        await self._requeue(c, delay, error, refund_attempt=False)
        m.BATCH_ITEMS.labels("retry").inc()

    async def _defer(self, c: _Claim, delay_s: float) -> None:
        await self._requeue(c, delay_s, None, refund_attempt=True)
        m.BATCH_ITEMS.labels("deferred").inc()

    async def _requeue(
        self, c: _Claim, delay_s: float, error: str | None, *, refund_attempt: bool
    ) -> None:
        async with self.rt.sessionmaker() as s, s.begin():
            await s.execute(
                update(BatchItem)
                .where(BatchItem.id == c.item_id, BatchItem.status == "in_progress")
                .values(
                    status="pending",
                    enqueued_at=text("now()"),
                    attempts=BatchItem.attempts - (1 if refund_attempt else 0),
                    last_error=error if error is not None else BatchItem.last_error,
                )
            )
        await self.q.delay(str(c.item_id), delay_s)


def _now() -> float:
    import time

    return time.monotonic()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if settings.worker_metrics_port:
        from prometheus_client import start_http_server

        start_http_server(settings.worker_metrics_port, registry=m.REGISTRY)
    # BLOCK reads hold a connection for block_ms, so the socket timeout must exceed it
    rt = await Runtime.create(redis_timeout_s=15.0, pool_size=settings.batch_concurrency + 4)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await Worker(rt).run(stop)
    finally:
        await rt.close()


if __name__ == "__main__":
    asyncio.run(main())
