from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import UsageEvent

log = logging.getLogger(__name__)

# Atomic check-and-reserve. Returns {allowed, used_after_or_current}
_RESERVE = """
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local cost, budget = tonumber(ARGV[1]), tonumber(ARGV[2])
if used + cost > budget then return {0, used} end
return {1, redis.call('INCRBY', KEYS[1], cost)}
"""


@dataclass(frozen=True)
class BudgetDecision:
    allowed: bool
    used: int
    budget: int
    degraded: bool = False


def month_start(now: datetime | None = None) -> datetime:
    n = now or datetime.now(UTC)
    return n.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


class BudgetManager:
    """Monthly token budget per tenant: reserve before the call, reconcile after.

    Redis holds the hot counter; Postgres `usage_events` is the source of truth. If the counter
    is missing (first request of the month, or Redis lost data) it is rebuilt from Postgres.
    If Redis is down we answer from Postgres instead: slower, but a budget cap stays a cap.
    """

    def __init__(self, client: aioredis.Redis, sessionmaker: async_sessionmaker[AsyncSession]):
        self._r = client
        self._sm = sessionmaker
        self._reserve = client.register_script(_RESERVE)

    @staticmethod
    def _key(tenant_id: uuid.UUID, now: datetime | None = None) -> str:
        return f"budget:{tenant_id}:{month_start(now):%Y%m}"

    async def _db_used(self, tenant_id: uuid.UUID) -> int:
        async with self._sm() as s:
            v = await s.scalar(
                select(func.coalesce(func.sum(UsageEvent.total_tokens), 0)).where(
                    UsageEvent.tenant_id == tenant_id, UsageEvent.created_at >= month_start()
                )
            )
        return int(v or 0)

    async def _ensure_counter(self, tenant_id: uuid.UUID) -> str:
        key = self._key(tenant_id)
        if not await self._r.exists(key):
            # SET NX: if two requests race to initialise, one wins and the other is a no-op.
            await self._r.set(key, await self._db_used(tenant_id), nx=True, ex=40 * 86400)
        return key

    async def reserve(self, tenant_id: uuid.UUID, budget: int, cost: int) -> BudgetDecision:
        try:
            key = await self._ensure_counter(tenant_id)
            ok, used = await self._reserve(keys=[key], args=[cost, budget])
            return BudgetDecision(bool(int(ok)), int(used), budget)
        except (RedisError, OSError) as e:
            log.warning("budget degraded, answering from Postgres: %r", e)
            used = await self._db_used(tenant_id)
            return BudgetDecision(used + cost <= budget, used, budget, degraded=True)

    async def adjust(self, tenant_id: uuid.UUID, delta: int) -> None:
        """Reconcile (actual - estimated) or release a reservation (delta = -estimated)."""
        if delta == 0:
            return
        try:
            await self._r.incrby(self._key(tenant_id), delta)
        except (RedisError, OSError) as e:
            log.warning("budget adjust skipped (will self-heal from Postgres): %r", e)

    async def used(self, tenant_id: uuid.UUID) -> int:
        try:
            return int(await self._r.get(await self._ensure_counter(tenant_id)) or 0)
        except (RedisError, OSError):
            return await self._db_used(tenant_id)
