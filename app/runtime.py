from __future__ import annotations

import redis.asyncio as aioredis

from app.auth_cache import AuthCache
from app.core.config import settings
from app.db.session import make_engine, make_sessionmaker
from app.limits.budget import BudgetManager
from app.limits.rate_limiter import RateLimiter
from app.providers.registry import build_providers
from app.routing.breaker import CircuitBreaker
from app.routing.router import Router
from app.usage import UsageRecorder


class Runtime:
    """All long-lived dependencies, built once per process (API server or batch worker).

    Attribute names match what the request handlers read off `app.state`, so the API copies them
    onto its state and the worker uses this object directly.
    """

    providers: dict
    engine: object
    sessionmaker: object
    redis: aioredis.Redis
    limiter: RateLimiter
    budget: BudgetManager
    auth_cache: AuthCache
    breaker: CircuitBreaker
    router: Router
    usage: UsageRecorder

    @classmethod
    async def create(cls, *, redis_timeout_s: float = 0.25, pool_size: int = 5) -> Runtime:
        self = cls()
        self.providers = build_providers()
        self.engine = make_engine(settings.database_url, pool_size=pool_size)
        self.sessionmaker = make_sessionmaker(self.engine)
        self.redis = aioredis.from_url(
            settings.redis_url,
            socket_timeout=redis_timeout_s,
            socket_connect_timeout=redis_timeout_s,
        )
        self.limiter = RateLimiter(self.redis)
        self.budget = BudgetManager(self.redis, self.sessionmaker)
        self.auth_cache = AuthCache(self.redis)
        self.breaker = CircuitBreaker(
            self.redis,
            settings.breaker_failure_threshold,
            settings.breaker_cooldown_s,
            settings.breaker_probe_timeout_s,
        )
        self.router = Router(self.providers, self.breaker, self.sessionmaker)
        self.usage = UsageRecorder(self.sessionmaker)
        self.usage.start()
        return self

    async def close(self) -> None:
        await self.usage.stop()
        await self.redis.aclose()
        await self.engine.dispose()
