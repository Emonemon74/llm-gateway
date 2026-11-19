from __future__ import annotations

import logging
from pathlib import Path

import redis.asyncio as aioredis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)


class CircuitBreaker:
    """Per-provider circuit breaker with state in Redis so all replicas agree.

    Redis outage policy: fail open (treat as closed). A broken breaker must never take the
    gateway down; failover still works because failed calls fall through to the next target.
    """

    def __init__(
        self,
        client: aioredis.Redis,
        threshold: int = 5,
        cooldown_s: float = 30.0,
        probe_timeout_s: float = 10.0,
    ):
        self._r = client
        self._script = client.register_script(
            (Path(__file__).parent / "lua_breaker.lua").read_text()
        )
        self._args = (threshold, int(cooldown_s * 1000), int(probe_timeout_s * 1000))

    @staticmethod
    def _key(provider: str) -> str:
        return f"cb:{provider}"

    async def _run(self, provider: str, action: str) -> tuple[bool, str]:
        try:
            ok, state = await self._script(keys=[self._key(provider)], args=[action, *self._args])
        except (RedisError, OSError) as e:
            log.warning("circuit breaker degraded (fail-open): %r", e)
            return True, "unknown"
        return bool(int(ok)), state.decode() if isinstance(state, bytes) else state

    async def allow(self, provider: str) -> bool:
        return (await self._run(provider, "allow"))[0]

    async def success(self, provider: str) -> None:
        await self._run(provider, "success")

    async def failure(self, provider: str) -> str:
        return (await self._run(provider, "failure"))[1]

    async def state(self, provider: str) -> str:
        try:
            v = await self._r.hget(self._key(provider), "state")
        except (RedisError, OSError):
            return "unknown"
        return (v.decode() if isinstance(v, bytes) else v) or "closed"
