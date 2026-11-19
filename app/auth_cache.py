from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass

import redis.asyncio as aioredis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

POSITIVE_TTL_S = 30  # upper bound on staleness for limit/budget changes
NEGATIVE_TTL_S = 10  # absorbs floods of invalid keys without hitting Postgres each time
_INVALID = "invalid"


@dataclass(frozen=True)
class CachedKey:
    tenant_id: str
    api_key_id: str
    rpm_limit: int
    tpm_limit: int
    monthly_token_budget: int

    @property
    def tenant_uuid(self) -> uuid.UUID:
        return uuid.UUID(self.tenant_id)


class AuthCache:
    """Redis cache in front of the per-request API-key lookup (key hash -> identity + limits).

    Revocation is immediate: the admin API deletes the entry, and every replica shares this
    Redis. Any other change (limits, budget) is visible within POSITIVE_TTL_S. Redis failures
    degrade to the Postgres path; the cache never decides to deny on its own.
    """

    def __init__(self, client: aioredis.Redis):
        self._r = client

    @staticmethod
    def _key(key_hash: str) -> str:
        return f"auth:{key_hash}"

    async def get(self, key_hash: str) -> CachedKey | str | None:
        """CachedKey on a hit, "invalid" on a negative hit, None on a miss/error."""
        try:
            raw = await self._r.get(self._key(key_hash))
        except (RedisError, OSError) as e:
            log.warning("auth cache read failed, falling back to Postgres: %r", e)
            return None
        if raw is None:
            return None
        text = raw.decode() if isinstance(raw, bytes) else raw
        return _INVALID if text == _INVALID else CachedKey(**json.loads(text))

    async def put(self, key_hash: str, value: CachedKey) -> None:
        await self._set(key_hash, json.dumps(asdict(value)), POSITIVE_TTL_S)

    async def put_invalid(self, key_hash: str) -> None:
        await self._set(key_hash, _INVALID, NEGATIVE_TTL_S)

    async def invalidate(self, key_hash: str) -> None:
        try:
            await self._r.delete(self._key(key_hash))
        except (RedisError, OSError) as e:
            # Can't guarantee instant revocation now; TTL bounds the damage.
            log.error("auth cache invalidate failed (stale up to %ss): %r", POSITIVE_TTL_S, e)

    async def _set(self, key_hash: str, value: str, ttl: int) -> None:
        try:
            await self._r.set(self._key(key_hash), value, ex=ttl)
        except (RedisError, OSError) as e:
            log.warning("auth cache write failed: %r", e)
