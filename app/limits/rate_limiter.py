from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import redis.asyncio as aioredis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)
_LUA = Path(__file__).parent / "lua"


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limited_by: str = ""  # "rpm" | "tpm"
    retry_after_s: int = 0
    too_large: bool = False  # estimated cost exceeds the TPM bucket capacity: can never succeed
    rpm_remaining: int = 0
    tpm_remaining: int = 0
    degraded: bool = False  # Redis was down; answered by the local fallback


class _LocalBucket:
    """In-process fallback used only while Redis is unreachable (fail-open, but bounded)."""

    def __init__(self) -> None:
        self._state: dict[str, tuple[float, float]] = {}

    def take(self, key: str, cap: int, per_min: int) -> tuple[bool, int]:
        now = time.monotonic()
        tokens, ts = self._state.get(key, (float(cap), now))
        tokens = min(cap, tokens + (now - ts) * per_min / 60)
        if tokens < 1:
            self._state[key] = (tokens, now)
            return False, int((1 - tokens) * 60 / per_min) + 1
        self._state[key] = (tokens - 1, now)
        return True, 0


class RateLimiter:
    """Distributed token-bucket limiter (RPM + TPM) backed by Redis Lua scripts.

    Redis outage policy (PLAN.md §4): fail OPEN, but not unbounded. Requests are admitted
    against a per-process RPM bucket until Redis recovers. Availability beats strictness here.
    """

    def __init__(self, client: aioredis.Redis):
        self._r = client
        self._consume = client.register_script((_LUA / "consume.lua").read_text())
        self._adjust = client.register_script((_LUA / "adjust.lua").read_text())
        self._local = _LocalBucket()

    @staticmethod
    def _keys(key_id: str) -> tuple[str, str]:
        return f"rl:{key_id}:rpm", f"rl:{key_id}:tpm"

    async def consume(self, key_id: str, rpm: int, tpm: int, cost: int) -> Decision:
        rk, tk = self._keys(key_id)
        try:
            status, by, retry_ms, r_left, t_left = await self._consume(
                keys=[rk, tk], args=[rpm, rpm / 60_000, tpm, tpm / 60_000, cost]
            )
        except (RedisError, OSError) as e:
            log.warning("rate limiter degraded, Redis unavailable: %r", e)
            ok, retry = self._local.take(key_id, rpm, rpm)
            return Decision(ok, "" if ok else "rpm", retry, degraded=True)
        status = int(status)
        by = by.decode() if isinstance(by, bytes) else by
        if status == 1:
            return Decision(True, rpm_remaining=int(r_left), tpm_remaining=int(t_left))
        return Decision(
            False,
            limited_by=by,
            retry_after_s=max(1, -(-int(retry_ms) // 1000)),
            too_large=status == -1,
            rpm_remaining=int(r_left),
            tpm_remaining=int(t_left),
        )

    async def reconcile(self, key_id: str, tpm: int, estimated: int, actual: int) -> None:
        """Correct the TPM bucket once real usage is known. Never raises."""
        if actual == estimated:
            return
        try:
            await self._adjust(
                keys=[self._keys(key_id)[1]], args=[tpm, tpm / 60_000, actual - estimated]
            )
        except (RedisError, OSError) as e:
            log.warning("reconcile skipped, Redis unavailable: %r", e)
