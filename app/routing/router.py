from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import metrics as m
from app.db.models import ModelRoute
from app.providers.base import Provider, ProviderError
from app.routing.breaker import CircuitBreaker

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Target:
    provider: str
    upstream_model: str
    cost_per_1k_in: float = 0.0
    cost_per_1k_out: float = 0.0

    def cost(self, prompt_tokens: int, completion_tokens: int) -> float:
        return (
            prompt_tokens / 1000 * self.cost_per_1k_in
            + completion_tokens / 1000 * self.cost_per_1k_out
        )


class NoRoute(Exception):
    pass


class AllProvidersFailed(Exception):
    def __init__(self, attempts: list[str], all_skipped: bool, upstream_s: float = 0.0):
        super().__init__(f"all providers failed: {attempts}")
        self.upstream_s = upstream_s  # time spent waiting on upstreams before giving up
        self.attempts = attempts
        self.all_skipped = all_skipped  # nobody was even tried (every breaker open/unconfigured)


@dataclass
class RouteResult:
    result: dict[str, Any]
    target: Target
    attempts: list[str] = field(default_factory=list)
    upstream_s: float = 0.0


@dataclass
class StreamResult:
    first: bytes
    rest: AsyncIterator[bytes]
    target: Target
    upstream_s: float = 0.0  # time until the first byte arrived from the winning upstream
    usage: dict[str, Any] | None = None  # filled in as the final chunk streams past
    failed: bool = False  # upstream died mid-stream


class Router:
    """Resolves a model alias to ordered targets and executes with failover + circuit breaking.

    Failover rules (PLAN.md §4):
      * retryable upstream error (429/5xx/timeout) -> count against breaker, try next target
      * non-retryable (4xx caller error)           -> surface to the caller; upstream was healthy
      * streaming: failover is only possible BEFORE the first byte is sent to the client
    """

    def __init__(
        self,
        providers: dict[str, Provider],
        breaker: CircuitBreaker,
        sessionmaker: async_sessionmaker[AsyncSession],
        route_ttl_s: float = 5.0,
    ):
        self.providers = providers
        self.breaker = breaker
        self._sm = sessionmaker
        self._ttl = route_ttl_s
        self._routes: dict[str, list[Target]] = {}
        self._loaded_at = float("-inf")

    # ---- route table -------------------------------------------------------------------
    def invalidate(self) -> None:
        self._loaded_at = float("-inf")

    async def _refresh(self) -> None:
        if time.monotonic() - self._loaded_at < self._ttl:
            return
        async with self._sm() as s:
            rows = (
                await s.execute(
                    select(ModelRoute)
                    .where(ModelRoute.enabled.is_(True))
                    .order_by(ModelRoute.alias, ModelRoute.priority)
                )
            ).scalars()
            routes: dict[str, list[Target]] = {}
            for r in rows:
                routes.setdefault(r.alias, []).append(
                    Target(r.provider, r.upstream_model, r.cost_per_1k_in, r.cost_per_1k_out)
                )
        self._routes, self._loaded_at = routes, time.monotonic()

    async def targets(self, model: str) -> list[Target]:
        await self._refresh()
        if model in self._routes:
            return self._routes[model]
        provider, _, upstream = model.partition("/")  # direct addressing: no failover
        if upstream and provider in self.providers:
            return [Target(provider, upstream)]
        raise NoRoute(model)

    def route_label(self, model: str) -> str:
        """Bounded metrics label: the alias if it is a configured one, else 'direct'."""
        return model if model in self._routes else "direct"

    # ---- execution ---------------------------------------------------------------------
    async def _usable(self, t: Target) -> bool:
        ok = t.provider in self.providers and await self.breaker.allow(t.provider)
        if not ok:
            m.UPSTREAM_REQUESTS.labels(t.provider, "skipped").inc()
        return ok

    @staticmethod
    def _observe(provider: str, started: float, outcome: str) -> float:
        dt = time.monotonic() - started
        m.UPSTREAM_SECONDS.labels(provider).observe(dt)
        m.UPSTREAM_REQUESTS.labels(provider, outcome).inc()
        return dt

    async def chat(self, model: str, body: dict[str, Any]) -> RouteResult:
        targets = await self.targets(model)
        attempts: list[str] = []
        skipped = 0
        upstream_s = 0.0
        for t in targets:
            if not await self._usable(t):
                skipped += 1
                continue
            started = time.monotonic()
            try:
                res = await self.providers[t.provider].chat(body | {"model": t.upstream_model})
            except ProviderError as e:
                if not e.retryable:
                    upstream_s += self._observe(t.provider, started, "client_error")
                    await self.breaker.success(t.provider)  # it answered; the caller was wrong
                    e.upstream_s = upstream_s  # type: ignore[attr-defined]
                    raise
                upstream_s += self._observe(t.provider, started, "retryable_error")
                await self.breaker.failure(t.provider)
                attempts.append(f"{t.provider}:{e.status}")
                continue
            upstream_s += self._observe(t.provider, started, "ok")
            await self.breaker.success(t.provider)
            return RouteResult(res, t, attempts, upstream_s)
        raise AllProvidersFailed(attempts, skipped == len(targets), upstream_s)

    async def chat_stream(self, model: str, body: dict[str, Any]) -> StreamResult:
        targets = await self.targets(model)
        attempts: list[str] = []
        skipped = 0
        # Ask upstream to report usage on the final chunk so budgets can be reconciled.
        body = body | {
            "stream_options": {"include_usage": True} | (body.get("stream_options") or {})
        }
        upstream_s = 0.0
        for t in targets:
            if not await self._usable(t):
                skipped += 1
                continue
            started = time.monotonic()
            gen = self.providers[t.provider].chat_stream(body | {"model": t.upstream_model})
            try:
                first = await anext(gen)
            except StopAsyncIteration:
                first = b""
            except ProviderError as e:
                if not e.retryable:
                    upstream_s += self._observe(t.provider, started, "client_error")
                    await self.breaker.success(t.provider)
                    e.upstream_s = upstream_s  # type: ignore[attr-defined]
                    raise
                upstream_s += self._observe(t.provider, started, "retryable_error")
                await self.breaker.failure(t.provider)
                attempts.append(f"{t.provider}:{e.status}")
                continue
            upstream_s += self._observe(t.provider, started, "ok")
            await self.breaker.success(t.provider)
            return StreamResult(first, gen, t, upstream_s)
        raise AllProvidersFailed(attempts, skipped == len(targets), upstream_s)

    async def relay(self, res: StreamResult) -> AsyncIterator[bytes]:
        """Forward the stream, sniff usage off the final chunk, and turn a mid-stream upstream
        failure into an SSE error event (we can't fail over once bytes have been sent)."""

        def sniff(line: bytes) -> None:
            if line.startswith(b"data: ") and b'"usage"' in line:
                try:
                    u = json.loads(line[6:]).get("usage")
                except ValueError:
                    return
                if u:
                    res.usage = u

        try:
            sniff(res.first)
            yield res.first
            async for line in res.rest:
                sniff(line)
                yield line
        except (ProviderError, httpx.HTTPError) as e:
            res.failed = True
            await self.breaker.failure(res.target.provider)
            log.warning("stream from %s died mid-flight: %r", res.target.provider, e)
            err = {"error": {"type": "upstream_error", "message": "upstream failed mid-stream"}}
            yield f"data: {json.dumps(err)}\n\n".encode()
