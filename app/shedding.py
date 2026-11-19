"""Admission control: shed load early instead of collapsing.

The load test showed that past capacity a replica queues work inside its event loop until
clients time out, so even accepted requests fail. Shedding turns that into: serve what fits at
normal latency, reject the rest in microseconds with 503 + Retry-After (clients/LBs retry).

Two signals:
  * event-loop lag  - the direct symptom of CPU saturation in an asyncio server; independent
                      of upstream latency, so it doesn't misfire when the model is just slow.
  * in-flight cap   - hard safety limit. Not used as the primary signal because legitimate
                      concurrency = rate x upstream latency (300 req/s x 2 s model = 600).
Only /v1/* is shed. Health, metrics and admin must keep answering during overload.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time

from app import metrics as m


class LoadShedder:
    def __init__(
        self, max_inflight: int = 0, max_lag_s: float = 0.0, probe_interval_s: float = 0.02
    ):
        self.max_inflight = max_inflight  # 0 = disabled
        self.max_lag_s = max_lag_s  # 0 = disabled
        self.inflight = 0
        self.lag_s = 0.0  # smoothed event-loop lag
        self._interval = probe_interval_s
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._monitor(), name="loop-lag-monitor")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _monitor(self) -> None:
        """Sleep a fixed interval and measure how late we wake up. Late = loop is saturated."""
        while True:
            t = time.monotonic()
            await asyncio.sleep(self._interval)
            lag = max(0.0, time.monotonic() - t - self._interval)
            # rise fast (react to overload quickly), fall slower (avoid flapping)
            alpha = 0.5 if lag > self.lag_s else 0.1
            self.lag_s += alpha * (lag - self.lag_s)
            m.LOOP_LAG.set(self.lag_s)

    def reject_reason(self) -> str | None:
        if self.max_inflight and self.inflight >= self.max_inflight:
            return "inflight"
        if self.max_lag_s and self.lag_s > self.max_lag_s:
            return "loop_lag"
        return None


_BODY = json.dumps(
    {"error": {"type": "overloaded", "message": "gateway is at capacity; retry shortly"}}
).encode()


class SheddingMiddleware:
    """Pure ASGI (no BaseHTTPMiddleware): runs before routing and any Redis/DB work, and
    tracks in-flight correctly for streaming responses (until the final body chunk)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope["path"].startswith("/v1/"):
            return await self.app(scope, receive, send)
        shedder: LoadShedder | None = getattr(scope["app"].state, "shedder", None)
        if shedder is None:
            return await self.app(scope, receive, send)

        reason = shedder.reject_reason()
        if reason:
            m.SHED.labels(reason).inc()
            await send(
                {
                    "type": "http.response.start",
                    "status": 503,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"retry-after", b"1"),
                        (b"content-length", str(len(_BODY)).encode()),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": _BODY})
            return

        shedder.inflight += 1
        done = False

        async def tracked_send(message):
            nonlocal done
            await send(message)
            if (
                message["type"] == "http.response.body"
                and not message.get("more_body")
                and not done
            ):
                done = True
                shedder.inflight -= 1

        try:
            await self.app(scope, receive, tracked_send)
        finally:
            if not done:  # exception / client disconnect before the last chunk
                shedder.inflight -= 1
