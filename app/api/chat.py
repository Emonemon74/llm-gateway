from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict

from app import metrics as m
from app.accounting import Accounting
from app.api.deps import Auth, AuthContext
from app.limits.estimate import estimate_tokens
from app.providers.base import ProviderError
from app.routing.router import AllProvidersFailed, NoRoute

router = APIRouter()
_background: set[asyncio.Task] = set()  # keep refs so fire-and-forget tasks aren't GC'd


class ChatRequest(BaseModel):
    # Accept any extra OpenAI fields (temperature, tools, ...) and pass them through.
    model_config = ConfigDict(extra="allow")
    model: str
    messages: list[dict[str, Any]]
    stream: bool = False


def _error(status: int, type_: str, message: str, headers: dict[str, str] | None = None):
    return HTTPException(status, {"error": {"type": type_, "message": message}}, headers=headers)


def _spawn(coro) -> None:
    t = asyncio.create_task(coro)
    _background.add(t)
    t.add_done_callback(_background.discard)


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest, request: Request, auth: AuthContext = Auth):
    state = request.app.state
    body = req.model_dump()
    request_id = uuid.uuid4().hex
    stream = req.stream
    t0 = getattr(request.state, "t0", time.monotonic())  # set when auth started
    label = "unknown"

    def count(outcome: str, provider: str = "none") -> None:
        m.REQUESTS.labels(label, provider, outcome, str(stream).lower()).inc()

    def first_byte(upstream_s: float) -> None:
        total = time.monotonic() - t0
        m.REQUEST_SECONDS.labels(label, str(stream).lower()).observe(total)
        m.OVERHEAD_SECONDS.labels(str(stream).lower()).observe(max(0.0, total - upstream_s))

    m.INFLIGHT.inc()
    handed_to_stream = False
    try:
        try:  # 404 before we spend any of the caller's quota
            await state.router.targets(req.model)
        except NoRoute:
            count("not_found")
            raise _error(404, "model_not_found", f"unknown model '{req.model}'") from None
        label = state.router.route_label(req.model)

        # 1. rate limit (RPM + TPM, atomic)
        estimated = estimate_tokens(body)
        key_id = str(auth.api_key_id)
        d = await state.limiter.consume(key_id, auth.rpm_limit, auth.tpm_limit, estimated)
        if d.too_large:
            m.RATE_LIMITED.labels("too_large").inc()
            count("too_large")
            raise _error(
                413,
                "request_too_large",
                "estimated tokens exceed this key's per-minute token limit",
            )
        if not d.allowed:
            m.RATE_LIMITED.labels(d.limited_by).inc()
            count("rate_limited")
            what = "requests" if d.limited_by == "rpm" else "tokens"
            raise _error(
                429,
                "rate_limit_exceeded",
                f"rate limit reached for {what} per minute; retry in {d.retry_after_s}s",
                {"Retry-After": str(d.retry_after_s)},
            )

        # 2. monthly budget: reserve the estimate now, reconcile with real usage after
        b = await state.budget.reserve(auth.tenant_id, auth.monthly_token_budget, estimated)
        if not b.allowed:
            await state.limiter.reconcile(key_id, auth.tpm_limit, estimated, 0)
            m.RATE_LIMITED.labels("budget").inc()
            count("budget_exceeded")
            raise _error(429, "insufficient_quota", "monthly token budget exhausted")

        acct = Accounting(state, auth, request_id, req.model, estimated, t0)
        headers = {
            "x-request-id": request_id,
            "x-ratelimit-limit-requests": str(auth.rpm_limit),
            "x-ratelimit-remaining-requests": str(d.rpm_remaining),
            "x-ratelimit-limit-tokens": str(auth.tpm_limit),
            "x-ratelimit-remaining-tokens": str(d.tpm_remaining),
        }

        # 3. route + call (failover + circuit breaking live in the Router)
        try:
            if not stream:
                rr = await state.router.chat(req.model, body)
            else:
                sr = await state.router.chat_stream(req.model, body)
        except ProviderError as e:  # non-retryable upstream rejection (e.g. 400)
            await acct.release()
            first_byte(getattr(e, "upstream_s", 0.0))
            count("client_error")
            raise _error(400, "upstream_rejected", e.message) from e
        except AllProvidersFailed as e:
            await acct.release()
            first_byte(e.upstream_s)
            if e.all_skipped:
                count("unavailable")
                raise _error(
                    503,
                    "providers_unavailable",
                    "all upstream providers are unavailable",
                    {"Retry-After": "5"},
                ) from e
            count("upstream_error")
            raise _error(502, "upstream_error", "all upstream providers failed") from e

        if not stream:
            await acct.settle(rr.target, rr.result.get("usage"))
            first_byte(rr.upstream_s)
            count("ok", rr.target.provider)
            return JSONResponse(
                rr.result, headers=headers | {"x-gateway-provider": rr.target.provider}
            )

        first_byte(sr.upstream_s)
        handed_to_stream = True

        async def gen():
            try:
                async for line in state.router.relay(sr):
                    yield line
            finally:
                # Runs on completion AND client disconnect; spawn so cancellation can't cut it short.
                _spawn(acct.settle(sr.target, sr.usage, "upstream_error" if sr.failed else "ok"))
                count("upstream_error" if sr.failed else "ok", sr.target.provider)
                m.INFLIGHT.dec()

        return StreamingResponse(
            gen(),
            media_type="text/event-stream",
            headers=headers | {"x-gateway-provider": sr.target.provider},
        )
    finally:
        if not handed_to_stream:
            m.INFLIGHT.dec()
