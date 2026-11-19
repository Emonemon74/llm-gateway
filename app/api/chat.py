from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict

from app.api.deps import Auth, AuthContext
from app.limits.estimate import estimate_tokens
from app.providers.base import ProviderError
from app.routing.router import AllProvidersFailed, NoRoute, Target

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


@dataclass
class Accounting:
    """Everything that must happen when a request ends: reconcile limits, record usage."""

    state: Any
    auth: AuthContext
    request_id: str
    model: str
    estimated: int
    t0: float

    async def release(self, status: str = "error") -> None:
        """Request produced nothing: give back the TPM tokens and the budget reservation."""
        key_id = str(self.auth.api_key_id)
        await self.state.limiter.reconcile(key_id, self.auth.tpm_limit, self.estimated, 0)
        await self.state.budget.adjust(self.auth.tenant_id, -self.estimated)
        self._record(None, 0, 0, 0, False, 0.0, status)

    async def settle(self, target: Target, usage: dict | None, status: str = "ok") -> None:
        key_id = str(self.auth.api_key_id)
        if usage and usage.get("total_tokens") is not None:
            p, c = int(usage.get("prompt_tokens", 0)), int(usage.get("completion_tokens", 0))
            total, estimated_flag = int(usage["total_tokens"]), False
        else:  # provider didn't report usage: keep the up-front estimate
            p = c = 0
            total, estimated_flag = self.estimated, True
        await self.state.limiter.reconcile(key_id, self.auth.tpm_limit, self.estimated, total)
        await self.state.budget.adjust(self.auth.tenant_id, total - self.estimated)
        self._record(target, p, c, total, estimated_flag, target.cost(p, c), status)

    def _record(self, target, p, c, total, estimated_flag, cost, status) -> None:
        self.state.usage.record(
            {
                "tenant_id": self.auth.tenant_id,
                "api_key_id": self.auth.api_key_id,
                "request_id": self.request_id,
                "provider": target.provider if target else None,
                "model": self.model,
                "prompt_tokens": p,
                "completion_tokens": c,
                "total_tokens": total,
                "tokens_estimated": estimated_flag,
                "cost_usd": cost,
                "latency_ms": int((time.monotonic() - self.t0) * 1000),
                "status": status,
            }
        )


def _spawn(coro) -> None:
    t = asyncio.create_task(coro)
    _background.add(t)
    t.add_done_callback(_background.discard)


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest, request: Request, auth: AuthContext = Auth):
    state = request.app.state
    body = req.model_dump()
    request_id = uuid.uuid4().hex

    try:  # 404 before we spend any of the caller's quota
        await state.router.targets(req.model)
    except NoRoute:
        raise _error(404, "model_not_found", f"unknown model '{req.model}'") from None

    # 1. rate limit (RPM + TPM, atomic)
    estimated = estimate_tokens(body)
    key_id = str(auth.api_key_id)
    d = await state.limiter.consume(key_id, auth.rpm_limit, auth.tpm_limit, estimated)
    if d.too_large:
        raise _error(
            413, "request_too_large", "estimated tokens exceed this key's per-minute token limit"
        )
    if not d.allowed:
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
        raise _error(429, "insufficient_quota", "monthly token budget exhausted")

    acct = Accounting(state, auth, request_id, req.model, estimated, time.monotonic())
    headers = {
        "x-request-id": request_id,
        "x-ratelimit-limit-requests": str(auth.rpm_limit),
        "x-ratelimit-remaining-requests": str(d.rpm_remaining),
        "x-ratelimit-limit-tokens": str(auth.tpm_limit),
        "x-ratelimit-remaining-tokens": str(d.tpm_remaining),
    }

    # 3. route + call (failover + circuit breaking live in the Router)
    try:
        if not req.stream:
            rr = await state.router.chat(req.model, body)
        else:
            sr = await state.router.chat_stream(req.model, body)
    except ProviderError as e:  # non-retryable upstream rejection (e.g. 400): caller's problem
        await acct.release()
        raise _error(400, "upstream_rejected", e.message) from e
    except AllProvidersFailed as e:
        await acct.release()
        if e.all_skipped:
            raise _error(
                503,
                "providers_unavailable",
                "all upstream providers are unavailable",
                {"Retry-After": "5"},
            ) from e
        raise _error(502, "upstream_error", "all upstream providers failed") from e

    if not req.stream:
        await acct.settle(rr.target, rr.result.get("usage"))
        return JSONResponse(rr.result, headers=headers | {"x-gateway-provider": rr.target.provider})

    async def gen():
        try:
            async for line in state.router.relay(sr):
                yield line
        finally:
            # Runs on completion AND client disconnect; spawn so cancellation can't cut it short.
            _spawn(acct.settle(sr.target, sr.usage, "upstream_error" if sr.failed else "ok"))

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers=headers | {"x-gateway-provider": sr.target.provider},
    )
