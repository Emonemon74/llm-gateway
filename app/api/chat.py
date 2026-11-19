from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict

from app.api.deps import Auth, AuthContext
from app.limits.estimate import estimate_tokens
from app.providers.base import ProviderError

router = APIRouter()


class ChatRequest(BaseModel):
    # Accept any extra OpenAI fields (temperature, tools, ...) and pass them through.
    model_config = ConfigDict(extra="allow")
    model: str
    messages: list[dict[str, Any]]
    stream: bool = False


def _provider_for(request: Request, model: str):
    # Week 1: model alias is "<provider>/<model>" e.g. "mock/test". Week 2 replaces
    # this with the routing table + ordered failover (PLAN.md §4).
    providers = request.app.state.providers
    name, _, upstream = model.partition("/")
    if name not in providers or not upstream:
        raise HTTPException(404, f"unknown model '{model}'. Use '<provider>/<model>'.")
    return providers[name], upstream


def _rate_limit_error(d) -> HTTPException:
    if d.too_large:
        return HTTPException(
            413,
            {
                "error": {
                    "type": "request_too_large",
                    "message": "estimated tokens exceed this key's per-minute token limit",
                }
            },
        )
    what = "requests" if d.limited_by == "rpm" else "tokens"
    return HTTPException(
        429,
        {
            "error": {
                "type": "rate_limit_exceeded",
                "message": f"rate limit reached for {what} per minute; retry in {d.retry_after_s}s",
            }
        },
        headers={"Retry-After": str(d.retry_after_s)},
    )


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest, request: Request, auth: AuthContext = Auth):
    provider, upstream_model = _provider_for(request, req.model)
    body = req.model_dump() | {"model": upstream_model}

    limiter = request.app.state.limiter
    key_id = str(auth.api_key_id)
    estimated = estimate_tokens(body)
    decision = await limiter.consume(key_id, auth.rpm_limit, auth.tpm_limit, estimated)
    if not decision.allowed:
        raise _rate_limit_error(decision)
    rl_headers = {
        "x-ratelimit-limit-requests": str(auth.rpm_limit),
        "x-ratelimit-remaining-requests": str(decision.rpm_remaining),
        "x-ratelimit-limit-tokens": str(auth.tpm_limit),
        "x-ratelimit-remaining-tokens": str(decision.tpm_remaining),
    }

    if not req.stream:
        try:
            result = await provider.chat(body)
        except ProviderError as e:
            raise HTTPException(502 if e.retryable else 400, e.message) from e
        actual = (result.get("usage") or {}).get("total_tokens")
        if actual is not None:
            await limiter.reconcile(key_id, auth.tpm_limit, estimated, int(actual))
        return JSONResponse(result, headers=rl_headers)

    stream = provider.chat_stream(body)
    try:
        first = await anext(stream)  # surfaces upstream errors before we commit to 200
    except ProviderError as e:
        raise HTTPException(502 if e.retryable else 400, e.message) from e
    except StopAsyncIteration:
        first = b""

    async def gen():
        yield first
        async for line in stream:
            yield line

    # TODO(week 2): reconcile streamed usage from the final chunk.
    return StreamingResponse(gen(), media_type="text/event-stream", headers=rl_headers)
