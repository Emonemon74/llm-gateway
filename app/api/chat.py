from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict

from app.api.deps import Auth, AuthContext
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


@router.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest, request: Request, auth: AuthContext = Auth):
    provider, upstream_model = _provider_for(request, req.model)
    body = req.model_dump() | {"model": upstream_model}

    if not req.stream:
        try:
            return JSONResponse(await provider.chat(body))
        except ProviderError as e:
            raise HTTPException(502 if e.retryable else 400, e.message) from e

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

    return StreamingResponse(gen(), media_type="text/event-stream")
