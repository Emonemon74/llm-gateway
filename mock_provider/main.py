"""Fake OpenAI-compatible upstream with tunable latency and failure rate.

Env: MOCK_LATENCY_MS (default 200), MOCK_ERROR_RATE (0..1, default 0),
     MOCK_ERROR_STATUS (default 500), MOCK_STREAM_CHUNKS (default 5).
Used for load tests and chaos tests; never hit real free-tier APIs for those.
`create_mock_app(cfg)` lets tests run several independent mocks and flip `cfg` at runtime.
"""

import asyncio
import json
import os
import random
import time
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse


@dataclass
class MockConfig:
    latency_ms: float = 200
    error_rate: float = 0.0
    error_status: int = 500
    chunks: int = 5
    calls: int = field(default=0)  # how many requests reached this mock (for assertions)

    @classmethod
    def from_env(cls) -> "MockConfig":
        return cls(
            latency_ms=float(os.getenv("MOCK_LATENCY_MS", "200")),
            error_rate=float(os.getenv("MOCK_ERROR_RATE", "0")),
            error_status=int(os.getenv("MOCK_ERROR_STATUS", "500")),
            chunks=int(os.getenv("MOCK_STREAM_CHUNKS", "5")),
        )


def create_mock_app(cfg: MockConfig) -> FastAPI:
    app = FastAPI(title="mock-provider")
    app.state.cfg = cfg

    def usage(body: dict) -> dict:
        n = sum(len(str(m.get("content", ""))) for m in body.get("messages", [])) // 4 + 1
        return {"prompt_tokens": n, "completion_tokens": cfg.chunks, "total_tokens": n + cfg.chunks}

    @app.post("/v1/chat/completions")
    async def completions(body: dict):
        cfg.calls += 1
        await asyncio.sleep(cfg.latency_ms / 1000)
        if random.random() < cfg.error_rate:
            raise HTTPException(cfg.error_status, "mock upstream failure")
        model = body.get("model", "mock")
        created = int(time.time())

        if not body.get("stream"):
            return {
                "id": "mock-1",
                "object": "chat.completion",
                "created": created,
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": " ".join(["token"] * cfg.chunks),
                        },
                    }
                ],
                "usage": usage(body),
            }

        async def gen():
            for i in range(cfg.chunks):
                chunk = {
                    "id": "mock-1",
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{"index": 0, "delta": {"content": f"token{i} "}}],
                }
                yield f"data: {json.dumps(chunk)}\n\n"
                await asyncio.sleep(cfg.latency_ms / 1000 / max(cfg.chunks, 1))
            final = {
                "id": "mock-1",
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [],
                "usage": usage(body),
            }
            yield f"data: {json.dumps(final)}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/stats")
    async def stats():
        return {"calls": cfg.calls}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_mock_app(MockConfig.from_env())
