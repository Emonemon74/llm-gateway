"""Fake OpenAI-compatible upstream with tunable latency and failure rate.

Env: MOCK_LATENCY_MS (default 200), MOCK_ERROR_RATE (0..1, default 0),
     MOCK_ERROR_STATUS (default 500), MOCK_STREAM_CHUNKS (default 5).
Used for load tests and chaos tests; never hit real free-tier APIs for those.
"""

import asyncio
import json
import os
import random
import time

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI(title="mock-provider")

LATENCY_S = float(os.getenv("MOCK_LATENCY_MS", "200")) / 1000
ERROR_RATE = float(os.getenv("MOCK_ERROR_RATE", "0"))
ERROR_STATUS = int(os.getenv("MOCK_ERROR_STATUS", "500"))
CHUNKS = int(os.getenv("MOCK_STREAM_CHUNKS", "5"))


def _usage(body: dict) -> dict:
    prompt = sum(len(str(m.get("content", ""))) for m in body.get("messages", [])) // 4 + 1
    return {"prompt_tokens": prompt, "completion_tokens": CHUNKS, "total_tokens": prompt + CHUNKS}


@app.post("/v1/chat/completions")
async def completions(body: dict):
    await asyncio.sleep(LATENCY_S)
    if random.random() < ERROR_RATE:
        raise HTTPException(ERROR_STATUS, "mock upstream failure")
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
                    "message": {"role": "assistant", "content": " ".join(["token"] * CHUNKS)},
                }
            ],
            "usage": _usage(body),
        }

    async def gen():
        for i in range(CHUNKS):
            chunk = {
                "id": "mock-1",
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"content": f"token{i} "}}],
            }
            yield f"data: {json.dumps(chunk)}\n\n"
            await asyncio.sleep(LATENCY_S / CHUNKS)
        final = {
            "id": "mock-1",
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": _usage(body),
        }
        yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
