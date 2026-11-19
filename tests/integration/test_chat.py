import json

import pytest

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


async def test_healthz(gateway):
    r = await gateway.get("/healthz")
    assert r.status_code == 200


async def test_chat_non_streaming(gateway):
    r = await gateway.post("/v1/chat/completions", json=BODY)
    assert r.status_code == 200
    data = r.json()
    assert data["object"] == "chat.completion"
    assert data["model"] == "test"  # gateway strips the provider prefix upstream
    assert data["usage"]["total_tokens"] > 0


async def test_chat_streaming(gateway):
    async with gateway.stream("POST", "/v1/chat/completions", json=BODY | {"stream": True}) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        lines = [ln async for ln in r.aiter_lines() if ln.startswith("data: ")]
    assert lines[-1] == "data: [DONE]"
    chunks = [json.loads(ln[6:]) for ln in lines[:-1]]
    text = "".join(c["choices"][0]["delta"].get("content", "") for c in chunks)
    assert "token0" in text


async def test_unknown_model_is_404(gateway):
    r = await gateway.post("/v1/chat/completions", json=BODY | {"model": "nope/x"})
    assert r.status_code == 404


@pytest.mark.parametrize("stream", [False, True])
async def test_upstream_5xx_becomes_502(monkeypatch, gateway, mock_module, stream):
    monkeypatch.setattr(mock_module, "ERROR_RATE", 1.0)
    r = await gateway.post("/v1/chat/completions", json=BODY | {"stream": stream})
    assert r.status_code == 502
