import asyncio
import time

import pytest

from app import metrics as m
from app.shedding import LoadShedder
from tests.conftest import ADMIN

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


@pytest.fixture
async def shedder(app):
    s = LoadShedder(max_inflight=0, max_lag_s=0.0)
    s.start()
    app.state.shedder = s
    yield s
    await s.stop()


def shed_count(reason: str) -> float:
    return m.REGISTRY.get_sample_value("gateway_shed_total", {"reason": reason}) or 0.0


async def test_inflight_cap_sheds_the_excess_with_503(gateway, shedder, mocks):
    shedder.max_inflight = 2
    mocks["mock"].latency_ms = 300  # keep the first requests in flight
    before = shed_count("inflight")
    rs = await asyncio.gather(*[gateway.post("/v1/chat/completions", json=BODY) for _ in range(6)])
    codes = sorted(r.status_code for r in rs)
    assert codes == [200, 200, 503, 503, 503, 503]
    shed = next(r for r in rs if r.status_code == 503)
    assert shed.headers["retry-after"] == "1"
    assert shed.json()["error"]["type"] == "overloaded"
    assert shed_count("inflight") - before == 4
    assert shedder.inflight == 0  # every admitted request was released


async def test_shed_is_fast_and_skips_all_work(gateway, shedder, mocks):
    shedder.max_inflight = 1
    mocks["mock"].latency_ms = 500
    slow = asyncio.create_task(gateway.post("/v1/chat/completions", json=BODY))
    await asyncio.sleep(0.05)
    t = time.monotonic()
    r = await gateway.post("/v1/chat/completions", json=BODY)
    assert r.status_code == 503 and time.monotonic() - t < 0.1  # no waiting behind the slow one
    assert (await slow).status_code == 200
    assert mocks["mock"].calls == 1  # the shed request never reached the upstream


async def test_loop_lag_sheds_and_recovers(gateway, shedder):
    shedder.max_lag_s = 0.05
    shedder.lag_s = 0.5  # simulate a CPU-saturated loop
    before = shed_count("loop_lag")
    assert (await gateway.post("/v1/chat/completions", json=BODY)).status_code == 503
    assert shed_count("loop_lag") - before == 1
    shedder.lag_s = 0.0
    assert (await gateway.post("/v1/chat/completions", json=BODY)).status_code == 200


async def test_lag_monitor_detects_a_blocked_loop(shedder):
    await asyncio.sleep(0.1)
    assert shedder.lag_s < 0.05
    time.sleep(0.3)  # noqa: ASYNC251 - deliberately block the loop like CPU-bound work
    await asyncio.sleep(0.05)
    assert shedder.lag_s > 0.1


async def test_health_metrics_admin_are_never_shed(client, shedder):
    shedder.max_inflight = 1
    shedder.inflight = 10**6  # wildly over capacity
    shedder.max_lag_s, shedder.lag_s = 0.01, 10.0
    try:
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/metrics", headers=ADMIN)).status_code == 200
        assert (
            await client.post("/admin/tenants", json={"name": "x"}, headers=ADMIN)
        ).status_code == 201
    finally:
        shedder.inflight = 0


async def test_inflight_released_after_streams_and_errors(gateway, shedder, mocks):
    shedder.max_inflight = 100
    async with gateway.stream("POST", "/v1/chat/completions", json=BODY | {"stream": True}) as r:
        _ = [ln async for ln in r.aiter_lines()]
    assert shedder.inflight == 0
    await gateway.post("/v1/chat/completions", json=BODY | {"model": "nope"})  # 404
    mocks["mock"].error_rate = 1.0
    await gateway.post("/v1/chat/completions", json=BODY)  # 502
    assert shedder.inflight == 0


async def test_stream_holds_its_slot_until_the_last_chunk():
    """httpx's ASGITransport buffers whole responses, so drive the middleware directly."""
    from types import SimpleNamespace

    from app.shedding import SheddingMiddleware

    shedder = LoadShedder(max_inflight=10)
    seen: list[int] = []
    gate = asyncio.Event()

    async def streaming_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"chunk1", "more_body": True})
        await gate.wait()
        await send({"type": "http.response.body", "body": b"chunk2", "more_body": False})

    async def send(message):
        seen.append(shedder.inflight)

    scope = {
        "type": "http",
        "path": "/v1/chat/completions",
        "app": SimpleNamespace(state=SimpleNamespace(shedder=shedder)),
    }
    task = asyncio.create_task(SheddingMiddleware(streaming_app)(scope, None, send))
    await asyncio.sleep(0.05)
    assert shedder.inflight == 1  # mid-stream: slot still held
    gate.set()
    await task
    assert shedder.inflight == 0  # released exactly once, after the final chunk
    assert seen == [1, 1, 1]
