import asyncio

import pytest

from app import metrics as m
from tests.conftest import ADMIN
from tests.integration.test_budget_usage import make_tenant_key
from tests.integration.test_failover import add_route

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


def val(name: str, **labels) -> float:
    return m.REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture
def delta():
    """Counters are process-global: assert on change since the test began."""
    snap: dict = {}

    def check(name, **labels):
        key = (name, tuple(sorted(labels.items())))
        snap.setdefault(key, val(name, **labels))
        return lambda: val(name, **labels) - snap[key]

    return check


async def test_metrics_endpoint_requires_admin_token(client):
    assert (await client.get("/metrics")).status_code == 401
    r = await client.get("/metrics", headers=ADMIN)
    assert r.status_code == 200 and "gateway_requests_total" in r.text


async def test_success_updates_request_counter_and_histograms(client, gateway, delta):
    ok = delta(
        "gateway_requests_total", route="direct", provider="mock", outcome="ok", stream="false"
    )
    lat = delta("gateway_request_duration_seconds_count", route="direct", stream="false")
    over = delta("gateway_overhead_seconds_count", stream="false")
    up = delta("gateway_upstream_duration_seconds_count", provider="mock")
    ok(), lat(), over(), up()  # take baselines
    await gateway.post("/v1/chat/completions", json=BODY)
    assert (ok(), lat(), over(), up()) == (1, 1, 1, 1)


async def test_overhead_is_less_than_total_latency(gateway):
    s0, o0 = (
        val("gateway_request_duration_seconds_sum", route="direct", stream="false"),
        val("gateway_overhead_seconds_sum", stream="false"),
    )
    await gateway.post("/v1/chat/completions", json=BODY)
    total = val("gateway_request_duration_seconds_sum", route="direct", stream="false") - s0
    overhead = val("gateway_overhead_seconds_sum", stream="false") - o0
    assert 0 <= overhead < total  # mock sleeps ~1ms upstream; overhead excludes it


async def test_tokens_and_cost_are_attributed_to_tenant(client, delta):
    tid, h = await make_tenant_key(client, budget=10_000)
    p = delta("gateway_tokens_total", tenant=tid, direction="prompt")
    c = delta("gateway_tokens_total", tenant=tid, direction="completion")
    p(), c()
    await client.post("/v1/chat/completions", json=BODY, headers=h)
    assert p() + c() == 6  # mock reports 6 total tokens


async def test_rate_limit_and_budget_rejections_are_counted(client, delta):
    rpm = delta("gateway_rate_limited_total", limit="rpm")
    bud = delta("gateway_rate_limited_total", limit="budget")
    rpm(), bud()
    _, h = await make_tenant_key(client, budget=10_000, rpm_limit=1)
    await client.post("/v1/chat/completions", json=BODY, headers=h)
    await client.post("/v1/chat/completions", json=BODY, headers=h)  # 2nd: over RPM
    _, h0 = await make_tenant_key(client, budget=0)
    await client.post("/v1/chat/completions", json=BODY, headers=h0)
    assert (rpm(), bud()) == (1, 1)


async def test_failover_is_visible_in_upstream_metrics(client, gateway, mocks, delta):
    await add_route(client, "smart", "alpha", 0)
    await add_route(client, "smart", "beta", 1)
    bad = delta("gateway_upstream_requests_total", provider="alpha", outcome="retryable_error")
    good = delta("gateway_upstream_requests_total", provider="beta", outcome="ok")
    served = delta(
        "gateway_requests_total", route="smart", provider="beta", outcome="ok", stream="false"
    )
    bad(), good(), served()
    mocks["alpha"].error_rate = 1.0
    await gateway.post("/v1/chat/completions", json=BODY | {"model": "smart"})
    assert (bad(), good(), served()) == (1, 1, 1)


async def test_circuit_state_gauge_reflects_open_breaker(client, gateway, mocks):
    await add_route(client, "smart", "alpha", 0)
    await add_route(client, "smart", "beta", 1)
    mocks["alpha"].error_rate = 1.0
    for _ in range(3):
        await gateway.post("/v1/chat/completions", json=BODY | {"model": "smart"})
    text = (await client.get("/metrics", headers=ADMIN)).text
    assert 'gateway_circuit_state{provider="alpha"} 2.0' in text
    assert 'gateway_circuit_state{provider="beta"} 0.0' in text


async def test_unbounded_model_strings_do_not_create_label_values(gateway, delta):
    a = delta(
        "gateway_requests_total", route="direct", provider="mock", outcome="ok", stream="false"
    )
    a()
    for i in range(5):
        await gateway.post("/v1/chat/completions", json=BODY | {"model": f"mock/random-{i}"})
    assert a() == 5  # all five collapsed into route="direct"
    from prometheus_client import generate_latest

    assert b"random-" not in generate_latest(m.REGISTRY)


async def test_unknown_model_counted_as_not_found(gateway, delta):
    nf = delta(
        "gateway_requests_total",
        route="unknown",
        provider="none",
        outcome="not_found",
        stream="false",
    )
    nf()
    await gateway.post("/v1/chat/completions", json=BODY | {"model": "nope"})
    assert nf() == 1


async def test_auth_failures_counted(client, delta):
    miss, inv = (
        delta("gateway_auth_failures_total", reason="missing"),
        delta("gateway_auth_failures_total", reason="invalid"),
    )
    miss(), inv()
    await client.post("/v1/chat/completions", json=BODY)
    await client.post(
        "/v1/chat/completions", json=BODY, headers={"Authorization": "Bearer sk-gw-x"}
    )
    assert (miss(), inv()) == (1, 1)


async def test_streaming_updates_metrics_and_inflight_returns_to_zero(gateway, delta):
    ok = delta(
        "gateway_requests_total", route="direct", provider="mock", outcome="ok", stream="true"
    )
    ok()
    base = val("gateway_inflight_requests")
    async with gateway.stream("POST", "/v1/chat/completions", json=BODY | {"stream": True}) as r:
        _ = [ln async for ln in r.aiter_lines()]
    await asyncio.sleep(0.1)
    assert ok() == 1
    assert val("gateway_inflight_requests") == base  # gauge doesn't leak on streams


async def test_inflight_does_not_leak_on_errors(client, gateway):
    base = val("gateway_inflight_requests")
    await gateway.post("/v1/chat/completions", json=BODY | {"model": "nope"})  # 404
    await client.post("/v1/chat/completions", json=BODY)  # 401
    assert val("gateway_inflight_requests") == base
