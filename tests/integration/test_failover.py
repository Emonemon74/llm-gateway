import asyncio

import pytest

from tests.conftest import ADMIN

BODY = {"model": "smart", "messages": [{"role": "user", "content": "hi"}]}


async def add_route(client, alias, provider, priority, upstream="m"):
    r = await client.post(
        "/admin/routes",
        json={
            "alias": alias,
            "provider": provider,
            "upstream_model": upstream,
            "priority": priority,
        },
        headers=ADMIN,
    )
    assert r.status_code == 201


@pytest.fixture
async def routes(client):
    await add_route(client, "smart", "alpha", 0)
    await add_route(client, "smart", "beta", 1)


async def chat(gw, **extra):
    return await gw.post("/v1/chat/completions", json=BODY | extra)


async def test_alias_uses_primary_when_healthy(gateway, routes, mocks):
    r = await chat(gateway)
    assert r.status_code == 200 and r.headers["x-gateway-provider"] == "alpha"
    assert mocks["alpha"].calls == 1 and mocks["beta"].calls == 0


async def test_failover_to_secondary_when_primary_errors(gateway, routes, mocks):
    mocks["alpha"].error_rate = 1.0
    r = await chat(gateway)
    assert r.status_code == 200 and r.headers["x-gateway-provider"] == "beta"


async def test_failover_on_upstream_429(gateway, routes, mocks):
    mocks["alpha"].error_rate, mocks["alpha"].error_status = 1.0, 429
    assert (await chat(gateway)).headers["x-gateway-provider"] == "beta"


async def test_streaming_failover_before_first_byte(gateway, routes, mocks):
    mocks["alpha"].error_rate = 1.0
    async with gateway.stream("POST", "/v1/chat/completions", json=BODY | {"stream": True}) as r:
        assert r.status_code == 200 and r.headers["x-gateway-provider"] == "beta"
        body = [ln async for ln in r.aiter_lines() if ln]
    assert body[-1] == "data: [DONE]"


async def test_all_providers_failing_is_502(gateway, routes, mocks):
    mocks["alpha"].error_rate = mocks["beta"].error_rate = 1.0
    r = await chat(gateway)
    assert r.status_code == 502 and r.json()["error"]["type"] == "upstream_error"


async def test_client_error_is_not_failed_over(gateway, routes, mocks):
    mocks["alpha"].error_rate, mocks["alpha"].error_status = 1.0, 400
    r = await chat(gateway)
    assert r.status_code == 400
    assert mocks["beta"].calls == 0  # a 400 is the caller's fault: don't retry elsewhere


async def test_unknown_alias_is_404(gateway):
    assert (await chat(gateway, model="nope")).status_code == 404


# ---- circuit breaker --------------------------------------------------------------------
async def test_breaker_opens_and_skips_dead_provider(gateway, routes, mocks, app):
    mocks["alpha"].error_rate = 1.0
    for _ in range(3):  # threshold = 3 consecutive failures
        assert (await chat(gateway)).status_code == 200  # served by beta each time
    assert await app.state.breaker.state("alpha") == "open"
    calls_before = mocks["alpha"].calls
    for _ in range(5):
        assert (await chat(gateway)).headers["x-gateway-provider"] == "beta"
    assert mocks["alpha"].calls == calls_before  # circuit open: alpha is not even attempted


async def test_breaker_recovers_via_half_open_probe(gateway, routes, mocks, app):
    mocks["alpha"].error_rate = 1.0
    for _ in range(3):
        await chat(gateway)
    assert await app.state.breaker.state("alpha") == "open"
    mocks["alpha"].error_rate = 0.0  # provider heals
    await asyncio.sleep(0.5)  # cooldown (0.4s) elapses
    r = await chat(gateway)  # this request is the half-open probe
    assert r.headers["x-gateway-provider"] == "alpha"
    assert await app.state.breaker.state("alpha") == "closed"


async def test_failed_probe_reopens_circuit(gateway, routes, mocks, app):
    mocks["alpha"].error_rate = 1.0
    for _ in range(3):
        await chat(gateway)
    await asyncio.sleep(0.5)
    r = await chat(gateway)  # probe fails -> back to open, request still served by beta
    assert r.status_code == 200 and r.headers["x-gateway-provider"] == "beta"
    assert await app.state.breaker.state("alpha") == "open"


async def test_half_open_admits_only_one_probe(app):
    b = app.state.breaker
    for _ in range(3):
        await b.failure("p")
    await asyncio.sleep(0.5)
    results = await asyncio.gather(*[b.allow("p") for _ in range(20)])
    assert sum(results) == 1


async def test_all_circuits_open_is_503(gateway, routes, mocks, app):
    mocks["alpha"].error_rate = mocks["beta"].error_rate = 1.0
    for _ in range(3):
        await chat(gateway)  # 502s while both breakers trip
    r = await chat(gateway)
    assert r.status_code == 503 and r.json()["error"]["type"] == "providers_unavailable"
    assert "retry-after" in r.headers


async def test_route_change_takes_effect(client, gateway, routes, mocks):
    routes_list = (await client.get("/admin/routes", headers=ADMIN)).json()
    alpha = next(r for r in routes_list if r["provider"] == "alpha")
    await client.delete(f"/admin/routes/{alpha['id']}", headers=ADMIN)
    assert (await chat(gateway)).headers["x-gateway-provider"] == "beta"
