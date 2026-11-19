import asyncio

import redis.asyncio as aioredis

from app.limits.rate_limiter import RateLimiter
from tests.conftest import ADMIN

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


# ---- limiter algorithm (real Redis, real Lua) -------------------------------------------
async def test_burst_then_limited(limiter):
    results = [await limiter.consume("k1", rpm=3, tpm=10_000, cost=1) for _ in range(4)]
    assert [d.allowed for d in results] == [True, True, True, False]
    denied = results[-1]
    assert denied.limited_by == "rpm" and denied.retry_after_s >= 1


async def test_refill_after_wait(limiter):
    # 60/min = 1 token/s. Drain the bucket, confirm denial, wait >1s, confirm one token is back.
    for _ in range(60):
        assert (await limiter.consume("k4", rpm=60, tpm=10**9, cost=1)).allowed
    assert not (await limiter.consume("k4", rpm=60, tpm=10**9, cost=1)).allowed
    await asyncio.sleep(1.2)
    assert (await limiter.consume("k4", rpm=60, tpm=10**9, cost=1)).allowed


async def test_tpm_denial_does_not_spend_rpm(limiter):
    assert (await limiter.consume("k5", rpm=5, tpm=100, cost=90)).allowed
    d = await limiter.consume("k5", rpm=5, tpm=100, cost=50)  # only ~10 tokens left
    assert not d.allowed and d.limited_by == "tpm"
    assert d.rpm_remaining == 4  # atomic all-or-nothing: the denied call kept its request slot


async def test_cost_above_capacity_is_too_large(limiter):
    d = await limiter.consume("k6", rpm=5, tpm=100, cost=101)
    assert not d.allowed and d.too_large


async def test_reconcile_refunds_overestimate(limiter):
    assert (await limiter.consume("k7", rpm=5, tpm=1000, cost=800)).allowed
    await limiter.reconcile("k7", tpm=1000, estimated=800, actual=100)
    d = await limiter.consume("k7", rpm=5, tpm=1000, cost=800)
    assert d.allowed  # would have been denied without the 700-token refund


async def test_keys_are_isolated(limiter):
    assert (await limiter.consume("a", rpm=1, tpm=1000, cost=1)).allowed
    assert not (await limiter.consume("a", rpm=1, tpm=1000, cost=1)).allowed
    assert (await limiter.consume("b", rpm=1, tpm=1000, cost=1)).allowed


async def test_concurrent_requests_cannot_overspend(limiter):
    """The whole point of doing this in Lua: 100 racing requests, capacity 10 -> exactly 10."""
    ds = await asyncio.gather(
        *[limiter.consume("race", rpm=10, tpm=10**6, cost=1) for _ in range(100)]
    )
    assert sum(d.allowed for d in ds) == 10


async def test_fail_open_when_redis_down():
    dead = aioredis.from_url("redis://localhost:1/0", socket_connect_timeout=0.1)
    lim = RateLimiter(dead)
    ds = [await lim.consume("x", rpm=2, tpm=1000, cost=1) for _ in range(3)]
    assert all(d.degraded for d in ds)
    assert [d.allowed for d in ds] == [True, True, False]  # open, but still bounded locally
    await dead.aclose()


# ---- HTTP behaviour ---------------------------------------------------------------------
async def _client_with_key(client, **limits):
    t = (await client.post("/admin/tenants", json={"name": "rl"}, headers=ADMIN)).json()
    k = (await client.post(f"/admin/tenants/{t['id']}/keys", json=limits, headers=ADMIN)).json()
    return {"Authorization": f"Bearer {k['key']}"}


async def test_http_429_with_retry_after_and_openai_error_shape(client):
    h = await _client_with_key(client, rpm_limit=2, tpm_limit=100_000)
    for _ in range(2):
        r = await client.post("/v1/chat/completions", json=BODY, headers=h)
        assert r.status_code == 200
        assert "x-ratelimit-remaining-requests" in r.headers
    r = await client.post("/v1/chat/completions", json=BODY, headers=h)
    assert r.status_code == 429
    assert int(r.headers["retry-after"]) >= 1
    assert r.json()["error"]["type"] == "rate_limit_exceeded"


async def test_http_413_when_request_exceeds_token_limit(client):
    h = await _client_with_key(client, rpm_limit=10, tpm_limit=50)  # default est ~257 tokens
    r = await client.post("/v1/chat/completions", json=BODY, headers=h)
    assert r.status_code == 413
    assert r.json()["error"]["type"] == "request_too_large"


async def test_http_limits_are_per_key(client):
    h1 = await _client_with_key(client, rpm_limit=1)
    t2 = (await client.post("/admin/tenants", json={"name": "other"}, headers=ADMIN)).json()
    k2 = (
        await client.post(f"/admin/tenants/{t2['id']}/keys", json={"rpm_limit": 1}, headers=ADMIN)
    ).json()
    h2 = {"Authorization": f"Bearer {k2['key']}"}
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h1)).status_code == 200
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h1)).status_code == 429
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h2)).status_code == 200
