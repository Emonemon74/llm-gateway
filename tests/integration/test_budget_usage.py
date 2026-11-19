import uuid

from sqlalchemy import text

from tests.conftest import ADMIN

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


async def make_tenant_key(client, budget, name=None, **key):
    t = (
        await client.post(
            "/admin/tenants",
            json={"name": name or f"t-{uuid.uuid4().hex[:6]}", "monthly_token_budget": budget},
            headers=ADMIN,
        )
    ).json()
    k = (await client.post(f"/admin/tenants/{t['id']}/keys", json=key, headers=ADMIN)).json()
    return t["id"], {"Authorization": f"Bearer {k['key']}"}


async def usage(client, tid):
    return (await client.get(f"/admin/tenants/{tid}/usage", headers=ADMIN)).json()


async def test_budget_exhausted_returns_429_insufficient_quota(client):
    # default estimate is ~257 tokens, so a 300-token budget fits one request, not two
    _, h = await make_tenant_key(client, budget=300)
    # first request reserves ~257, then reconciles down to real usage (6 tokens)
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 200
    # tiny real usage means more requests fit: budget is about actual tokens, not estimates
    for _ in range(40):
        r = await client.post("/v1/chat/completions", json=BODY, headers=h)
        if r.status_code != 200:
            break
    assert r.status_code == 429 and r.json()["error"]["type"] == "insufficient_quota"


async def test_zero_budget_blocks_everything(client):
    _, h = await make_tenant_key(client, budget=0)
    r = await client.post("/v1/chat/completions", json=BODY, headers=h)
    assert r.status_code == 429 and r.json()["error"]["type"] == "insufficient_quota"


async def test_reservation_is_reconciled_to_actual_usage(client, app):
    tid, h = await make_tenant_key(client, budget=10_000)
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 200
    u = await usage(client, tid)
    # estimate was ~257 but the mock reports 6 real tokens: counter must hold the real figure
    assert u["used_tokens"] == 6


async def test_failed_request_releases_reservation(client, mocks):
    tid, h = await make_tenant_key(client, budget=10_000)
    mocks["mock"].error_rate = 1.0
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 502
    assert (await usage(client, tid))["used_tokens"] == 0  # nothing was consumed


async def test_budget_is_per_tenant(client):
    _, h0 = await make_tenant_key(client, budget=0)
    _, h1 = await make_tenant_key(client, budget=10_000)
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h0)).status_code == 429
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h1)).status_code == 200


async def test_concurrent_requests_cannot_overspend_budget(client):
    # est ~257/request, budget 1000 -> at most 3 reservations can be in flight; with real usage
    # of 6 tokens they settle quickly, so just assert the invariant: used never exceeds budget.
    tid, h = await make_tenant_key(client, budget=1000, rpm_limit=1000)
    import asyncio

    rs = await asyncio.gather(
        *[client.post("/v1/chat/completions", json=BODY, headers=h) for _ in range(30)]
    )
    assert any(r.status_code == 200 for r in rs)
    assert (await usage(client, tid))["used_tokens"] <= 1000


async def test_counter_rebuilds_from_postgres_after_redis_loss(client, app):
    tid, h = await make_tenant_key(client, budget=10_000)
    for _ in range(3):
        await client.post("/v1/chat/completions", json=BODY, headers=h)
    await app.state.usage.flush()
    assert (await usage(client, tid))["used_tokens"] == 18
    await app.state.redis.flushdb()  # simulate Redis losing its data
    assert (await usage(client, tid))["used_tokens"] == 18  # rebuilt from usage_events


async def test_budget_falls_back_to_postgres_when_redis_is_down(client, app):
    import redis.asyncio as aioredis

    from app.limits.budget import BudgetManager

    tid, h = await make_tenant_key(client, budget=300)  # covers the ~257-token reservation
    r = await client.post("/v1/chat/completions", json=BODY, headers=h)  # 6 tokens really used
    assert r.status_code == 200
    await app.state.usage.flush()
    dead = aioredis.from_url("redis://localhost:1/0", socket_connect_timeout=0.1)
    bm = BudgetManager(dead, app.state.sessionmaker)
    ok = await bm.reserve(uuid.UUID(tid), 300, 3)
    over = await bm.reserve(uuid.UUID(tid), 300, 295)  # 6 used + 295 > 300
    assert ok.degraded and ok.allowed
    assert over.degraded and not over.allowed  # cap still enforced without Redis
    await dead.aclose()


# ---- usage events -----------------------------------------------------------------------
async def test_usage_event_recorded_with_tokens_and_provider(client, app):
    _, h = await make_tenant_key(client, budget=10_000)
    r = await client.post("/v1/chat/completions", json=BODY, headers=h)
    await app.state.usage.flush()
    async with app.state.sessionmaker() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT provider, model, total_tokens, status, request_id, tokens_estimated "
                    "FROM usage_events"
                )
            )
        ).all()
    assert len(rows) == 1
    row = rows[0]
    assert (row.provider, row.model, row.total_tokens, row.status) == ("mock", "mock/test", 6, "ok")
    assert row.request_id == r.headers["x-request-id"] and row.tokens_estimated is False


async def test_failed_request_is_recorded_as_error(client, app, mocks):
    _, h = await make_tenant_key(client, budget=10_000)
    mocks["mock"].error_rate = 1.0
    await client.post("/v1/chat/completions", json=BODY, headers=h)
    await app.state.usage.flush()
    async with app.state.sessionmaker() as s:
        row = (await s.execute(text("SELECT status, total_tokens FROM usage_events"))).one()
    assert row.status == "error" and row.total_tokens == 0


async def test_streaming_usage_is_reconciled_from_final_chunk(client, app):
    import asyncio

    tid, h = await make_tenant_key(client, budget=10_000)
    async with client.stream(
        "POST", "/v1/chat/completions", json=BODY | {"stream": True}, headers=h
    ) as r:
        _ = [ln async for ln in r.aiter_lines()]
    await asyncio.sleep(0.1)  # settlement runs as a background task after the stream closes
    await app.state.usage.flush()
    assert (await usage(client, tid))["used_tokens"] == 6
    async with app.state.sessionmaker() as s:
        row = (
            await s.execute(text("SELECT total_tokens, tokens_estimated FROM usage_events"))
        ).one()
    assert row.total_tokens == 6 and row.tokens_estimated is False
