import asyncio
import json
import uuid

import pytest
from sqlalchemy import text

from app.batch.queue import DELAYED, DLQ, STREAM, BatchQueue
from app.routing.breaker import CircuitBreaker
from tests.conftest import ADMIN
from tests.integration.test_failover import add_route


def items(n, model="mock/test", prefix="i"):
    return [
        {
            "custom_id": f"{prefix}{k}",
            "body": {"model": model, "messages": [{"role": "user", "content": f"q{k}"}]},
        }
        for k in range(n)
    ]


async def tenant_key(client, budget=10**9, **key):
    t = (
        await client.post(
            "/admin/tenants",
            json={"name": f"t-{uuid.uuid4().hex[:6]}", "monthly_token_budget": budget},
            headers=ADMIN,
        )
    ).json()
    k = (await client.post(f"/admin/tenants/{t['id']}/keys", json=key, headers=ADMIN)).json()
    return t["id"], k["id"], {"Authorization": f"Bearer {k['key']}"}


async def submit(client, h, reqs, **extra):
    r = await client.post("/v1/batches", json={"requests": reqs} | extra, headers=h)
    return r


async def wait_done(client, h, batch_id, timeout=15):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        b = (await client.get(f"/v1/batches/{batch_id}", headers=h)).json()
        if b["status"] != "running":
            return b
        if loop.time() > deadline:
            raise AssertionError(f"batch did not finish in {timeout}s: {b}")
        await asyncio.sleep(0.05)


async def item_rows(app, batch_id):
    async with app.state.sessionmaker() as s:
        return (
            await s.execute(
                text(
                    "SELECT custom_id, status, attempts, last_error FROM batch_items "
                    "WHERE batch_id = :b ORDER BY idx"
                ),
                {"b": batch_id},
            )
        ).all()


@pytest.fixture
async def no_breaker(app):
    """Effectively disable the circuit breaker so retry/DLQ behaviour is tested in isolation."""
    app.state.router.breaker = CircuitBreaker(app.state.redis, threshold=10**6)


# ---- happy path & API ---------------------------------------------------------------------
async def test_batch_happy_path(client, app, run_worker):
    _, _, h = await tenant_key(client)
    r = await submit(client, h, items(20))
    assert r.status_code == 202 and r.json()["total"] == 20 and r.json()["status"] == "running"
    bid = r.json()["id"]
    await run_worker()
    b = await wait_done(client, h, bid)
    assert (b["status"], b["completed"], b["failed"]) == ("completed", 20, 0)
    assert b["finished_at"]

    res = await client.get(f"/v1/batches/{bid}/results", headers=h)
    lines = [json.loads(x) for x in res.text.splitlines()]
    assert [x["custom_id"] for x in lines] == [f"i{k}" for k in range(20)]  # submission order
    assert all(
        x["status"] == "done" and x["response"]["object"] == "chat.completion" for x in lines
    )

    await app.state.usage.flush()
    async with app.state.sessionmaker() as s:
        n = await s.scalar(text("SELECT count(*) FROM usage_events WHERE status='ok'"))
    assert n == 20
    assert await app.state.redis.xlen(DLQ) == 0


async def test_submit_validation(client):
    _, _, h = await tenant_key(client)
    dup = await submit(client, h, items(1) + items(1))
    assert dup.status_code == 422 and "unique" in dup.text
    unknown = await submit(client, h, items(1, model="nope/x"))
    assert unknown.status_code == 422 and unknown.json()["error"]["type"] == "model_not_found"
    streamed = items(1)
    streamed[0]["body"]["stream"] = True
    assert (await submit(client, h, streamed)).status_code == 422
    assert (await submit(client, h, [])).status_code == 422
    assert (await client.post("/v1/batches", json={"requests": items(1)})).status_code == 401


async def test_idempotency_key_returns_same_batch(client, app):
    _, _, h = await tenant_key(client)
    hh = h | {"Idempotency-Key": "order-42"}
    a = await client.post("/v1/batches", json={"requests": items(5)}, headers=hh)
    b = await client.post("/v1/batches", json={"requests": items(5)}, headers=hh)
    assert (a.status_code, b.status_code) == (202, 200)
    assert a.json()["id"] == b.json()["id"]
    async with app.state.sessionmaker() as s:
        assert await s.scalar(text("SELECT count(*) FROM batch_items")) == 5  # not 10
        assert await s.scalar(text("SELECT count(*) FROM batches")) == 1


async def test_batches_are_tenant_isolated(client):
    _, _, h1 = await tenant_key(client)
    _, _, h2 = await tenant_key(client)
    bid = (await submit(client, h1, items(2))).json()["id"]
    assert (await client.get(f"/v1/batches/{bid}", headers=h2)).status_code == 404
    assert (await client.get(f"/v1/batches/{bid}/results", headers=h2)).status_code == 404
    assert (await client.get(f"/v1/batches/{bid}", headers=h1)).status_code == 200


# ---- retries, DLQ, failure classes ----------------------------------------------------------
async def test_transient_failures_are_retried_then_succeed(
    client, app, run_worker, mocks, no_breaker
):
    _, _, h = await tenant_key(client)
    await add_route(client, "flaky", "alpha", 0)
    mocks["alpha"].error_rate = 1.0
    bid = (await submit(client, h, items(5, model="flaky"), max_attempts=10)).json()["id"]
    await run_worker()
    await asyncio.sleep(0.3)  # a few failed rounds happen (far fewer than max_attempts)
    assert (await client.get(f"/v1/batches/{bid}", headers=h)).json()["completed"] == 0
    mocks["alpha"].error_rate = 0.0  # provider recovers
    b = await wait_done(client, h, bid)
    assert (b["status"], b["completed"], b["failed"]) == ("completed", 5, 0)
    assert all(r.attempts > 1 for r in await item_rows(app, bid))  # they really were retried


async def test_item_is_dead_lettered_after_max_attempts(client, app, run_worker, mocks, no_breaker):
    _, _, h = await tenant_key(client)
    mocks["mock"].error_rate = 1.0
    bid = (await submit(client, h, items(4), max_attempts=3)).json()["id"]
    await run_worker()
    b = await wait_done(client, h, bid)
    assert (b["status"], b["completed"], b["failed"]) == ("completed_with_errors", 0, 4)
    rows = await item_rows(app, bid)
    assert all(r.status == "failed" and r.attempts == 3 and "gave up" in r.last_error for r in rows)
    assert await app.state.redis.xlen(DLQ) == 4
    res = [
        json.loads(x)
        for x in (await client.get(f"/v1/batches/{bid}/results", headers=h)).text.splitlines()
    ]
    assert all(x["status"] == "failed" and "gave up" in x["error"] for x in res)


async def test_client_errors_fail_immediately_without_retry(
    client, app, run_worker, mocks, no_breaker
):
    _, _, h = await tenant_key(client)
    mocks["mock"].error_rate, mocks["mock"].error_status = 1.0, 400
    bid = (await submit(client, h, items(3), max_attempts=5)).json()["id"]
    await run_worker()
    b = await wait_done(client, h, bid)
    assert b["failed"] == 3
    assert all(r.attempts == 1 for r in await item_rows(app, bid))  # a 400 will never succeed
    assert mocks["mock"].calls == 3


async def test_budget_exhaustion_fails_items_permanently(client, app, run_worker):
    _, _, h = await tenant_key(client, budget=0)
    bid = (await submit(client, h, items(3))).json()["id"]
    await run_worker()
    b = await wait_done(client, h, bid)
    assert b["failed"] == 3
    assert all("budget" in r.last_error for r in await item_rows(app, bid))


async def test_open_circuit_defers_without_burning_attempts(client, app, run_worker, mocks):
    """A provider outage must not dead-letter the batch: items wait it out."""
    _, _, h = await tenant_key(client)
    await add_route(client, "only", "alpha", 0)
    mocks["alpha"].error_rate = 1.0
    bid = (await submit(client, h, items(6, model="only"), max_attempts=3)).json()["id"]
    await run_worker()
    await asyncio.sleep(1.0)  # breaker (threshold 3) opens; items get deferred, not failed
    assert await app.state.breaker.state("alpha") in ("open", "half_open")
    mocks["alpha"].error_rate = 0.0
    b = await wait_done(client, h, bid, timeout=20)
    assert (b["status"], b["failed"]) == ("completed", 0)


async def test_rate_limit_backpressure_does_not_burn_attempts(client, app, run_worker):
    _, key_id, h = await tenant_key(client, rpm_limit=600)  # batch bucket: cap 300, 5 tokens/s
    for _ in range(300):  # drain the batch bucket completely
        await app.state.limiter.consume(f"{key_id}:batch", 300, 10**9, 1)
    bid = (await submit(client, h, items(4), max_attempts=2)).json()["id"]
    await run_worker()
    b = await wait_done(client, h, bid, timeout=20)
    assert (b["status"], b["failed"]) == ("completed", 0)
    assert all(r.attempts == 1 for r in await item_rows(app, bid))  # waiting != failing


async def test_batch_uses_its_own_rate_limit_bucket(client, app, run_worker):
    """Draining the batch bucket must not touch the key's interactive budget."""
    _, _key_id, h = await tenant_key(client, rpm_limit=60)
    bid = (await submit(client, h, items(3))).json()["id"]
    await run_worker()
    await wait_done(client, h, bid)
    r = await client.post(
        "/v1/chat/completions",
        json={"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]},
        headers=h,
    )
    assert r.status_code == 200 and int(r.headers["x-ratelimit-remaining-requests"]) == 59


# ---- delivery guarantees ---------------------------------------------------------------------
async def test_duplicate_deliveries_never_double_count(client, app, run_worker, mocks):
    _, _, h = await tenant_key(client)
    bid = (await submit(client, h, items(1))).json()["id"]
    async with app.state.sessionmaker() as s:
        iid = str(await s.scalar(text("SELECT id FROM batch_items")))
    q = BatchQueue(app.state.redis)
    await q.enqueue([iid, iid])  # two extra concurrent deliveries on top of the original
    await run_worker()
    b = await wait_done(client, h, bid)
    await asyncio.sleep(0.3)  # let every delivery drain
    # Guarantee: the RESULT is recorded once, however many deliveries raced.
    # (Concurrent duplicates may each call the upstream: at-least-once, documented.)
    assert (b["completed"], b["failed"], b["total"]) == (1, 0, 1)
    assert 1 <= mocks["mock"].calls <= 3
    async with app.state.sessionmaker() as s:
        assert await s.scalar(text("SELECT count(*) FROM usage_events WHERE status = 'ok'")) >= 1

    # Duplicates arriving AFTER completion are free: no upstream call at all.
    calls = mocks["mock"].calls
    await q.enqueue([iid, iid, iid])
    await asyncio.sleep(0.5)
    assert mocks["mock"].calls == calls
    assert (await client.get(f"/v1/batches/{bid}", headers=h)).json()["completed"] == 1
    assert await app.state.redis.xlen(STREAM) == 0  # and everything got acked/trimmed


async def test_messages_of_a_dead_consumer_are_reclaimed(client, app, run_worker):
    _, _, h = await tenant_key(client)
    bid = (await submit(client, h, items(5))).json()["id"]
    q = BatchQueue(app.state.redis)
    await q.ensure_group()
    stolen = await q.read("ghost-worker", 10, 100)  # a worker takes them... and dies silently
    assert len(stolen) == 5
    await run_worker(visibility_timeout_s=0.3)
    b = await wait_done(client, h, bid)
    assert (b["status"], b["completed"]) == ("completed", 5)


async def test_reconciler_recovers_items_redis_forgot(client, app, run_worker):
    _, _, h = await tenant_key(client)
    bid = (await submit(client, h, items(6))).json()["id"]
    await app.state.redis.delete(STREAM, DELAYED)  # Redis loses the queue entirely
    assert await app.state.redis.xlen(STREAM) == 0
    await run_worker(reconcile_after_s=0.2)
    b = await wait_done(client, h, bid)
    assert (b["status"], b["completed"]) == ("completed", 6)


async def test_delayed_items_move_to_stream_only_when_due(app):
    q = BatchQueue(app.state.redis)
    await q.ensure_group()
    await q.delay("item-x", 0.25)
    assert await q.move_due() == 0
    await asyncio.sleep(0.35)
    assert await q.move_due() == 1
    assert await q.move_due() == 0  # moved exactly once
    assert await app.state.redis.xlen(STREAM) == 1


async def test_two_workers_share_the_work_without_duplicating_results(
    client, app, run_worker, mocks
):
    _, _, h = await tenant_key(client, rpm_limit=10**6)
    bid = (await submit(client, h, items(60))).json()["id"]
    await run_worker(concurrency=4)
    await run_worker(concurrency=4)
    b = await wait_done(client, h, bid)
    assert (b["completed"], b["failed"]) == (60, 0)
    assert mocks["mock"].calls == 60  # each item hit the upstream exactly once


async def test_queue_depth_metrics_visible(client, app):
    _, _, h = await tenant_key(client)
    await submit(client, h, items(7))
    text_ = (await client.get("/metrics", headers=ADMIN)).text
    assert "gateway_queue_depth 7.0" in text_
