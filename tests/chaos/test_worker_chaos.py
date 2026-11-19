"""Chaos tests: REAL worker processes, hard-killed / starved of Redis mid-batch.

These prove the headline guarantee: after kill -9 (or Redis losing all data) every item still
completes, and every RESULT is recorded exactly once. The upstream call may be repeated for
work that was in flight at the moment of the crash (at-least-once), so we report that number.
"""

import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text

from app.batch.queue import DLQ, GROUP, STREAM
from tests.conftest import ADMIN

pytestmark = pytest.mark.chaos
ROOT = Path(__file__).resolve().parents[2]
N_ITEMS = 400
MOCK_LATENCY_MS = 300


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def spawn(args, env, log: Path):
    f = log.open("wb")
    return await asyncio.create_subprocess_exec(
        sys.executable, *args, cwd=ROOT, env={**os.environ, **env}, stdout=f, stderr=f
    )


@pytest.fixture
async def mock_server(tmp_path):
    port = free_port()
    proc = await spawn(
        ["-m", "uvicorn", "mock_provider.main:app", "--port", str(port), "--log-level", "warning"],
        {"MOCK_LATENCY_MS": str(MOCK_LATENCY_MS)},
        tmp_path / "mock.log",
    )
    base = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient() as c:
        for _ in range(100):
            with contextlib.suppress(httpx.HTTPError):
                if (await c.get(f"{base}/healthz")).status_code == 200:
                    break
            await asyncio.sleep(0.1)
        else:
            raise RuntimeError("mock server did not start")
    yield base
    proc.kill()
    await proc.wait()


@pytest.fixture
async def workers(tmp_path, mock_server):
    procs: list[asyncio.subprocess.Process] = []

    async def start(**env):
        e = {
            "EXTRA_PROVIDERS": json.dumps({"mockx": {"base_url": f"{mock_server}/v1"}}),
            "BATCH_CONCURRENCY": "8",
            "BATCH_VISIBILITY_TIMEOUT_S": "2",
            "BATCH_RECONCILE_AFTER_S": "600",
            "BATCH_RECONCILE_EVERY_S": "1",
            "BATCH_BACKOFF_BASE_S": "0.1",
        } | {k.upper(): str(v) for k, v in env.items()}
        p = await spawn(["-m", "app.batch.worker"], e, tmp_path / f"worker{len(procs)}.log")
        procs.append(p)
        return p

    yield start
    for p in procs:
        if p.returncode is None:
            p.kill()
        await p.wait()


async def submit_big_batch(client, app):
    app.state.providers["mockx"] = app.state.providers["mock"]  # lets the API accept "mockx/..."
    t = (
        await client.post(
            "/admin/tenants",
            json={"name": f"chaos-{uuid.uuid4().hex[:5]}", "monthly_token_budget": 10**12},
            headers=ADMIN,
        )
    ).json()
    k = (
        await client.post(
            f"/admin/tenants/{t['id']}/keys",
            json={"rpm_limit": 10**9, "tpm_limit": 10**9},
            headers=ADMIN,
        )
    ).json()
    h = {"Authorization": f"Bearer {k['key']}"}
    reqs = [
        {
            "custom_id": f"c{i}",
            "body": {"model": "mockx/test", "messages": [{"role": "user", "content": f"q{i}"}]},
        }
        for i in range(N_ITEMS)
    ]
    r = await client.post("/v1/batches", json={"requests": reqs}, headers=h)
    assert r.status_code == 202, r.text
    return r.json()["id"], h


async def done_count(app, bid) -> int:
    async with app.state.sessionmaker() as s:
        return await s.scalar(
            text("SELECT count(*) FROM batch_items WHERE batch_id=:b AND status='done'"), {"b": bid}
        )


async def pending_for(app, pid: int) -> int:
    """Messages currently delivered-but-unacked to the consumer whose name contains this pid."""
    for c in await app.state.redis.xinfo_consumers(STREAM, GROUP):
        name = c["name"].decode() if isinstance(c["name"], bytes) else c["name"]
        if f"-{pid}-" in name:
            return int(c["pending"])
    return 0


async def wait_finished(client, h, bid, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        b = (await client.get(f"/v1/batches/{bid}", headers=h)).json()
        if b["status"] != "running":
            return b
        await asyncio.sleep(0.2)
    raise AssertionError(f"batch still running after {timeout}s: {b}")


async def assert_exactly_once(client, app, h, bid, mock_base):
    b = (await client.get(f"/v1/batches/{bid}", headers=h)).json()
    assert (b["status"], b["completed"], b["failed"], b["total"]) == (
        "completed",
        N_ITEMS,
        0,
        N_ITEMS,
    )
    async with app.state.sessionmaker() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT count(*) AS n, count(DISTINCT custom_id) AS d, "
                    "count(*) FILTER (WHERE status='done' AND response_json IS NOT NULL) AS ok "
                    "FROM batch_items WHERE batch_id=:b"
                ),
                {"b": bid},
            )
        ).one()
    assert (rows.n, rows.d, rows.ok) == (N_ITEMS, N_ITEMS, N_ITEMS)  # 1 row, 1 result per item
    res = [
        json.loads(x)
        for x in (await client.get(f"/v1/batches/{bid}/results", headers=h)).text.splitlines()
    ]
    assert [x["custom_id"] for x in res] == [f"c{i}" for i in range(N_ITEMS)]
    assert all(x["status"] == "done" for x in res)
    assert await app.state.redis.xlen(DLQ) == 0
    async with httpx.AsyncClient() as c:
        calls = (await c.get(f"{mock_base}/stats")).json()["calls"]
    assert calls >= N_ITEMS
    return calls


async def test_kill_9_one_worker_mid_batch_loses_nothing(client, app, workers, mock_server):
    bid, h = await submit_big_batch(client, app)
    a = await workers()
    b = await workers()

    # Wait until the batch is genuinely mid-flight AND worker A is holding unacked messages.
    for _ in range(300):
        if await done_count(app, bid) >= 40 and await pending_for(app, a.pid) > 0:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("never reached mid-flight state")
    held = await pending_for(app, a.pid)
    done_at_kill = await done_count(app, bid)
    a.send_signal(signal.SIGKILL)  # no cleanup handlers, no ack: a power-cut
    await a.wait()
    assert 0 < done_at_kill < N_ITEMS and held > 0

    fin = await wait_finished(client, h, bid)
    assert fin["status"] == "completed"
    calls = await assert_exactly_once(client, app, h, bid, mock_server)
    summary = await app.state.redis.xpending(STREAM, GROUP)
    assert (summary["pending"] if isinstance(summary, dict) else summary[0]) == 0  # nothing stuck
    print(
        f"\n[chaos] kill -9 with {held} unacked msgs held, {done_at_kill}/{N_ITEMS} done at kill: "
        f"all {N_ITEMS} completed exactly once; upstream calls={calls} "
        f"(+{calls - N_ITEMS} repeated for in-flight work)"
    )

    b.send_signal(signal.SIGTERM)  # graceful shutdown must exit cleanly
    assert await asyncio.wait_for(b.wait(), 15) == 0


async def test_kill_every_worker_then_start_a_new_one(client, app, workers, mock_server):
    bid, h = await submit_big_batch(client, app)
    w1, w2 = await workers(), await workers()
    for _ in range(300):
        if await done_count(app, bid) >= 30:
            break
        await asyncio.sleep(0.05)
    for w in (w1, w2):
        w.send_signal(signal.SIGKILL)
        await w.wait()
    mid = await done_count(app, bid)
    assert 0 < mid < N_ITEMS
    await asyncio.sleep(1.0)  # total outage: nothing is making progress
    assert await done_count(app, bid) == mid

    await workers()  # a replacement comes up and picks up the abandoned messages
    await wait_finished(client, h, bid)
    await assert_exactly_once(client, app, h, bid, mock_server)


async def test_redis_loses_all_data_mid_batch(client, app, workers, mock_server):
    bid, h = await submit_big_batch(client, app)
    await workers(BATCH_RECONCILE_AFTER_S=3, BATCH_RECONCILE_EVERY_S=1)
    await workers(BATCH_RECONCILE_AFTER_S=3, BATCH_RECONCILE_EVERY_S=1)
    for _ in range(300):
        if await done_count(app, bid) >= 40:
            break
        await asyncio.sleep(0.05)
    await app.state.redis.flushall()  # queue, consumer group, delayed set: all gone
    await wait_finished(client, h, bid, timeout=120)
    await assert_exactly_once(client, app, h, bid, mock_server)
