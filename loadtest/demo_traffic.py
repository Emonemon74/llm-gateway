"""Generate varied traffic so the Grafana dashboard has something to show.

Usage: uv run python loadtest/demo_traffic.py [seconds] [gateway_url]
Needs ADMIN_TOKEN in the environment and routes: alias "smart" -> alpha (p0), beta (p1).
Two tenants: "heavy" (busy, high limits) and "tiny" (RPM 5, so it gets rate limited).
"""

import asyncio
import os
import random
import sys
import time
import uuid

import httpx

SECONDS = float(sys.argv[1]) if len(sys.argv) > 1 else 30
URL = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8000"
ADMIN = {"Authorization": f"Bearer {os.environ['ADMIN_TOKEN']}"}


async def tenant(c: httpx.AsyncClient, name: str, rpm: int) -> dict:
    t = (
        await c.post(
            "/admin/tenants",
            json={"name": f"{name}-{uuid.uuid4().hex[:5]}", "monthly_token_budget": 5_000_000},
            headers=ADMIN,
        )
    ).json()
    k = (
        await c.post(f"/admin/tenants/{t['id']}/keys", json={"rpm_limit": rpm}, headers=ADMIN)
    ).json()
    return {"Authorization": f"Bearer {k['key']}"}


async def worker(c, headers, until, stats):
    while time.monotonic() < until:
        body = {
            "model": "smart",
            "stream": random.random() < 0.3,
            "messages": [{"role": "user", "content": "hello " * random.randint(1, 40)}],
        }
        try:
            r = await c.post("/v1/chat/completions", json=body, headers=headers)
            stats[r.status_code] = stats.get(r.status_code, 0) + 1
        except httpx.HTTPError:
            stats["conn_err"] = stats.get("conn_err", 0) + 1
        await asyncio.sleep(random.uniform(0.01, 0.1))


async def main():
    async with httpx.AsyncClient(base_url=URL, timeout=30) as c:
        heavy, tiny = await tenant(c, "heavy", 6000), await tenant(c, "tiny", 5)
        until, stats = time.monotonic() + SECONDS, {}
        await asyncio.gather(
            *[worker(c, heavy, until, stats) for _ in range(8)],
            *[worker(c, tiny, until, stats) for _ in range(2)],
        )
    print("status codes:", dict(sorted(stats.items(), key=str)))


asyncio.run(main())
