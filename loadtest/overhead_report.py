"""Measure gateway-added latency from the gateway's own histograms (not the client's clock).

Usage: ADMIN_TOKEN=... uv run python loadtest/overhead_report.py [requests=3000] [concurrency=16]
Needs the gateway on :8000 and the mock provider on :9000 (MOCK_LATENCY_MS fixed, e.g. 40).
Everything shares one machine, so treat results as RELATIVE (before/after), not as a benchmark.
"""

import asyncio
import os
import sys
import time
import uuid

import httpx
from prometheus_client.parser import text_string_to_metric_families

N = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
CONC = int(sys.argv[2]) if len(sys.argv) > 2 else 16
URL = "http://localhost:8000"
ADMIN = {"Authorization": f"Bearer {os.environ['ADMIN_TOKEN']}"}
BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hello world"}]}


async def scrape(c: httpx.AsyncClient) -> dict[float, float]:
    """Cumulative bucket counts of gateway_overhead_seconds{stream="false"}."""
    text = (await c.get("/metrics", headers=ADMIN)).text
    out: dict[float, float] = {}
    for fam in text_string_to_metric_families(text):
        if fam.name == "gateway_overhead_seconds":
            for s in fam.samples:
                if s.name.endswith("_bucket") and s.labels["stream"] == "false":
                    out[float(s.labels["le"])] = s.value
    return out


def quantile(buckets: dict[float, float], q: float) -> float:
    les = sorted(buckets)
    total = buckets[les[-1]]
    rank, prev_le, prev_c = q * total, 0.0, 0.0
    for le in les:
        if buckets[le] >= rank:
            span = buckets[le] - prev_c
            return prev_le + (le - prev_le) * ((rank - prev_c) / span if span else 0)
        prev_le, prev_c = le, buckets[le]
    return les[-1]


async def main() -> None:
    async with httpx.AsyncClient(base_url=URL, timeout=30) as c:
        t = (
            await c.post(
                "/admin/tenants",
                json={"name": f"bench-{uuid.uuid4().hex[:6]}", "monthly_token_budget": 10**12},
                headers=ADMIN,
            )
        ).json()
        k = (
            await c.post(
                f"/admin/tenants/{t['id']}/keys",
                json={"rpm_limit": 10**7, "tpm_limit": 10**9},
                headers=ADMIN,
            )
        ).json()
        h = {"Authorization": f"Bearer {k['key']}"}
        sem = asyncio.Semaphore(CONC)

        async def one() -> int:
            async with sem:
                return (await c.post("/v1/chat/completions", json=BODY, headers=h)).status_code

        await asyncio.gather(*[one() for _ in range(200)])  # warm-up
        before = await scrape(c)
        t0 = time.monotonic()
        codes = await asyncio.gather(*[one() for _ in range(N)])
        wall = time.monotonic() - t0
        after = await scrape(c)

    delta = {le: after[le] - before.get(le, 0) for le in after}
    ok = sum(1 for x in codes if x == 200)
    print(f"requests={N} ok={ok} concurrency={CONC} throughput={N / wall:.0f} req/s")
    ms = lambda q: f"{quantile(delta, q) * 1000:.1f} ms"
    print(f"gateway overhead  p50={ms(0.5)}  p95={ms(0.95)}  p99={ms(0.99)}")


asyncio.run(main())
