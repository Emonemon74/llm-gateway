"""Reproducible load test: clean stack -> provision -> k6 -> scrape the gateway's own metrics.

    ADMIN_TOKEN=... uv run python loadtest/run_load_test.py [sweep|failover|ratelimit|all]

Everything (gateway replicas, mock upstreams, k6, Postgres, Redis) shares ONE machine, so absolute
numbers are conservative and noisy; compare scenarios against each other, not against a datasheet.
Uses its own database (gateway_load) and Redis db 2 so it never touches dev or test data.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import psutil
from prometheus_client.parser import text_string_to_metric_families

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "loadtest" / "results" / time.strftime("%Y%m%d-%H%M%S")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "load-admin")
DB_URL = "postgresql+asyncpg://gateway:gateway@localhost:5432/gateway_load"
REDIS_URL = "redis://localhost:6379/2"
UPSTREAM_MS = 50
BASE_PORT, ALPHA_PORT, BETA_PORT = 8100, 9100, 9101


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True, **kw)


def prepare_db() -> None:
    sh(
        [
            "docker",
            "exec",
            "deploy-postgres-1",
            "psql",
            "-U",
            "gateway",
            "-c",
            "DROP DATABASE IF EXISTS gateway_load",
        ]
    )
    sh(
        [
            "docker",
            "exec",
            "deploy-postgres-1",
            "psql",
            "-U",
            "gateway",
            "-c",
            "CREATE DATABASE gateway_load",
        ]
    )
    sh(["docker", "exec", "deploy-redis-1", "redis-cli", "-n", "2", "flushdb"])
    env = {**os.environ, "DATABASE_URL": DB_URL}
    subprocess.run(
        ["uv", "run", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
    )


@dataclass
class Stack:
    bases: list[str]
    keys: list[str]
    procs: list[subprocess.Popen]
    alpha: str
    beta: str
    gateways: list[subprocess.Popen] | None = None

    def stop(self) -> None:
        for p in self.procs:
            with contextlib.suppress(ProcessLookupError):
                p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                p.kill()


async def _wait_http(url: str, tries: int = 100) -> None:
    async with httpx.AsyncClient() as c:
        for _ in range(tries):
            with contextlib.suppress(httpx.HTTPError):
                if (await c.get(url)).status_code == 200:
                    return
            await asyncio.sleep(0.1)
    raise RuntimeError(f"{url} never came up")


async def start_stack(
    replicas: int, n_keys: int = 10, key_rpm: int = 10**9, mock_workers: int = 2
) -> Stack:
    prepare_db()
    env = {
        **os.environ,
        "DATABASE_URL": DB_URL,
        "REDIS_URL": REDIS_URL,
        "ADMIN_TOKEN": ADMIN_TOKEN,
        "EXTRA_PROVIDERS": json.dumps(
            {
                "alpha": {"base_url": f"http://127.0.0.1:{ALPHA_PORT}/v1"},
                "beta": {"base_url": f"http://127.0.0.1:{BETA_PORT}/v1"},
            }
        ),
        "BREAKER_FAILURE_THRESHOLD": "5",
        "BREAKER_COOLDOWN_S": "5",
        "MOCK_LATENCY_MS": str(UPSTREAM_MS),
    }
    procs: list[subprocess.Popen] = []

    def spawn(args: list[str]) -> None:
        procs.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    *args,
                    "--log-level",
                    "warning",
                    "--no-access-log",
                ],
                cwd=ROOT,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )

    # >1 mock worker keeps the fake upstream from being the bottleneck, but /control only reaches
    # one worker, so scenarios that flip behaviour at runtime must use mock_workers=1.
    spawn(["mock_provider.main:app", "--port", str(ALPHA_PORT), "--workers", str(mock_workers)])
    spawn(["mock_provider.main:app", "--port", str(BETA_PORT), "--workers", str(mock_workers)])
    bases = [f"http://127.0.0.1:{BASE_PORT + i}" for i in range(replicas)]
    n_before = len(procs)
    for i in range(replicas):
        spawn(["app.main:app", "--port", str(BASE_PORT + i)])
    stack = Stack(
        bases,
        [],
        procs,
        f"http://127.0.0.1:{ALPHA_PORT}",
        f"http://127.0.0.1:{BETA_PORT}",
        gateways=procs[n_before:],
    )
    try:
        await asyncio.gather(
            *[_wait_http(f"{u}/healthz") for u in bases],
            _wait_http(stack.alpha + "/healthz"),
            _wait_http(stack.beta + "/healthz"),
        )
        admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
        async with httpx.AsyncClient(base_url=bases[0], headers=admin, timeout=30) as c:
            for prio, prov in enumerate(("alpha", "beta")):
                await c.post(
                    "/admin/routes",
                    json={
                        "alias": "fast",
                        "provider": prov,
                        "upstream_model": "m",
                        "priority": prio,
                    },
                )
            for i in range(n_keys):
                t = (
                    await c.post(
                        "/admin/tenants", json={"name": f"load-{i}", "monthly_token_budget": 10**12}
                    )
                ).json()
                k = (
                    await c.post(
                        f"/admin/tenants/{t['id']}/keys",
                        json={"rpm_limit": key_rpm, "tpm_limit": 2_000_000_000},
                    )
                ).json()
                stack.keys.append(k["key"])
    except BaseException:
        stack.stop()
        raise
    return stack


# ---- k6 + metrics ------------------------------------------------------------------------
def run_k6(
    stack: Stack,
    name: str,
    rate: int,
    duration: str,
    *,
    model: str = "fast",
    keys: list[str] | None = None,
    expect_429: bool = False,
) -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    summary = OUT / f"{name}.json"
    env = {
        **os.environ,
        "BASES": ",".join(stack.bases),
        "KEYS": ",".join(keys or stack.keys),
        "RATE": str(rate),
        "DURATION": duration,
        "MODEL": model,
        "EXPECT_429": "1" if expect_429 else "",
    }
    subprocess.run(
        ["k6", "run", "--quiet", "--summary-export", str(summary), "loadtest/k6/chat.js"],
        cwd=ROOT,
        env=env,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    m = json.loads(summary.read_text())["metrics"]

    def val(metric: str, stat: str, default: float = 0.0) -> float:
        d = m.get(metric, {})
        return float(d.get("values", d).get(stat, default))

    return {
        "offered_rps": rate,
        "duration": duration,
        "achieved_rps": val("http_reqs", "rate"),
        "requests": int(val("http_reqs", "count")),
        "ok": int(val("status_200", "count")),
        "r429": int(val("status_429", "count")),
        "err5xx": int(val("status_5xx", "count")),
        "other": int(val("status_other", "count")),
        "shed503": int(val("status_503", "count")),
        "ok_p50_ms": val("latency_ok", "med"),
        "ok_p95_ms": val("latency_ok", "p(95)"),
        "ok_p99_ms": val("latency_ok", "p(99)"),
        "dropped": int(val("dropped_iterations", "count")),
        "p50_ms": val("http_req_duration", "med"),
        "p95_ms": val("http_req_duration", "p(95)"),
        "p99_ms": val("http_req_duration", "p(99)"),
        "max_ms": val("http_req_duration", "max"),
    }


def _overhead_buckets(bases: list[str]) -> dict[float, float]:
    total: dict[float, float] = {}
    for b in bases:
        text = httpx.get(f"{b}/metrics", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"}).text
        for fam in text_string_to_metric_families(text):
            if fam.name == "gateway_overhead_seconds":
                for s in fam.samples:
                    if s.name.endswith("_bucket") and s.labels["stream"] == "false":
                        le = float(s.labels["le"])
                        total[le] = total.get(le, 0.0) + s.value
    return total


def _quantile(b: dict[float, float], q: float) -> float:
    les = sorted(b)
    if not les or b[les[-1]] == 0:
        return 0.0
    rank, prev_le, prev_c = q * b[les[-1]], 0.0, 0.0
    for le in les:
        if b[le] >= rank:
            span = b[le] - prev_c
            return prev_le + (le - prev_le) * ((rank - prev_c) / span if span else 0)
        prev_le, prev_c = le, b[le]
    return les[-1]


def wait_for_quiet(max_load: float = 3.0, timeout_s: int = 60) -> float:
    """Block until the 1-minute load average drops, so we measure the gateway, not the laptop."""
    deadline = time.monotonic() + timeout_s
    while os.getloadavg()[0] > max_load and time.monotonic() < deadline:
        time.sleep(5)
    return os.getloadavg()[0]


def with_overhead(stack: Stack, fn, *, wait: bool = True):
    load_before = wait_for_quiet() if wait else os.getloadavg()[0]
    before = _overhead_buckets(stack.bases)
    procs = [psutil.Process(p.pid) for p in (stack.gateways or [])]
    for p in procs:
        p.cpu_percent(None)  # prime the counters
    samples: list[float] = []
    stop = threading.Event()

    def sampler() -> None:
        time.sleep(4)  # skip ramp-up
        while not stop.wait(1.0):
            samples.append(sum(p.cpu_percent(None) for p in procs) / 100)  # in cores

    t = threading.Thread(target=sampler, daemon=True)
    t.start()
    out = fn()
    stop.set()
    t.join()
    after = _overhead_buckets(stack.bases)
    d = {le: after[le] - before.get(le, 0) for le in after}
    out |= {f"overhead_p{int(q * 100)}_ms": _quantile(d, q) * 1000 for q in (0.5, 0.95, 0.99)}
    out["gw_cpu_cores"] = statistics.mean(samples) if samples else 0.0
    out["load1_before"] = load_before
    out["load1_after"] = os.getloadavg()[0]
    return out


def fmt(r: dict) -> str:
    return (
        f"offered={r['offered_rps']:>5}  achieved={r['achieved_rps']:>7.1f}  ok={r['ok']:>6}  "
        f"5xx={r['err5xx']:>4} (shed={r['shed503']:>4})  other={r['other']:>4}  429={r['r429']:>5}  dropped={r['dropped']:>5}  "
        f"p50={r['p50_ms']:>6.0f}  p95={r['p95_ms']:>6.0f}  p99={r['p99_ms']:>6.0f} ms"
        f"  | 200s only p50/p95/p99 = {r['ok_p50_ms']:.0f}/{r['ok_p95_ms']:.0f}/{r['ok_p99_ms']:.0f} ms"
        + (
            f"  | gw-overhead p50/p95/p99 = {r['overhead_p50_ms']:.1f}/"
            f"{r['overhead_p95_ms']:.1f}/{r['overhead_p99_ms']:.1f} ms  | gw cpu={r['gw_cpu_cores']:.2f} cores"
            f"  | load {r['load1_before']:.1f}->{r['load1_after']:.1f}"
            if "overhead_p50_ms" in r
            else ""
        )
    )


# ---- scenarios ---------------------------------------------------------------------------
async def scenario_sweep(replicas: int, rates: list[int], duration: str = "20s") -> list[dict]:
    print(f"\n=== SWEEP: {replicas} gateway replica(s), upstream fixed at {UPSTREAM_MS} ms ===")
    stack = await start_stack(replicas)
    results = []
    try:
        run_k6(stack, f"warmup-{replicas}r", 50, "5s")
        for rate in rates:
            await asyncio.sleep(3)  # let queues drain between steps
            r = with_overhead(
                stack, lambda rate=rate: run_k6(stack, f"sweep-{replicas}r-{rate}", rate, duration)
            )
            r["replicas"] = replicas
            print(" ", fmt(r), flush=True)
            results.append(r)
    finally:
        stack.stop()
    return results


async def scenario_failover(
    rate: int = 200, total_s: int = 60, down_at: int = 15, up_at: int = 40
) -> dict:
    print(
        f"\n=== FAILOVER: {rate} req/s for {total_s}s; alpha (primary) fully down from t={down_at}s to t={up_at}s ==="
    )
    stack = await start_stack(2, mock_workers=1)  # /control must reach the only worker
    timeline: list[tuple[float, str]] = []
    try:
        run_k6(stack, "warmup-failover", 50, "5s")
        # Settle the machine FIRST, then start the clock together with k6. (Waiting inside the
        # timed section once let the whole outage pass before any traffic started.)
        await asyncio.to_thread(wait_for_quiet)
        task = asyncio.create_task(
            asyncio.to_thread(
                lambda: with_overhead(
                    stack, lambda: run_k6(stack, "failover", rate, f"{total_s}s"), wait=False
                )
            )
        )
        t0 = time.monotonic()
        last = None
        async with httpx.AsyncClient() as c:
            while not task.done():
                t = time.monotonic() - t0
                if down_at <= t < up_at and not getattr(stack, "_down", False):
                    await c.post(f"{stack.alpha}/control", json={"error_rate": 1.0})
                    stack._down = True  # type: ignore[attr-defined]
                    timeline.append((t, "alpha outage STARTS"))
                if t >= up_at and getattr(stack, "_down", False):
                    await c.post(f"{stack.alpha}/control", json={"error_rate": 0.0})
                    stack._down = False  # type: ignore[attr-defined]
                    timeline.append((t, "alpha outage ENDS"))
                text = (
                    await c.get(
                        f"{stack.bases[0]}/metrics",
                        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                    )
                ).text
                for fam in text_string_to_metric_families(text):
                    if fam.name == "gateway_circuit_state":
                        for s in fam.samples:
                            if s.labels["provider"] == "alpha":
                                state = {0: "closed", 1: "half_open", 2: "open"}.get(
                                    int(s.value), "?"
                                )
                                if state != last:
                                    timeline.append((t, f"alpha circuit -> {state}"))
                                    last = state
                await asyncio.sleep(0.25)
        res = await task
    finally:
        stack.stop()
    res["timeline"] = [(round(t, 1), e) for t, e in timeline]
    sent = res["requests"]
    res["success_pct"] = 100 * res["ok"] / sent if sent else 0
    print(" ", fmt(res))
    print(f"  success rate: {res['success_pct']:.3f}%  ({res['ok']}/{sent}; 5xx={res['err5xx']})")
    for t, e in res["timeline"]:
        print(f"    t={t:>5}s  {e}")
    return res


async def scenario_ratelimit(
    replicas: int = 3, rpm: int = 600, rate: int = 100, seconds: int = 20
) -> dict:
    print(
        f"\n=== RATE LIMIT ACCURACY: key limited to {rpm} req/min, {rate} req/s offered for {seconds}s "
        f"across {replicas} replicas ==="
    )
    stack = await start_stack(replicas)
    try:
        admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
        async with httpx.AsyncClient(base_url=stack.bases[0], headers=admin) as c:
            t = (
                await c.post(
                    "/admin/tenants", json={"name": "limited", "monthly_token_budget": 10**12}
                )
            ).json()
            k = (
                await c.post(
                    f"/admin/tenants/{t['id']}/keys",
                    json={"rpm_limit": rpm, "tpm_limit": 2_000_000_000},
                )
            ).json()
        res = run_k6(stack, "ratelimit", rate, f"{seconds}s", keys=[k["key"]], expect_429=True)
    finally:
        stack.stop()
    expected = rpm + rpm / 60 * seconds  # full bucket at start + refill during the run
    res["expected_allowed"] = expected
    res["error_pct"] = 100 * (res["ok"] - expected) / expected
    print(
        f"  offered {res['requests']} requests; allowed={res['ok']} rejected(429)={res['r429']} "
        f"5xx={res['err5xx']}"
    )
    print(
        f"  theoretical allowed = {rpm} (initial burst) + {rpm / 60:.0f}/s x {seconds}s = {expected:.0f}  "
        f"-> deviation {res['error_pct']:+.2f}%"
    )
    return res


async def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    results: dict = {}
    if which in ("sweep", "all"):
        results["sweep_1"] = await scenario_sweep(1, [200, 300, 400, 500, 600])
    if which == "scale":  # only meaningful on a machine with spare cores
        results["sweep_3"] = await scenario_sweep(3, [400, 800, 1200, 1600])
    if which in ("failover", "all"):
        results["failover"] = await scenario_failover()
    if which in ("ratelimit", "all"):
        results["ratelimit"] = await scenario_ratelimit()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "results.json").write_text(json.dumps(results, indent=1))
    print(f"\nraw results: {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
