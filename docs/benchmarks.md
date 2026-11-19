# Benchmarks

**Read this first:** every number here is from one laptop running the gateway, the mock provider,
Postgres, Redis and the load generator together. They are valid for *before/after comparison on
the same machine*, not as absolute capacity claims. The real load test (k6, separate host) is
tracked in the roadmap.

Tool: `loadtest/overhead_report.py` — reads the gateway's own `gateway_overhead_seconds`
histogram (request time minus time waiting on the upstream), 3000 requests, concurrency 16,
mock upstream fixed at 40 ms. Quantiles are interpolated from histogram buckets (coarse above 10 ms).

## Auth cache (Redis in front of the per-request Postgres key lookup)

| run | p50 before | p50 after | p95 before | p95 after | p99 before | p99 after |
|---|---|---|---|---|---|---|
| 1 | 5.5 ms | 3.5 ms | 16.8 ms | 9.6 ms | 23.6 ms | 22.8 ms |
| 2 | 5.3 ms | 3.7 ms | 13.1 ms | 9.3 ms | 22.8 ms | 22.1 ms |
| 3 | 5.3 ms | 3.9 ms | 19.0 ms | 17.0 ms | 24.7 ms | 24.9 ms |

- **p50: ~5.4 → ~3.7 ms (about 30% lower), consistent across all runs.**
- p95: lower in 2 of 3 runs (13–19 → ~9.5 ms) but noisy; one "after" run did not improve. Not
  claimed as a reliable win.
- p99: unchanged (~23 ms). The tail is not auth: it points at event-loop contention and the ~7
  Redis round trips per request (limiter, budget x2, breaker x2, reconcile, auth cache).
- Throughput (~325 req/s) is capped by the test itself (16 in flight / 40 ms ≈ 400 req/s) and is
  not a gateway limit.

Next candidate for the tail: collapse Redis round trips (pipeline/one script per request).

## Load test (k6, open-loop) — 2026-11-19

Harness: `loadtest/run_load_test.py` (clean DB + Redis db, provisions 10 tenants, starts gateway
replicas and two mock upstreams fixed at 50 ms, drives `loadtest/k6/chat.js` with a
**constant-arrival-rate** executor so overload shows up as latency/errors instead of a silently
lower request rate). Gateway-added latency comes from the gateway's own histogram, CPU from psutil.

**Environment caveat (important):** one 8-core / 8 GB laptop running the gateway, mocks, k6,
Postgres and Redis (Docker VM) together, with background load from macOS (`mediaanalysisd` at
~180% CPU during part of the session; 1-minute load average 4-12). Results below are a
**lower bound** and are reported with the machine load at the time. The harness records it.

### Failover under load (the result that matters)
200 req/s for 60 s across 2 replicas, alias `fast` = alpha (primary) then beta. Alpha returned
500 for **every** request from t=15.2 s to t=40.1 s.

| requests | succeeded | 5xx | p50 | p95 | p99 | gateway overhead p95 |
|---|---|---|---|---|---|---|
| 12,000 | **12,000 (100.000%)** | 0 | 55 ms | 60 ms | 91 ms | 7.9 ms |

Circuit timeline (sampled every 0.25 s): opened 0.3 s after the outage began, a half-open probe
at t=30.3 s failed and re-opened it, closed 0.5 s after the provider recovered.

### Rate-limit accuracy across replicas
Key limited to 600 req/min, 100 req/s offered for 20 s, round-robined over **3 replicas**.
Theoretical allowance = 600 (full bucket) + 10/s x 20 s = 800.
Allowed **799** in both runs (-0.12%); the rest got 429. (2-replica smoke run: 339 vs 340.)
The limit is enforced globally because the bucket lives in Redis, not in a replica.

### Single-replica throughput sweep (upstream 50 ms)
Last run (load average shown; earlier runs agree on the shape):

| offered | achieved | ok | errors | p50 | p95 | gw overhead p50 / p95 | gw CPU | machine load |
|---|---|---|---|---|---|---|---|---|
| 100/s | 99.7 | 2,000 | 0 | 60 ms | 94 ms | 6.0 / 24.5 ms | 0.39 cores | 11.7 |
| 200/s | 199.5 | 4,001 | 0 | 55 ms | 71 ms | 3.5 / 16.3 ms | 0.42 cores | 6.1 |
| 300/s | 299.2 | 6,001 | 0 | 53 ms | 60 ms | 2.0 / 8.1 ms | 0.41 cores | 3.7 |
| 400/s | 337.1 | 5,365 | 2,322 timeouts | 53 ms | 5.2 s | saturated | 0.88 cores | 3.8 |
| 500/s | 421.2 | 3,396 | 6,206 timeouts | - | 6.9 s | saturated | 0.97 cores | 4.4 |

Across 4 sweep runs: **200 req/s was clean every time** (overhead p95 5-16 ms); 300 req/s was
clean in 2 of 4 (the others coincided with load spikes); 400 req/s saturated in 3 of 4.
At saturation the gateway process sits at ~0.9-1.0 core: **one replica is CPU-bound on a single
core**, as expected for one Python process. Horizontal scaling is by replicas (all state is in
Redis/Postgres), but this laptop has no spare cores to demonstrate it; a 3-replica run
(`run_load_test.py scale`) was CPU-starved by the generator and Docker VM, so no scaling number
is claimed.

**Finding — overload collapses instead of degrading.** Past saturation, requests queue inside the
process until clients time out, so *accepted* traffic suffers too. Fix (next): admission control
(cap in-flight requests per replica, shed the excess immediately with 503 + Retry-After) so a
replica serves its capacity at normal latency and rejects the rest fast.
