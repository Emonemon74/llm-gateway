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
