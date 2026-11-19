# llm-gateway — Project Plan

Working name: **llm-gateway**. A multi-tenant, OpenAI-compatible gateway that sits in front of several LLM providers (Groq, Gemini, Cerebras) and adds the production concerns they don't give you: per-tenant auth, rate limits, budgets, failover, caching, durable batch processing and observability.

Target outcome: a deployed, load-tested system with numbers you can quote in an interview and on a resume.

---

## 1. Goals and non-goals

**Goals**
- One endpoint (`/v1/chat/completions`) that is drop-in compatible with the OpenAI SDK.
- Per-tenant API keys, rate limits (requests/min and tokens/min) and monthly token budgets.
- Automatic provider failover with a circuit breaker.
- Exact + semantic response cache with measured hit rate and cost saved.
- Async batch API with at-least-once processing, retries, idempotency and a dead-letter queue.
- Streaming (SSE) pass-through.
- Observability: traces, metrics, structured logs, a Grafana dashboard.
- Measured performance (k6) and measured resilience (chaos tests).

**Non-goals (say these out loud in interviews, scoping is a skill)**
- No model hosting or fine-tuning.
- No billing/payments, only usage metering.
- No multi-region. Single region, horizontally scalable stateless API.
- No fancy UI. A small admin dashboard at most.

---

## 2. Architecture

```
                    ┌──────────────────────────────────────────┐
  Client / SDK ───► │  API  (FastAPI, async, N replicas)       │
  (OpenAI-compat)   │  auth → rate limit → budget → cache →    │
                    │  router → provider adapter → stream      │
                    └───┬──────────────┬───────────────┬───────┘
                        │              │               │
                        ▼              ▼               ▼
                  ┌──────────┐   ┌───────────┐   ┌─────────────────────┐
                  │  Redis   │   │ Postgres  │   │ Providers           │
                  │ - token  │   │ - tenants │   │ Groq / Gemini /     │
                  │   bucket │   │ - keys    │   │ Cerebras / Mock     │
                  │ - budget │   │ - usage   │   └─────────────────────┘
                  │ - breaker│   │ - jobs    │             ▲
                  │ - streams│   │ - cache   │             │
                  └────┬─────┘   │ (pgvector)│             │
                       │         └─────▲─────┘             │
                       ▼               │                   │
                  ┌──────────────────────────┐             │
                  │ Worker pool (M replicas) │─────────────┘
                  │ consumes Redis Streams   │
                  │ retries, backoff, DLQ    │
                  └───────────┬──────────────┘
                              │ events (budget exceeded, job done)
                              ▼
                  ┌──────────────────────────┐
                  │ notification-service     │  (your existing repo, optional)
                  └──────────────────────────┘

  Cross-cutting: OpenTelemetry → Collector → Tempo/Jaeger, Prometheus → Grafana
```

### Components

| Component | Tech | Responsibility |
|---|---|---|
| API | FastAPI + httpx (async) | Stateless. Auth, limits, routing, streaming, batch submission. |
| Worker | Python asyncio, Redis Streams consumer groups | Process batch items, retry, DLQ. |
| Postgres | Postgres 16 + pgvector | Source of truth: tenants, keys, usage, jobs, cache embeddings. |
| Redis | Redis 7 | Hot state: rate limit buckets, budget counters, circuit breaker state, job queue. |
| Mock provider | Small FastAPI app | Configurable latency and error rate. **Required** for load tests and chaos tests, you can't load test free-tier APIs. |
| Observability | OTel, Prometheus, Grafana | Metrics and traces. |

### Why these choices (interview answers)
- **Redis Streams over Celery/Arq:** consumer groups give at-least-once delivery, pending-entry lists and `XAUTOCLAIM` for recovering from dead workers. Building the retry/DLQ logic yourself is the learning. Trade-off: more code than Celery; no built-in scheduling.
- **Postgres as source of truth, Redis as cache/hot state:** Redis can lose data; usage and jobs must not be lost. Budget counters in Redis are periodically reconciled against `usage_events` in Postgres.
- **Stateless API:** any replica can serve any request, so scaling is just adding replicas behind a load balancer. All shared state lives in Redis/Postgres.
- **Python/FastAPI:** your strongest stack; async I/O fits a proxy workload (mostly waiting on upstream).

---

## 3. Data model (Postgres)

```
tenants        (id, name, monthly_token_budget, created_at)
api_keys       (id, tenant_id, key_prefix, key_hash, rpm_limit, tpm_limit,
                revoked_at, created_at)            -- store SHA-256 hash only
usage_events   (id, tenant_id, api_key_id, request_id, provider, model,
                prompt_tokens, completion_tokens, cost_usd, cache_status,
                latency_ms, status, created_at)    -- partition by month later
provider_models(alias, provider, upstream_model, priority, cost_per_1k_in,
                cost_per_1k_out, enabled)           -- routing table
batches        (id, tenant_id, status, total, completed, failed,
                created_at, finished_at)
batch_items    (id, batch_id, idempotency_key UNIQUE, request_json,
                response_json, status, attempts, last_error,
                next_attempt_at)
cache_entries  (id, tenant_id, prompt_hash, embedding vector(N),
                model_alias, response_json, created_at, hit_count)
```

Indexes: `api_keys(key_hash)`, `usage_events(tenant_id, created_at)`, `batch_items(batch_id, status)`, HNSW or IVFFlat on `cache_entries.embedding`.

---

## 4. Request flow (sync path)

1. **Auth:** parse `Authorization: Bearer sk-...`, SHA-256, look up in Redis cache (fallback Postgres). Reject revoked.
2. **Rate limit:** atomic token bucket in a Redis **Lua script** (RPM) plus a token-per-minute bucket (TPM, estimated from prompt length, reconciled after response). Return `429` with `Retry-After`.
3. **Budget check:** `reserve` estimated tokens against the tenant's monthly counter in Redis; reject with `402`/`429` if exceeded. After the response, `reconcile` actual usage (refund the difference).
4. **Cache lookup** (only when `temperature == 0` and not streaming, to start):
   - exact match on hash of (tenant, model, messages) → hit
   - else embed prompt, pgvector cosine search, scoped **per tenant**, threshold e.g. 0.95 → hit
5. **Route:** map the model alias to an ordered provider list; skip providers whose circuit is open.
6. **Call provider** with timeout. On 429/5xx/timeout: record failure on the breaker, try next provider.
7. **Respond / stream:** SSE pass-through; count tokens from the final chunk or by tokenizer.
8. **Record:** write `usage_events` asynchronously (don't block the response), update metrics, store cache entry.

### Key design decisions to document in the README
- **Failover after first streamed byte is impossible.** Failover happens only before streaming starts. Mid-stream failure returns an error chunk. State this trade-off explicitly.
- **Fail-open vs fail-closed on Redis outage:** rate limiter fails **open** with a conservative local in-memory limit (availability over strictness); budget check fails **closed** for tenants near their limit. Document the choice.
- **Token bucket vs sliding window:** token bucket allows bursts, O(1) state per key, easy in Lua. Mention sliding-window-log as the more precise but costlier alternative.
- **Cache poisoning / data isolation:** cache is strictly per tenant. Never share cached answers across tenants.
- **Circuit breaker states:** closed → open after N failures in window → half-open after cooldown (allow 1 probe) → closed on success. State lives in Redis so all API replicas agree.

---

## 5. Batch / async path

```
POST /v1/batches  (JSONL, up to 10k lines, Idempotency-Key header)
   → insert batch + batch_items (status=pending) in one transaction
   → XADD each item id to stream  "batch:items"
Workers (consumer group "workers"):
   XREADGROUP → mark in_progress → call provider (reuses the same router,
   rate limits and budget as the sync path) → store response → XACK
   on failure: attempts++ , next_attempt_at = now + backoff*2^n + jitter
               re-enqueue via a delayed set (ZSET scored by next_attempt_at)
   after max_attempts: move to DLQ stream "batch:dlq", status=failed
Janitor loop: XAUTOCLAIM entries idle > visibility timeout (dead worker)
GET /v1/batches/{id}  → progress; GET .../results → JSONL
```

**Guarantees to state:** at-least-once delivery; **idempotent** effects via `idempotency_key UNIQUE` and "only write response if status != done"; so effectively-once results.

**Backpressure:** workers share the tenant's rate limiter with the sync path so a huge batch can't starve interactive traffic (use a lower priority / separate bucket share for batch).

---

## 6. Observability

**Metrics (Prometheus):** `gateway_requests_total{tenant,provider,model,status,cache}`, `gateway_request_duration_seconds` histogram, `gateway_upstream_duration_seconds{provider}`, `gateway_tokens_total{tenant,direction}`, `gateway_cost_usd_total{tenant}`, `gateway_circuit_state{provider}`, `gateway_rate_limited_total`, `gateway_queue_depth`, `gateway_dlq_depth`, `gateway_cache_hits_total`.

**Traces (OpenTelemetry):** one span per stage (auth, ratelimit, cache, route, upstream, record) so you can *show* where latency goes. Propagate `request_id`.

**Logs:** structured JSON with `request_id`, `tenant_id`, never log prompts or keys by default.

**Grafana dashboard:** RPS, p50/p95/p99, error rate, per-provider latency, breaker state, cache hit rate, cost per tenant, queue depth.

**Gateway overhead metric:** `request_duration - upstream_duration` is the number to brag about (e.g. "gateway adds < 8 ms p95").

---

## 7. Testing strategy

| Layer | What |
|---|---|
| Unit | token bucket math, backoff calc, breaker state machine, key hashing, cache threshold logic |
| Integration | real Postgres + Redis via Docker (testcontainers or compose) : auth, limits, budget reserve/reconcile, batch lifecycle |
| Contract | run the official `openai` Python SDK against the gateway (proves compatibility) |
| Load (k6) | against the **mock provider** with fixed 200 ms latency: ramp to find saturation; report RPS, p95, error rate |
| Chaos | (a) mock provider returns 50% 500s → failover success rate; (b) `kill -9` a worker mid-batch → all items still complete exactly once; (c) restart Redis → system recovers, no data loss in Postgres |

CI (GitHub Actions): lint (ruff), type check (mypy), unit + integration tests, build Docker image. Deploy job optional.

---

## 8. Performance targets (adjust to what you measure, never claim unmeasured numbers)

- Gateway overhead: p95 < 10 ms (excluding upstream)
- Throughput: ≥ 1,000 req/s on a small instance against the mock provider
- Failover: ≥ 99% success with one provider at 50% error rate
- Batch: 10k items complete with zero loss after worker kill
- Cache: report hit rate on a replayed workload and the $ saved

---

## 9. Repo structure

```
llm-gateway/
├── app/
│   ├── main.py                # FastAPI app factory
│   ├── api/                   # routes: chat.py, batches.py, admin.py, health.py
│   ├── core/                  # config, security (key hashing), logging, otel
│   ├── limits/                # token_bucket.py (+ lua/), budget.py
│   ├── providers/             # base.py, groq.py, gemini.py, cerebras.py, mock.py
│   ├── routing/               # router.py, circuit_breaker.py
│   ├── cache/                 # exact.py, semantic.py, embeddings.py
│   ├── batch/                 # service.py, worker.py, janitor.py
│   ├── db/                    # models.py, migrations/ (alembic), session.py
│   └── metrics.py
├── mock_provider/             # separate tiny app for load/chaos tests
├── tests/{unit,integration,contract,chaos}/
├── loadtest/                  # k6 scripts + results/
├── deploy/                    # docker-compose.yml, prometheus.yml, grafana/, Caddyfile
├── docs/                      # PLAN.md, ARCHITECTURE.md, ADRs/, benchmarks.md
├── .github/workflows/ci.yml
├── pyproject.toml  (uv)
└── README.md                  # lead with diagram, live URL, benchmark table
```

Keep short **ADRs** (architecture decision records) in `docs/ADRs/`: "Why Redis Streams", "Fail-open rate limiter", "Per-tenant cache isolation". Interviewers love these.

---

## 10. Roadmap (about 4 weeks, adjust to your pace)

### Week 1: Core proxy + auth
- [ ] Scaffold repo, `uv`, Docker Compose (Postgres, Redis), CI skeleton
- [ ] Alembic migrations for tenants/api_keys/usage_events
- [ ] Provider adapter interface + Groq adapter + **mock provider**
- [ ] `/v1/chat/completions` non-streaming, then SSE streaming
- [ ] API key issue/revoke (admin endpoint), hashed storage, auth middleware
- [ ] Test with the official OpenAI SDK
- **Exit criteria:** `openai` SDK works against the gateway with a gateway-issued key.

### Week 2: Protection layer
- [ ] Redis Lua token bucket (RPM + TPM), `429` + `Retry-After`
- [ ] Monthly token budget: reserve → reconcile
- [ ] Second and third provider adapters (Gemini, Cerebras)
- [ ] Router with ordered fallback + circuit breaker in Redis
- [ ] Usage events written async
- [ ] Prometheus metrics + first Grafana dashboard
- **Exit criteria:** kill/force-error one provider, traffic keeps flowing; limits and budgets enforced with tests.

### Week 3: Batch + cache
- [ ] Batch API, Redis Streams worker, retries with backoff + jitter, DLQ, janitor (`XAUTOCLAIM`)
- [ ] Idempotency keys
- [ ] Exact cache, then semantic cache (pgvector), per-tenant isolation
- [ ] OpenTelemetry tracing
- [ ] Chaos tests: worker kill, provider errors, Redis restart
- **Exit criteria:** 10k-item batch survives `kill -9` of a worker with zero loss/duplication.

### Week 4: Prove it + ship it
- [ ] k6 load tests, tune (connection pooling, uvloop, workers), record results in `docs/benchmarks.md`
- [ ] Deploy (EC2 + Caddy + `nip.io` like your support platform, or Railway), live demo with a demo key
- [ ] README: diagram, benchmark table, design decisions, "what I'd do next"
- [ ] Optional: hook `notification-service` for budget/outage alerts; small admin dashboard
- [ ] Record a 2–3 minute demo (curl + Grafana)
- **Exit criteria:** public URL, CI green, benchmark table in README.

### Stretch (only if time)
- Plug in the Java/Spring Boot distributed rate limiter as the limits service (gRPC or HTTP)
- Prompt-injection / PII redaction middleware
- Per-tenant model allow-lists, A/B routing
- Postgres partitioning of `usage_events`

---

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Free-tier provider limits during tests | Mock provider for all load/chaos tests; real providers only for a smoke test |
| Scope creep | Stick to week exit criteria; stretch items only after week 4 |
| Redis Streams complexity | Build the happy path first, add retry/DLQ/janitor incrementally with a test per behavior |
| Token counting inaccuracy | Estimate on reserve, reconcile with provider-reported usage; document the approximation |
| Embedding cost/latency for semantic cache | Exact-match first; semantic only on exact miss; measure added latency honestly |

---

## 12. Resume bullets (fill in the numbers you actually measure)

- Built a multi-tenant, OpenAI-compatible LLM gateway (FastAPI, Redis, Postgres/pgvector) serving **X req/s at p95 < Y ms** gateway overhead under k6 load.
- Implemented Redis Lua token-bucket rate limiting and reserve/reconcile token budgets, enforced per tenant across **N** stateless API replicas.
- Designed provider failover with a shared circuit breaker, sustaining **Z%** success while one provider ran at 50% error rate.
- Built an at-least-once batch pipeline on Redis Streams (retries with jittered backoff, DLQ, idempotent writes) that completed **10k/10k** items after killing a worker mid-run.
- Added per-tenant semantic caching reducing LLM spend by **X%** on a replayed workload; instrumented with OpenTelemetry/Prometheus/Grafana.

## 13. Interview questions this project prepares you for
Design a rate limiter · Design a job queue / what does at-least-once mean · How do you make retries safe (idempotency) · Cache invalidation and isolation · Circuit breaker states · Fail-open vs fail-closed · How would you scale this to 10× · Where is your bottleneck and how did you find it · What happens when Redis dies.
