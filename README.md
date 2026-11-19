# llm-gateway

Multi-tenant, OpenAI-compatible gateway in front of Groq / Gemini / Cerebras with
rate limits, budgets, failover, caching and a durable batch API.
Design and roadmap: [docs/PLAN.md](docs/PLAN.md).

## Status
Done: OpenAI-compatible proxy (+SSE), tenants and hashed API keys, Redis Lua rate limiting
(RPM+TPM), monthly token budgets (reserve/reconcile, rebuilt from Postgres), model aliases with
ordered failover, shared circuit breaker, async usage events.
Prometheus metrics + Grafana dashboard (incl. gateway-overhead latency).
Batch API on Redis Streams (retries, DLQ, crash recovery, chaos-tested).
Next: semantic cache, tracing, k6 load test, deploy.

## Run locally
```bash
uv sync
uv run uvicorn mock_provider.main:app --port 9000 &     # fake upstream
uv run uvicorn app.main:app --port 8000                 # gateway
curl localhost:8000/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"mock/test","messages":[{"role":"user","content":"hi"}]}'
uv run pytest
```
Model names are `<provider>/<model>` for now (e.g. `groq/llama-3.1-8b-instant`).

## Routing and failover
```bash
# alias "smart" -> try provider alpha, fall back to beta
curl -XPOST localhost:8000/admin/routes -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"alias":"smart","provider":"alpha","upstream_model":"m","priority":0}'
```
Extra OpenAI-compatible upstreams: `EXTRA_PROVIDERS='{"alpha":{"base_url":"http://...","api_key":"..."}}'`.
Responses carry `x-gateway-provider` and `x-request-id`. Streaming can only fail over before the
first byte is sent; a later upstream failure ends the stream with an SSE error event.

## Observability
```bash
docker compose -f deploy/docker-compose.yml up -d       # + Prometheus :9090, Grafana :3000
ADMIN_TOKEN=dev-admin uv run uvicorn app.main:app --port 8000
ADMIN_TOKEN=dev-admin uv run python loadtest/demo_traffic.py 30   # something to look at
open http://localhost:3000/d/llm-gateway                # dashboard "LLM Gateway" (no login in dev)
```
`/metrics` requires the admin token. Key series: `gateway_overhead_seconds` (latency the gateway
adds on top of the upstream), `gateway_upstream_requests_total{outcome}` (failovers),
`gateway_circuit_state`, `gateway_rate_limited_total`, per-tenant token and cost counters.
Label cardinality is bounded by design (see `app/metrics.py`).

## Batch API
```bash
# submit (Idempotency-Key makes a retried submit return the same batch)
curl -XPOST localhost:8000/v1/batches -H "Authorization: Bearer $KEY" -H "Idempotency-Key: job-1" \
  -H 'content-type: application/json' \
  -d '{"requests":[{"custom_id":"a","body":{"model":"smart","messages":[{"role":"user","content":"hi"}]}}]}'
curl localhost:8000/v1/batches/$ID            # progress
curl localhost:8000/v1/batches/$ID/results    # NDJSON, submission order
uv run python -m app.batch.worker             # run one or more workers (scale by adding processes)
```
**Guarantees.** Delivery is at-least-once (Redis Streams consumer group; a message is acked only
after its outcome is durable in Postgres; a dead worker's messages are reclaimed via `XAUTOCLAIM`).
Results are recorded exactly once (completion is a conditional `UPDATE`, so duplicates can't
double-write or double-count). The upstream call itself can repeat for work in flight when a
worker died. Retries use full-jitter exponential backoff, then dead-letter after `max_attempts`;
an open circuit or a rate limit *defers* an item without burning an attempt. Postgres is the
source of truth: a reconciler re-enqueues unfinished items Redis forgot (e.g. after a data wipe).
Batch work has its own rate-limit bucket (`BATCH_RATE_SHARE` of the key's limits) so it cannot
starve interactive traffic.

**Chaos-tested** (`tests/chaos`, real processes): `kill -9` a worker holding unacked messages,
kill every worker then start a new one, and `FLUSHALL` Redis mid-batch. A 400-item batch completes
with every result recorded once in all three; a typical `kill -9` run shows ~6-8 upstream calls
repeated for in-flight work.
