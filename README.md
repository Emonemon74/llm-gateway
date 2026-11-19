# llm-gateway

Multi-tenant, OpenAI-compatible gateway in front of Groq / Gemini / Cerebras with
rate limits, budgets, failover, caching and a durable batch API.
Design and roadmap: [docs/PLAN.md](docs/PLAN.md).

## Status
Done: OpenAI-compatible proxy (+SSE), tenants and hashed API keys, Redis Lua rate limiting
(RPM+TPM), monthly token budgets (reserve/reconcile, rebuilt from Postgres), model aliases with
ordered failover, shared circuit breaker, async usage events.
Prometheus metrics + Grafana dashboard (incl. gateway-overhead latency).
Next: batch API on Redis Streams, semantic cache, tracing, load tests, deploy.

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
