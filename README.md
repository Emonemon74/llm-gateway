# llm-gateway

Multi-tenant, OpenAI-compatible gateway in front of Groq / Gemini / Cerebras with
rate limits, budgets, failover, caching and a durable batch API.
Design and roadmap: [docs/PLAN.md](docs/PLAN.md).

## Status
Done: OpenAI-compatible proxy (+SSE), tenants and hashed API keys, Redis Lua rate limiting
(RPM+TPM), monthly token budgets (reserve/reconcile, rebuilt from Postgres), model aliases with
ordered failover, shared circuit breaker, async usage events.
Next: Prometheus/Grafana, batch API on Redis Streams, semantic cache, load tests, deploy.

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
