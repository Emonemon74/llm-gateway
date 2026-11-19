# llm-gateway

Multi-tenant, OpenAI-compatible gateway in front of Groq / Gemini / Cerebras with
rate limits, budgets, failover, caching and a durable batch API.
Design and roadmap: [docs/PLAN.md](docs/PLAN.md).

## Status
Week 1 in progress: proxy endpoint (non-streaming + SSE) with a mock provider. Auth, limits,
failover, batch and cache are next, per the plan.

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
