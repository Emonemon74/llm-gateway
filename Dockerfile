# One image, several commands: the API (default), the batch worker, migrations, the mock provider.
#   api:     uvicorn app.main:app           (default CMD)
#   worker:  python -m app.batch.worker
#   migrate: alembic upgrade head
#   mock:    uvicorn mock_provider.main:app
FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir uv==0.11.21
WORKDIR /app
# Dependencies first, so code-only changes reuse this cached layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

FROM python:3.13-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home /app app
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY alembic.ini ./
COPY app ./app
COPY mock_provider ./mock_provider
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER app
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
  CMD curl -fsS http://localhost:8000/healthz || exit 1
# One uvicorn process per container: scale by adding containers (state lives in Redis/Postgres).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
