import asyncio
import os

# Must be set before any `app` import: settings are read at import time.
os.environ["DATABASE_URL"] = "postgresql+asyncpg://gateway:gateway@localhost:5432/gateway_test"
os.environ["ADMIN_TOKEN"] = "test-admin-token"
os.environ["REDIS_URL"] = "redis://localhost:6379/1"  # db 1: never touches dev data


import httpx
import pytest
import redis.asyncio as aioredis
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.pool import NullPool

from app.auth_cache import AuthCache
from app.core.config import settings
from app.db.session import make_engine, make_sessionmaker
from app.limits.budget import BudgetManager
from app.limits.rate_limiter import RateLimiter
from app.main import create_app
from app.providers.openai_compat import OpenAICompatProvider
from app.routing.breaker import CircuitBreaker
from app.routing.router import Router
from app.usage import UsageRecorder
from mock_provider.main import MockConfig, create_mock_app

ADMIN = {"Authorization": f"Bearer {settings.admin_token}"}


@pytest.fixture(scope="session", autouse=True)
def _migrated_db():
    """Apply real Alembic migrations to the test DB once per run."""
    command.upgrade(Config("alembic.ini"), "head")


MOCK_NAMES = ("mock", "alpha", "beta")


@pytest.fixture
def mocks() -> dict[str, MockConfig]:
    """Independent fake upstreams. Tests flip `.error_rate` etc. to break one on demand."""
    return {n: MockConfig(latency_ms=1) for n in MOCK_NAMES}


@pytest.fixture
async def app(mocks):
    app = create_app()
    app.state.providers = {
        n: OpenAICompatProvider(
            n,
            f"http://{n}/v1",
            "",
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_mock_app(cfg)), base_url=f"http://{n}"
            ),
        )
        for n, cfg in mocks.items()
    }
    engine = make_engine(settings.database_url, poolclass=NullPool)
    sm = make_sessionmaker(engine)
    app.state.sessionmaker = sm
    redis = aioredis.from_url(settings.redis_url)
    await redis.flushdb()
    app.state.redis = redis
    app.state.limiter = RateLimiter(redis)
    app.state.auth_cache = AuthCache(redis)
    app.state.budget = BudgetManager(redis, sm)
    # short cooldown so breaker recovery is testable; threshold 3 keeps tests small
    app.state.breaker = CircuitBreaker(redis, threshold=3, cooldown_s=0.4, probe_timeout_s=5)
    app.state.router = Router(app.state.providers, app.state.breaker, sm, route_ttl_s=0)
    app.state.usage = UsageRecorder(sm, linger_s=0.01)
    app.state.usage.start()
    yield app
    await app.state.usage.stop()
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE tenants, model_routes CASCADE"))
    await redis.flushdb()
    await redis.aclose()
    await engine.dispose()


@pytest.fixture
async def client(app):
    """Unauthenticated client."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


@pytest.fixture
async def api_key(client) -> str:
    t = await client.post("/admin/tenants", json={"name": "acme"}, headers=ADMIN)
    k = await client.post(f"/admin/tenants/{t.json()['id']}/keys", json={}, headers=ADMIN)
    return k.json()["key"]


@pytest.fixture
async def gateway(app, api_key):
    """Authenticated client (a valid tenant key is attached)."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gw",
        headers={"Authorization": f"Bearer {api_key}"},
    ) as c:
        yield c


@pytest.fixture
def limiter(app) -> RateLimiter:
    return app.state.limiter


@pytest.fixture
async def run_worker(app):
    """Start in-process batch workers; stopped (gracefully) at teardown."""
    from app.batch.worker import Worker

    started = []

    async def start(**kw):
        defaults = {
            "concurrency": 8,
            "block_ms": 100,
            "delayed_every_s": 0.05,
            "reclaim_every_s": 0.1,
            "reconcile_every_s": 0.1,
            "reconcile_after_s": 60,
            "backoff_base_s": 0.02,
            "backoff_cap_s": 0.1,
            "unavailable_delay_s": 0.1,
            "visibility_timeout_s": 30,
        }
        w = Worker(app.state, **(defaults | kw))
        stop = asyncio.Event()
        task = asyncio.create_task(w.run(stop))
        started.append((stop, task))
        return w

    yield start
    for stop, _ in started:
        stop.set()
    for _, task in started:
        await asyncio.wait_for(task, 10)
