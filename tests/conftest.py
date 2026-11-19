import os

# Must be set before any `app` import: settings are read at import time.
os.environ["DATABASE_URL"] = "postgresql+asyncpg://gateway:gateway@localhost:5432/gateway_test"
os.environ["ADMIN_TOKEN"] = "test-admin-token"

import importlib

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.db.session import make_engine, make_sessionmaker
from app.main import create_app
from app.providers.openai_compat import OpenAICompatProvider

ADMIN = {"Authorization": f"Bearer {settings.admin_token}"}


@pytest.fixture(scope="session", autouse=True)
def _migrated_db():
    """Apply real Alembic migrations to the test DB once per run."""
    command.upgrade(Config("alembic.ini"), "head")


def make_app(mock_app):
    """Gateway app wired in-process to a mock-provider ASGI app (no sockets)."""
    upstream = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mock_app), base_url="http://mock"
    )
    app = create_app()
    app.state.providers = {
        "mock": OpenAICompatProvider("mock", "http://mock/v1", "", client=upstream)
    }
    return app


@pytest.fixture
def mock_module(monkeypatch):
    monkeypatch.setenv("MOCK_LATENCY_MS", "1")
    import mock_provider.main as m

    return importlib.reload(m)


@pytest.fixture
async def app(mock_module):
    app = make_app(mock_module.app)
    engine = make_engine(settings.database_url, poolclass=NullPool)
    app.state.sessionmaker = make_sessionmaker(engine)
    yield app
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE tenants CASCADE"))
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
