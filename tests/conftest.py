import httpx
import pytest

from app.main import create_app
from app.providers.openai_compat import OpenAICompatProvider


def make_gateway(mock_app) -> httpx.AsyncClient:
    """Gateway under test, wired in-process to a mock-provider ASGI app (no sockets)."""
    upstream = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_app),
                                 base_url="http://mock")
    app = create_app()
    app.state.providers = {
        "mock": OpenAICompatProvider("mock", "http://mock/v1", "", client=upstream)
    }
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


@pytest.fixture
def mock_module(monkeypatch):
    monkeypatch.setenv("MOCK_LATENCY_MS", "1")
    import importlib

    import mock_provider.main as m
    return importlib.reload(m)


@pytest.fixture
async def gateway(mock_module):
    async with make_gateway(mock_module.app) as c:
        yield c
