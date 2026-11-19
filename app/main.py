from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api import admin, chat
from app.core.config import settings
from app.db.session import make_engine, make_sessionmaker
from app.limits.budget import BudgetManager
from app.limits.rate_limiter import RateLimiter
from app.providers.registry import build_providers
from app.routing.breaker import CircuitBreaker
from app.routing.router import Router
from app.usage import UsageRecorder


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.providers = build_providers()
    engine = make_engine(settings.database_url)
    app.state.sessionmaker = make_sessionmaker(engine)
    redis = aioredis.from_url(settings.redis_url, socket_timeout=0.25, socket_connect_timeout=0.25)
    app.state.redis = redis
    app.state.limiter = RateLimiter(redis)
    app.state.budget = BudgetManager(redis, app.state.sessionmaker)
    app.state.router = Router(
        app.state.providers,
        CircuitBreaker(
            redis,
            settings.breaker_failure_threshold,
            settings.breaker_cooldown_s,
            settings.breaker_probe_timeout_s,
        ),
        app.state.sessionmaker,
    )
    app.state.usage = UsageRecorder(app.state.sessionmaker)
    app.state.usage.start()
    yield
    await app.state.usage.stop()
    await redis.aclose()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="llm-gateway", lifespan=lifespan)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_, exc: StarletteHTTPException):
        # OpenAI-style error envelope when the route supplied one; FastAPI default otherwise.
        body = (
            exc.detail
            if isinstance(exc.detail, dict) and "error" in exc.detail
            else {"error": {"type": "gateway_error", "message": str(exc.detail)}}
        )
        return JSONResponse(body, status_code=exc.status_code, headers=exc.headers)

    app.include_router(chat.router)
    app.include_router(admin.router)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
