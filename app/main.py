from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api import admin, batches, chat, metrics
from app.core.config import settings
from app.runtime import Runtime
from app.shedding import LoadShedder, SheddingMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    rt = await Runtime.create()
    for name, value in vars(rt).items():
        setattr(app.state, name, value)
    app.state.shedder = LoadShedder(
        settings.shed_max_inflight, settings.shed_max_loop_lag_ms / 1000
    )
    app.state.shedder.start()
    yield
    await app.state.shedder.stop()
    await rt.close()


def create_app() -> FastAPI:
    app = FastAPI(title="llm-gateway", lifespan=lifespan)
    app.add_middleware(SheddingMiddleware)

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
    app.include_router(batches.router)
    app.include_router(metrics.router)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
