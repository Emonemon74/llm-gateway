from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api import admin, batches, chat, metrics
from app.runtime import Runtime


@asynccontextmanager
async def lifespan(app: FastAPI):
    rt = await Runtime.create()
    for name, value in vars(rt).items():
        setattr(app.state, name, value)
    yield
    await rt.close()


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
    app.include_router(batches.router)
    app.include_router(metrics.router)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
