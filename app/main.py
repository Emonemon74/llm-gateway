from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import chat
from app.providers.registry import build_providers


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.providers = build_providers()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="llm-gateway", lifespan=lifespan)
    app.include_router(chat.router)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
