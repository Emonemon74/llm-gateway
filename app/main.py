from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import admin, chat
from app.core.config import settings
from app.db.session import make_engine, make_sessionmaker
from app.providers.registry import build_providers


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.providers = build_providers()
    engine = make_engine(settings.database_url)
    app.state.sessionmaker = make_sessionmaker(engine)
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="llm-gateway", lifespan=lifespan)
    app.include_router(chat.router)
    app.include_router(admin.router)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


app = create_app()
