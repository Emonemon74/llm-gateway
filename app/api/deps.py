from __future__ import annotations

import hmac
import time
import uuid
from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy import select

from app import metrics as m
from app.core.config import settings
from app.core.security import hash_key
from app.db.models import ApiKey, Tenant


@dataclass(frozen=True)
class AuthContext:
    tenant_id: uuid.UUID
    api_key_id: uuid.UUID
    rpm_limit: int
    tpm_limit: int
    monthly_token_budget: int


def _bearer(authorization: str | None) -> str | None:
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


async def authenticate(
    request: Request, authorization: str | None = Header(default=None)
) -> AuthContext:
    request.state.t0 = time.monotonic()  # request clock starts here (auth is gateway overhead)
    token = _bearer(authorization)
    if not token:
        m.AUTH_FAILURES.labels("missing").inc()
        raise HTTPException(401, "missing bearer token", headers={"WWW-Authenticate": "Bearer"})
    async with request.app.state.sessionmaker() as session:
        found = (
            await session.execute(
                select(ApiKey, Tenant.monthly_token_budget)
                .join(Tenant, Tenant.id == ApiKey.tenant_id)
                .where(ApiKey.key_hash == hash_key(token))
            )
        ).one_or_none()
    row, budget = found if found else (None, 0)
    if row is None or row.revoked_at is not None:
        m.AUTH_FAILURES.labels("invalid").inc()
        raise HTTPException(
            401, "invalid or revoked api key", headers={"WWW-Authenticate": "Bearer"}
        )
    return AuthContext(row.tenant_id, row.id, row.rpm_limit, row.tpm_limit, budget)


async def require_admin(authorization: str | None = Header(default=None)) -> None:
    token = _bearer(authorization)
    if not settings.admin_token:
        raise HTTPException(503, "admin api disabled (ADMIN_TOKEN not set)")
    if not token or not hmac.compare_digest(token, settings.admin_token):
        raise HTTPException(401, "invalid admin token")


Auth = Depends(authenticate)
