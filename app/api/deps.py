from __future__ import annotations

import hmac
import time
import uuid
from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy import select

from app import metrics as m
from app.auth_cache import CachedKey
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


_WWW = {"WWW-Authenticate": "Bearer"}


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
    key_hash = hash_key(token)
    cache = request.app.state.auth_cache
    hit = await cache.get(key_hash)
    if hit is not None:
        if hit == "invalid":
            m.AUTH_FAILURES.labels("invalid").inc()
            raise HTTPException(401, "invalid or revoked api key", headers=_WWW)
        m.AUTH_CACHE.labels("hit").inc()
        return AuthContext(
            hit.tenant_uuid,
            uuid.UUID(hit.api_key_id),
            hit.rpm_limit,
            hit.tpm_limit,
            hit.monthly_token_budget,
        )

    m.AUTH_CACHE.labels("miss").inc()
    async with request.app.state.sessionmaker() as session:
        found = (
            await session.execute(
                select(ApiKey, Tenant.monthly_token_budget)
                .join(Tenant, Tenant.id == ApiKey.tenant_id)
                .where(ApiKey.key_hash == key_hash)
            )
        ).one_or_none()
    row, budget = found if found else (None, 0)
    if row is None or row.revoked_at is not None:
        m.AUTH_FAILURES.labels("invalid").inc()
        await cache.put_invalid(key_hash)
        raise HTTPException(401, "invalid or revoked api key", headers=_WWW)
    await cache.put(
        key_hash,
        CachedKey(str(row.tenant_id), str(row.id), row.rpm_limit, row.tpm_limit, budget),
    )
    return AuthContext(row.tenant_id, row.id, row.rpm_limit, row.tpm_limit, budget)


async def require_admin(authorization: str | None = Header(default=None)) -> None:
    token = _bearer(authorization)
    if not settings.admin_token:
        raise HTTPException(503, "admin api disabled (ADMIN_TOKEN not set)")
    if not token or not hmac.compare_digest(token, settings.admin_token):
        raise HTTPException(401, "invalid admin token")


Auth = Depends(authenticate)
