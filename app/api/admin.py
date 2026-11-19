from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.api.deps import require_admin
from app.core.security import generate_key
from app.db.models import ApiKey, ModelRoute, Tenant

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)], tags=["admin"])


class TenantIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    monthly_token_budget: int = Field(default=1_000_000, ge=0)


class KeyIn(BaseModel):
    rpm_limit: int = Field(default=60, ge=1)
    tpm_limit: int = Field(default=100_000, ge=1)


@router.post("/tenants", status_code=201)
async def create_tenant(body: TenantIn, request: Request):
    async with request.app.state.sessionmaker() as s:
        t = Tenant(name=body.name, monthly_token_budget=body.monthly_token_budget)
        s.add(t)
        try:
            await s.commit()
        except IntegrityError:
            raise HTTPException(409, "tenant name already exists") from None
        return {"id": str(t.id), "name": t.name, "monthly_token_budget": t.monthly_token_budget}


@router.post("/tenants/{tenant_id}/keys", status_code=201)
async def create_key(tenant_id: uuid.UUID, body: KeyIn, request: Request):
    async with request.app.state.sessionmaker() as s:
        if await s.get(Tenant, tenant_id) is None:
            raise HTTPException(404, "tenant not found")
        plaintext, prefix, digest = generate_key()
        k = ApiKey(
            tenant_id=tenant_id,
            key_prefix=prefix,
            key_hash=digest,
            rpm_limit=body.rpm_limit,
            tpm_limit=body.tpm_limit,
        )
        s.add(k)
        await s.commit()
        # The only time the plaintext key is ever returned.
        return {"id": str(k.id), "key": plaintext, "prefix": prefix}


@router.get("/tenants/{tenant_id}/keys")
async def list_keys(tenant_id: uuid.UUID, request: Request):
    async with request.app.state.sessionmaker() as s:
        rows = (await s.execute(select(ApiKey).where(ApiKey.tenant_id == tenant_id))).scalars()
        return [
            {
                "id": str(k.id),
                "prefix": k.key_prefix,
                "revoked": k.revoked_at is not None,
                "rpm_limit": k.rpm_limit,
                "tpm_limit": k.tpm_limit,
            }
            for k in rows
        ]


@router.delete("/keys/{key_id}", status_code=204)
async def revoke_key(key_id: uuid.UUID, request: Request):
    async with request.app.state.sessionmaker() as s:
        k = await s.get(ApiKey, key_id)
        if k is None:
            raise HTTPException(404, "key not found")
        if k.revoked_at is None:
            k.revoked_at = datetime.now(UTC)
            await s.commit()


class RouteIn(BaseModel):
    alias: str = Field(min_length=1, max_length=100)
    provider: str = Field(min_length=1, max_length=50)
    upstream_model: str = Field(min_length=1, max_length=200)
    priority: int = 0  # lower = tried first
    cost_per_1k_in: float = Field(default=0.0, ge=0)
    cost_per_1k_out: float = Field(default=0.0, ge=0)


@router.post("/routes", status_code=201)
async def create_route(body: RouteIn, request: Request):
    async with request.app.state.sessionmaker() as s:
        r = ModelRoute(**body.model_dump())
        s.add(r)
        await s.commit()
    request.app.state.router.invalidate()
    return {"id": str(r.id), **body.model_dump()}


@router.get("/routes")
async def list_routes(request: Request):
    async with request.app.state.sessionmaker() as s:
        rows = (
            await s.execute(select(ModelRoute).order_by(ModelRoute.alias, ModelRoute.priority))
        ).scalars()
        return [
            {
                "id": str(r.id),
                "alias": r.alias,
                "provider": r.provider,
                "upstream_model": r.upstream_model,
                "priority": r.priority,
                "enabled": r.enabled,
            }
            for r in rows
        ]


@router.delete("/routes/{route_id}", status_code=204)
async def delete_route(route_id: uuid.UUID, request: Request):
    async with request.app.state.sessionmaker() as s:
        r = await s.get(ModelRoute, route_id)
        if r is None:
            raise HTTPException(404, "route not found")
        await s.delete(r)
        await s.commit()
    request.app.state.router.invalidate()


@router.get("/tenants/{tenant_id}/usage")
async def tenant_usage(tenant_id: uuid.UUID, request: Request):
    async with request.app.state.sessionmaker() as s:
        t = await s.get(Tenant, tenant_id)
    if t is None:
        raise HTTPException(404, "tenant not found")
    used = await request.app.state.budget.used(tenant_id)
    return {
        "tenant_id": str(tenant_id),
        "monthly_token_budget": t.monthly_token_budget,
        "used_tokens": used,
        "remaining_tokens": max(0, t.monthly_token_budget - used),
    }
