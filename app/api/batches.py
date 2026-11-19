from __future__ import annotations

import json
import uuid
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.api.chat import ChatRequest, _error
from app.api.deps import Auth, AuthContext
from app.batch.service import MAX_ITEMS, submit
from app.db.models import Batch, BatchItem
from app.routing.router import NoRoute

router = APIRouter(prefix="/v1/batches", tags=["batches"])


class BatchRequestItem(BaseModel):
    custom_id: str = Field(min_length=1, max_length=64)
    body: ChatRequest


class BatchIn(BaseModel):
    requests: list[BatchRequestItem] = Field(min_length=1, max_length=MAX_ITEMS)
    max_attempts: int = Field(default=5, ge=1, le=10)


def _view(b: Batch) -> dict[str, Any]:
    return {
        "id": str(b.id),
        "status": b.status,
        "total": b.total,
        "completed": b.completed,
        "failed": b.failed,
        "max_attempts": b.max_attempts,
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "finished_at": b.finished_at.isoformat() if b.finished_at else None,
    }


@router.post("", status_code=202)
async def create_batch(
    body: BatchIn,
    request: Request,
    response: Response,
    auth: AuthContext = Auth,
    idempotency_key: str | None = Header(default=None, max_length=100),
):
    state = request.app.state
    ids = [r.custom_id for r in body.requests]
    if len(set(ids)) != len(ids):
        raise _error(422, "invalid_request", "custom_id values must be unique within a batch")
    if any(r.body.stream for r in body.requests):
        raise _error(422, "invalid_request", "streaming is not supported in batches")

    unknown = []
    for model in {r.body.model for r in body.requests}:
        try:  # fail the whole submit now rather than N failed items later
            await state.router.targets(model)
        except NoRoute:
            unknown.append(model)
    if unknown:
        raise _error(422, "model_not_found", f"unknown model(s): {sorted(unknown)}")

    result = await submit(
        state,
        auth,
        [(r.custom_id, r.body.model_dump()) for r in body.requests],
        idempotency_key,
        body.max_attempts,
    )
    if not result.created:
        response.status_code = 200  # same Idempotency-Key: here is the batch you already made
    return _view(result.batch)


async def _owned(request: Request, auth: AuthContext, batch_id: uuid.UUID) -> Batch:
    async with request.app.state.sessionmaker() as s:
        b = await s.get(Batch, batch_id)
    if b is None or b.tenant_id != auth.tenant_id:  # 404, not 403: don't reveal other tenants' ids
        raise HTTPException(404, {"error": {"type": "not_found", "message": "batch not found"}})
    return b


@router.get("/{batch_id}")
async def get_batch(batch_id: uuid.UUID, request: Request, auth: AuthContext = Auth):
    return _view(await _owned(request, auth, batch_id))


@router.get("/{batch_id}/results")
async def batch_results(batch_id: uuid.UUID, request: Request, auth: AuthContext = Auth):
    """NDJSON, one line per item in submission order. Unfinished items show their status."""
    await _owned(request, auth, batch_id)
    sm = request.app.state.sessionmaker

    async def gen():
        last = -1
        while True:  # keyset pagination: constant memory for a 10k-item batch
            async with sm() as s:
                rows = (
                    (
                        await s.execute(
                            select(BatchItem)
                            .where(BatchItem.batch_id == batch_id, BatchItem.idx > last)
                            .order_by(BatchItem.idx)
                            .limit(500)
                        )
                    )
                    .scalars()
                    .all()
                )
            if not rows:
                return
            for r in rows:
                line: dict[str, Any] = {"custom_id": r.custom_id, "status": r.status}
                if r.status == "done":
                    line["response"] = r.response_json
                elif r.status == "failed":
                    line["error"] = r.last_error
                yield json.dumps(line) + "\n"
            last = rows[-1].idx

    return StreamingResponse(gen(), media_type="application/x-ndjson")
