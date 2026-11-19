from fastapi import APIRouter, Depends, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app import metrics as m
from app.api.deps import require_admin

router = APIRouter()


@router.get("/metrics", dependencies=[Depends(require_admin)], include_in_schema=False)
async def metrics(request: Request) -> Response:
    state = request.app.state
    # Circuit state lives in Redis (shared by all replicas), so read it at scrape time.
    for name in state.providers:
        s = await state.breaker.state(name)
        m.CIRCUIT_STATE.labels(name).set(m.CIRCUIT_VALUES.get(s, -1))
    m.USAGE_QUEUE.set(state.usage.depth)
    m.USAGE_DROPPED.set(state.usage.dropped)
    return Response(generate_latest(m.REGISTRY), media_type=CONTENT_TYPE_LATEST)
