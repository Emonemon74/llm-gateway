import logging

from fastapi import APIRouter, Depends, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.exceptions import RedisError

from app import metrics as m
from app.api.deps import require_admin
from app.batch.queue import BatchQueue

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/metrics", dependencies=[Depends(require_admin)], include_in_schema=False)
async def metrics(request: Request) -> Response:
    state = request.app.state
    # Circuit state lives in Redis (shared by all replicas), so read it at scrape time.
    for name in state.providers:
        s = await state.breaker.state(name)
        m.CIRCUIT_STATE.labels(name).set(m.CIRCUIT_VALUES.get(s, -1))
    try:
        queued, delayed, dlq = await BatchQueue(state.redis).depths()
        m.QUEUE_DEPTH.set(queued)
        m.DELAYED_DEPTH.set(delayed)
        m.DLQ_DEPTH.set(dlq)
    except (RedisError, OSError) as e:  # Redis down: keep last values, scrape must still succeed
        log.warning("queue depth unavailable: %r", e)
    m.USAGE_QUEUE.set(state.usage.depth)
    m.USAGE_DROPPED.set(state.usage.dropped)
    return Response(generate_latest(m.REGISTRY), media_type=CONTENT_TYPE_LATEST)
