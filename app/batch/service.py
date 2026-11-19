from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from app.api.deps import AuthContext
from app.batch.queue import BatchQueue
from app.db.models import Batch, BatchItem

log = logging.getLogger(__name__)
MAX_ITEMS = 10_000


@dataclass
class Submitted:
    batch: Batch
    created: bool  # False: an earlier submit with the same Idempotency-Key already made it


async def submit(
    state: Any,
    auth: AuthContext,
    items: list[tuple[str, dict[str, Any]]],  # (custom_id, chat body)
    idempotency_key: str | None,
    max_attempts: int,
) -> Submitted:
    """Persist the batch and its items (one transaction), then enqueue them.

    Postgres first, Redis second: if we crash between the two, the items exist in the database
    and the reconciler enqueues them. If it were the other way round we could enqueue ids that
    no row backs. The 202 therefore only promises "durably accepted", not "already queued".
    """
    batch_id = uuid.uuid4()
    item_ids = [uuid.uuid4() for _ in items]
    async with state.sessionmaker() as s:
        try:
            async with s.begin():
                s.add(
                    Batch(
                        id=batch_id,
                        tenant_id=auth.tenant_id,
                        api_key_id=auth.api_key_id,
                        idempotency_key=idempotency_key,
                        total=len(items),
                        max_attempts=max_attempts,
                    )
                )
                await s.flush()  # the batch row must exist before its items reference it
                await s.execute(
                    insert(BatchItem),
                    [
                        {
                            "id": iid,
                            "batch_id": batch_id,
                            "idx": i,
                            "custom_id": cid,
                            "request_json": {"body": body},
                        }
                        for i, (iid, (cid, body)) in enumerate(zip(item_ids, items, strict=True))
                    ],
                )
        except IntegrityError:
            if idempotency_key is None:
                raise
            existing = (
                await s.execute(
                    select(Batch).where(
                        Batch.tenant_id == auth.tenant_id, Batch.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one()
            return Submitted(existing, created=False)

        batch = await s.get(Batch, batch_id)

    try:
        await BatchQueue(state.redis).enqueue([str(i) for i in item_ids])
        async with state.sessionmaker() as s, s.begin():
            await s.execute(
                update(BatchItem).where(BatchItem.batch_id == batch_id).values(enqueued_at=_now())
            )
    except (RedisError, OSError) as e:
        log.error(
            "batch %s accepted but not enqueued (reconciler will pick it up): %r", batch_id, e
        )
    return Submitted(batch, created=True)


def _now():
    from sqlalchemy import func

    return func.now()
