import redis.asyncio as aioredis
from sqlalchemy import text

from app import metrics as m
from app.auth_cache import AuthCache
from tests.conftest import ADMIN

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


def lookups(result: str) -> float:
    return m.REGISTRY.get_sample_value("gateway_auth_cache_total", {"result": result}) or 0.0


async def _key(client):
    t = (await client.post("/admin/tenants", json={"name": "c"}, headers=ADMIN)).json()
    k = (await client.post(f"/admin/tenants/{t['id']}/keys", json={}, headers=ADMIN)).json()
    return k, {"Authorization": f"Bearer {k['key']}"}


async def test_second_request_is_served_from_cache(client):
    _, h = await _key(client)
    hit0, miss0 = lookups("hit"), lookups("miss")
    for _ in range(3):
        assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 200
    assert (lookups("miss") - miss0, lookups("hit") - hit0) == (1, 2)


async def test_cache_hit_does_not_touch_postgres(client, app):
    _, h = await _key(client)
    await client.post("/v1/chat/completions", json=BODY, headers=h)  # warms the cache
    # Break Postgres access for lookups: if the cache works, requests still succeed.
    real = app.state.sessionmaker
    calls = []

    def spy(*a, **kw):
        calls.append(1)
        return real(*a, **kw)

    app.state.sessionmaker = spy
    try:
        r = await client.post("/v1/chat/completions", json=BODY, headers=h)
    finally:
        app.state.sessionmaker = real
    assert r.status_code == 200
    # auth made no DB call; only the budget path may (counter exists in Redis, so none either)
    assert calls == []


async def test_revocation_is_immediate_despite_cache(client):
    k, h = await _key(client)
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 200
    assert (await client.delete(f"/admin/keys/{k['id']}", headers=ADMIN)).status_code == 204
    # no waiting for the TTL: the admin API invalidated the cache entry
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 401


async def test_invalid_keys_are_negatively_cached(client):
    h = {"Authorization": "Bearer sk-gw-doesnotexist"}
    miss0 = lookups("miss")
    for _ in range(5):
        assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 401
    assert lookups("miss") - miss0 == 1  # flood of bad keys hits Postgres once


async def test_cache_stores_no_plaintext_key(client, app):
    k, h = await _key(client)
    await client.post("/v1/chat/completions", json=BODY, headers=h)
    keys = [x.decode() async for x in app.state.redis.scan_iter("auth:*")]
    assert keys and all(k["key"] not in x for x in keys)  # entries are keyed by the hash


async def test_falls_back_to_postgres_when_redis_is_down(client, app):
    _, h = await _key(client)
    dead = aioredis.from_url("redis://localhost:1/0", socket_connect_timeout=0.1)
    app.state.auth_cache = AuthCache(dead)
    try:
        r = await client.post("/v1/chat/completions", json=BODY, headers=h)
    finally:
        await dead.aclose()
    # auth succeeded via Postgres even though its cache was unreachable
    # (the request may still be limited elsewhere, but it must not be a 401/500)
    assert r.status_code == 200


async def test_stale_cache_still_honors_db_revocation_after_ttl(client, app):
    _k, h = await _key(client)
    await client.post("/v1/chat/completions", json=BODY, headers=h)
    # Revoke directly in the DB (bypassing the admin API), then expire the cache entry:
    async with app.state.sessionmaker() as s:
        await s.execute(text("UPDATE api_keys SET revoked_at = now()"))
        await s.commit()
    async for key in app.state.redis.scan_iter("auth:*"):
        await app.state.redis.delete(key)
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 401
