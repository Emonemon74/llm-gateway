from tests.conftest import ADMIN

BODY = {"model": "mock/test", "messages": [{"role": "user", "content": "hi"}]}


async def test_chat_requires_key(client):
    r = await client.post("/v1/chat/completions", json=BODY)
    assert r.status_code == 401


async def test_chat_rejects_bad_key(client):
    r = await client.post(
        "/v1/chat/completions", json=BODY, headers={"Authorization": "Bearer sk-gw-nope"}
    )
    assert r.status_code == 401


async def test_valid_key_works(client, api_key):
    r = await client.post(
        "/v1/chat/completions", json=BODY, headers={"Authorization": f"Bearer {api_key}"}
    )
    assert r.status_code == 200


async def test_revoked_key_is_rejected(client):
    t = (await client.post("/admin/tenants", json={"name": "x"}, headers=ADMIN)).json()
    k = (await client.post(f"/admin/tenants/{t['id']}/keys", json={}, headers=ADMIN)).json()
    h = {"Authorization": f"Bearer {k['key']}"}
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 200
    assert (await client.delete(f"/admin/keys/{k['id']}", headers=ADMIN)).status_code == 204
    assert (await client.post("/v1/chat/completions", json=BODY, headers=h)).status_code == 401


async def test_plaintext_key_is_never_stored(client, app):
    from sqlalchemy import text

    t = (await client.post("/admin/tenants", json={"name": "y"}, headers=ADMIN)).json()
    k = (await client.post(f"/admin/tenants/{t['id']}/keys", json={}, headers=ADMIN)).json()
    async with app.state.sessionmaker() as s:
        rows = (await s.execute(text("SELECT key_prefix, key_hash FROM api_keys"))).all()
    assert all(k["key"] not in "".join(r) for r in rows)
    assert rows[0].key_hash != k["key"] and len(rows[0].key_hash) == 64
    # listing never exposes the secret either
    listed = (await client.get(f"/admin/tenants/{t['id']}/keys", headers=ADMIN)).json()
    assert "key" not in listed[0] and listed[0]["prefix"] == k["prefix"]


async def test_admin_requires_token(client):
    assert (await client.post("/admin/tenants", json={"name": "z"})).status_code == 401
    bad = {"Authorization": "Bearer wrong"}
    assert (await client.post("/admin/tenants", json={"name": "z"}, headers=bad)).status_code == 401


async def test_duplicate_tenant_is_409(client):
    await client.post("/admin/tenants", json={"name": "dup"}, headers=ADMIN)
    r = await client.post("/admin/tenants", json={"name": "dup"}, headers=ADMIN)
    assert r.status_code == 409


async def test_key_for_unknown_tenant_is_404(client):
    r = await client.post(
        "/admin/tenants/00000000-0000-0000-0000-000000000000/keys", json={}, headers=ADMIN
    )
    assert r.status_code == 404


async def test_out_of_range_key_limits_are_422_not_500(client):
    t = (await client.post("/admin/tenants", json={"name": "big"}, headers=ADMIN)).json()
    for bad in ({"tpm_limit": 10**10}, {"rpm_limit": 0}, {"rpm_limit": 2**31}):
        r = await client.post(f"/admin/tenants/{t['id']}/keys", json=bad, headers=ADMIN)
        assert r.status_code == 422
