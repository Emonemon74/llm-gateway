"""The official OpenAI SDK must work against the gateway unmodified."""

from openai import AsyncOpenAI


async def test_sdk_chat(gateway, api_key):
    client = AsyncOpenAI(base_url="http://gw/v1", api_key=api_key, http_client=gateway)
    r = await client.chat.completions.create(
        model="mock/test", messages=[{"role": "user", "content": "hi"}]
    )
    assert r.choices[0].message.content
    assert r.usage and r.usage.total_tokens > 0


async def test_sdk_stream(gateway, api_key):
    client = AsyncOpenAI(base_url="http://gw/v1", api_key=api_key, http_client=gateway)
    stream = await client.chat.completions.create(
        model="mock/test", messages=[{"role": "user", "content": "hi"}], stream=True
    )
    parts = [c.choices[0].delta.content or "" async for c in stream if c.choices]
    assert "token0" in "".join(parts)
