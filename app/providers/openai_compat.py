from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.providers.base import Provider, ProviderError


def _is_retryable(status: int) -> bool:
    return status == 429 or status >= 500


class OpenAICompatProvider(Provider):
    """One adapter for every upstream that speaks OpenAI's chat-completions API
    (Groq, Cerebras, Gemini's compat endpoint, and our mock provider)."""

    def __init__(
        self,
        name: str,
        base_url: str,
        api_key: str,
        upstream_model: str | None = None,
        timeout_s: float = 60.0,
        client: httpx.AsyncClient | None = None,
    ):
        self.name = name
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._upstream_model = upstream_model
        self._client = client or httpx.AsyncClient(timeout=timeout_s)

    def _prepare(self, body: dict[str, Any]) -> dict[str, Any]:
        out = dict(body)
        if self._upstream_model:
            out["model"] = self._upstream_model
        return out

    async def chat(self, body: dict[str, Any]) -> dict[str, Any]:
        try:
            resp = await self._client.post(
                f"{self._base_url}/chat/completions",
                json=self._prepare(body) | {"stream": False},
                headers=self._headers,
            )
        except httpx.HTTPError as e:
            raise ProviderError(self.name, None, f"transport error: {e!r}", True) from e
        if resp.status_code >= 400:
            raise ProviderError(
                self.name, resp.status_code, resp.text[:300], _is_retryable(resp.status_code)
            )
        return resp.json()

    async def chat_stream(self, body: dict[str, Any]) -> AsyncIterator[bytes]:
        req = self._client.build_request(
            "POST",
            f"{self._base_url}/chat/completions",
            json=self._prepare(body) | {"stream": True},
            headers=self._headers,
        )
        try:
            resp = await self._client.send(req, stream=True)
        except httpx.HTTPError as e:
            raise ProviderError(self.name, None, f"transport error: {e!r}", True) from e
        try:
            if resp.status_code >= 400:
                text = (await resp.aread()).decode(errors="replace")[:300]
                raise ProviderError(
                    self.name, resp.status_code, text, _is_retryable(resp.status_code)
                )
            async for line in resp.aiter_lines():
                yield (line + "\n").encode()
        finally:
            await resp.aclose()
