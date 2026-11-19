from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any


class ProviderError(Exception):
    """Upstream failure. `retryable` drives failover and circuit-breaker logic."""

    def __init__(self, provider: str, status: int | None, message: str, retryable: bool):
        super().__init__(f"{provider}: {status} {message}")
        self.provider = provider
        self.status = status
        self.message = message
        self.retryable = retryable


class Provider(ABC):
    name: str

    @abstractmethod
    async def chat(self, body: dict[str, Any]) -> dict[str, Any]:
        """Non-streaming completion. Returns the OpenAI-format response dict."""

    @abstractmethod
    def chat_stream(self, body: dict[str, Any]) -> AsyncIterator[bytes]:
        """Streaming completion. Yields raw SSE lines (bytes, newline-terminated).

        Must raise ProviderError *before yielding anything* if the upstream
        rejects the request, so callers can still fail over (see PLAN.md §4).
        """
