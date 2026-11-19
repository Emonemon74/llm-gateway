from __future__ import annotations

from app.core.config import settings
from app.providers.base import Provider
from app.providers.openai_compat import OpenAICompatProvider


def build_providers() -> dict[str, Provider]:
    """Providers keyed by name. Real ones are registered only when a key is set."""
    t = settings.upstream_timeout_s
    providers: dict[str, Provider] = {
        "mock": OpenAICompatProvider("mock", settings.mock_base_url, "", timeout_s=t),
    }
    if settings.groq_api_key:
        providers["groq"] = OpenAICompatProvider(
            "groq", settings.groq_base_url, settings.groq_api_key, timeout_s=t
        )
    if settings.cerebras_api_key:
        providers["cerebras"] = OpenAICompatProvider(
            "cerebras", settings.cerebras_base_url, settings.cerebras_api_key, timeout_s=t
        )
    if settings.gemini_api_key:
        providers["gemini"] = OpenAICompatProvider(
            "gemini", settings.gemini_base_url, settings.gemini_api_key, timeout_s=t
        )
    return providers
