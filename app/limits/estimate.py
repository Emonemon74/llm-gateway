from typing import Any

DEFAULT_MAX_TOKENS = 256


def estimate_tokens(body: dict[str, Any]) -> int:
    """Cheap pre-flight estimate: ~4 chars/token for the prompt + requested completion size.

    Reconciled against provider-reported usage after the response (see RateLimiter.reconcile).
    """
    chars = 0
    for m in body.get("messages", []):
        c = m.get("content", "")
        chars += len(c) if isinstance(c, str) else len(str(c))
    completion = body.get("max_completion_tokens") or body.get("max_tokens") or DEFAULT_MAX_TOKENS
    return chars // 4 + 1 + int(completion)
