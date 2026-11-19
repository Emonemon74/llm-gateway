import random


def backoff_s(attempt: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """Full-jitter exponential backoff: uniform(0, min(cap, base * 2**(attempt-1))).

    Jitter matters: without it, every item that failed together retries together and hammers
    the provider that just recovered (thundering herd).
    """
    ceiling = min(cap, base * (2 ** max(0, attempt - 1)))
    return (rng or random).uniform(0, ceiling)
