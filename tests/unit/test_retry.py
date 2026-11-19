import random

from app.batch.retry import backoff_s


def test_backoff_is_bounded_by_exponential_ceiling():
    rng = random.Random(1)
    for attempt, ceiling in [(1, 1), (2, 2), (3, 4), (4, 8)]:
        draws = [backoff_s(attempt, base=1, cap=60, rng=rng) for _ in range(500)]
        assert 0 <= min(draws) and max(draws) <= ceiling
        assert max(draws) > ceiling * 0.8  # it really uses the range


def test_backoff_respects_cap():
    rng = random.Random(2)
    assert max(backoff_s(20, base=1, cap=30, rng=rng) for _ in range(500)) <= 30


def test_backoff_is_jittered_not_constant():
    rng = random.Random(3)
    assert len({round(backoff_s(5, 1, 60, rng), 3) for _ in range(50)}) > 40
