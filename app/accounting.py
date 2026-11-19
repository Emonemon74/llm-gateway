from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from app import metrics as m
from app.api.deps import AuthContext
from app.routing.router import Target


@dataclass
class Accounting:
    """Everything that must happen when a request ends: reconcile limits, record usage."""

    state: Any
    auth: AuthContext
    request_id: str
    model: str
    estimated: int
    t0: float
    bucket_key: str | None = None  # rate-limit bucket; default = the API key's own
    bucket_tpm: int | None = None

    def _bucket(self) -> tuple[str, int]:
        return (
            self.bucket_key or str(self.auth.api_key_id),
            self.bucket_tpm or self.auth.tpm_limit,
        )

    async def release(self, status: str = "error") -> None:
        """Request produced nothing: give back the TPM tokens and the budget reservation."""
        key_id, tpm = self._bucket()
        await self.state.limiter.reconcile(key_id, tpm, self.estimated, 0)
        await self.state.budget.adjust(self.auth.tenant_id, -self.estimated)
        self._record(None, 0, 0, 0, False, 0.0, status)

    async def settle(self, target: Target, usage: dict | None, status: str = "ok") -> None:
        key_id, tpm = self._bucket()
        if usage and usage.get("total_tokens") is not None:
            p, c = int(usage.get("prompt_tokens", 0)), int(usage.get("completion_tokens", 0))
            total, estimated_flag = int(usage["total_tokens"]), False
        else:  # provider didn't report usage: keep the up-front estimate
            p = c = 0
            total, estimated_flag = self.estimated, True
        await self.state.limiter.reconcile(key_id, tpm, self.estimated, total)
        await self.state.budget.adjust(self.auth.tenant_id, total - self.estimated)
        cost = target.cost(p, c)
        if estimated_flag:
            m.TOKENS_ESTIMATED.inc()
        else:
            tenant = str(self.auth.tenant_id)
            m.TOKENS.labels(tenant, "prompt").inc(p)
            m.TOKENS.labels(tenant, "completion").inc(c)
            m.COST_USD.labels(tenant).inc(cost)
        self._record(target, p, c, total, estimated_flag, cost, status)

    def _record(self, target, p, c, total, estimated_flag, cost, status) -> None:
        self.state.usage.record(
            {
                "tenant_id": self.auth.tenant_id,
                "api_key_id": self.auth.api_key_id,
                "request_id": self.request_id,
                "provider": target.provider if target else None,
                "model": self.model,
                "prompt_tokens": p,
                "completion_tokens": c,
                "total_tokens": total,
                "tokens_estimated": estimated_flag,
                "cost_usd": cost,
                "latency_ms": int((time.monotonic() - self.t0) * 1000),
                "status": status,
            }
        )
