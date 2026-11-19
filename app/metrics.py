"""Prometheus metrics. One dedicated registry so tests can read values and nothing else leaks in.

Cardinality rules (an unbounded label value is a memory leak):
  * `route` is the alias from the routing table, or the literal "direct" for provider/model
    addressing; never the raw client-supplied model string.
  * `tenant` appears ONLY on token and cost counters (per-tenant metering), never on latency
    histograms. Tenant count is operator-controlled, so this stays bounded.
Run one uvicorn process per container and scale by replicas (no multiprocess mode needed).
"""

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    PlatformCollector,
    ProcessCollector,
)

REGISTRY = CollectorRegistry()
ProcessCollector(registry=REGISTRY)
PlatformCollector(registry=REGISTRY)

_LAT = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
_OVERHEAD = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1)

REQUESTS = Counter(
    "gateway_requests_total",
    "Client requests by final outcome.",
    ["route", "provider", "outcome", "stream"],
    registry=REGISTRY,
)
REQUEST_SECONDS = Histogram(
    "gateway_request_duration_seconds",
    "End-to-end request time (streams: time to first byte).",
    ["route", "stream"],
    buckets=_LAT,
    registry=REGISTRY,
)
UPSTREAM_SECONDS = Histogram(
    "gateway_upstream_duration_seconds",
    "Time spent waiting on one upstream provider call.",
    ["provider"],
    buckets=_LAT,
    registry=REGISTRY,
)
OVERHEAD_SECONDS = Histogram(
    "gateway_overhead_seconds",
    "Gateway-added latency = request time minus time spent waiting on upstreams.",
    ["stream"],
    buckets=_OVERHEAD,
    registry=REGISTRY,
)
UPSTREAM_REQUESTS = Counter(
    "gateway_upstream_requests_total",
    "Upstream attempts. outcome: ok | retryable_error | client_error | skipped.",
    ["provider", "outcome"],
    registry=REGISTRY,
)
RATE_LIMITED = Counter(
    "gateway_rate_limited_total",
    "Requests rejected before reaching upstream. limit: rpm | tpm | too_large | budget.",
    ["limit"],
    registry=REGISTRY,
)
TOKENS = Counter(
    "gateway_tokens_total",
    "Provider-reported tokens. direction: prompt | completion.",
    ["tenant", "direction"],
    registry=REGISTRY,
)
COST_USD = Counter(
    "gateway_cost_usd_total",
    "Estimated spend from the routing table prices.",
    ["tenant"],
    registry=REGISTRY,
)
TOKENS_ESTIMATED = Counter(
    "gateway_requests_token_estimated_total",
    "Requests settled on the pre-call estimate because the provider reported no usage.",
    registry=REGISTRY,
)
AUTH_FAILURES = Counter(
    "gateway_auth_failures_total",
    "Rejected API keys. reason: missing | invalid.",
    ["reason"],
    registry=REGISTRY,
)
AUTH_CACHE = Counter(
    "gateway_auth_cache_total",
    "API-key lookups. result: hit | miss.",
    ["result"],
    registry=REGISTRY,
)
INFLIGHT = Gauge("gateway_inflight_requests", "Requests currently being served.", registry=REGISTRY)
CIRCUIT_STATE = Gauge(
    "gateway_circuit_state",
    "Circuit breaker state per provider: 0 closed, 1 half_open, 2 open, -1 unknown.",
    ["provider"],
    registry=REGISTRY,
)
USAGE_QUEUE = Gauge(
    "gateway_usage_queue_depth",
    "Usage events waiting to be written to Postgres.",
    registry=REGISTRY,
)
USAGE_DROPPED = Gauge(
    "gateway_usage_events_dropped",
    "Usage events dropped because the queue was full.",
    registry=REGISTRY,
)

BATCH_ITEMS = Counter(
    "gateway_batch_items_total",
    "Batch item processing outcomes. outcome: done | retry | deferred | failed | duplicate.",
    ["outcome"],
    registry=REGISTRY,
)
QUEUE_DEPTH = Gauge(
    "gateway_queue_depth",
    "Batch work items queued or in flight (stream length).",
    registry=REGISTRY,
)
DELAYED_DEPTH = Gauge(
    "gateway_delayed_depth", "Batch items waiting on a retry/backpressure delay.", registry=REGISTRY
)
DLQ_DEPTH = Gauge("gateway_dlq_depth", "Dead-lettered batch items.", registry=REGISTRY)

CIRCUIT_VALUES = {"closed": 0, "half_open": 1, "open": 2}
