from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Upstream providers (all speak the OpenAI chat-completions dialect).
    mock_base_url: str = "http://localhost:9000/v1"
    groq_api_key: str = ""
    groq_base_url: str = "https://api.groq.com/openai/v1"
    cerebras_api_key: str = ""
    cerebras_base_url: str = "https://api.cerebras.ai/v1"
    gemini_api_key: str = ""
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta/openai"

    # Any other OpenAI-compatible upstream, as JSON:
    #   EXTRA_PROVIDERS='{"together": {"base_url": "https://...", "api_key": "..."}}'
    extra_providers: dict[str, dict[str, str]] = {}

    upstream_timeout_s: float = 60.0

    database_url: str = "postgresql+asyncpg://gateway:gateway@localhost:5432/gateway"
    breaker_failure_threshold: int = 5
    breaker_cooldown_s: float = 30.0
    breaker_probe_timeout_s: float = 10.0

    # Batch workers (see docs/PLAN.md section 5)
    batch_concurrency: int = 16
    batch_visibility_timeout_s: float = 120.0  # > upstream timeout, else healthy work is reclaimed
    batch_backoff_base_s: float = 1.0
    batch_backoff_cap_s: float = 60.0
    batch_reconcile_after_s: float = 900.0
    batch_reconcile_every_s: float = 10.0
    batch_rate_share: float = 0.5  # fraction of a key's RPM/TPM that batch work may use
    worker_metrics_port: int = 0  # 0 = off

    redis_url: str = "redis://localhost:6379/0"
    # Bearer token for /admin/*. Empty = admin API disabled.
    admin_token: str = ""


settings = Settings()
