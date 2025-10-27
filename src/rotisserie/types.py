from dataclasses import dataclass, field
from typing import Literal


@dataclass
class KeyConfig:
    name: str
    token: str
    # Optional: estimated local per-key window (requests, seconds). If None, react to 429 only.
    per_window: tuple[int, int] | None = None


@dataclass(frozen=True)
class AuthConfig:
    header: str = "Authorization"
    scheme: str = "Bearer"
    in_: Literal["header", "query"] = "header"
    query_param: str = "api_key"


@dataclass(frozen=True)
class RetryConfig:
    # Retry-After handling (429)
    retry_after_base: float = 0.5
    retry_after_growth: float = 2.0
    retry_after_cap: float = 30.0

    # Transport errors/backoff
    error_base: float = 0.25
    error_growth: float = 2.0
    error_cap: float = 5.0

    # retry attempts and methods
    retry_attempts: int = 8
    retry_for_methods: list[str] = field(default_factory=lambda: ["GET", "HEAD", "OPTIONS"])