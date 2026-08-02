import math
from dataclasses import dataclass, field
from typing import Literal, Union

PER_WINDOW_PARTS = 2


@dataclass
class KeyConfig:
    name: str
    token: str
    # Optional: estimated local per-key window (requests, seconds). If None, react to 429 only.
    per_window: Union[tuple[int, int], None] = None

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("KeyConfig.name must be a non-empty string")
        if not isinstance(self.token, str) or not self.token:
            raise ValueError("KeyConfig.token must be a non-empty string")
        if self.per_window is not None and (
            not isinstance(self.per_window, tuple)
            or len(self.per_window) != PER_WINDOW_PARTS
            or not all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in self.per_window
            )
            or self.per_window[0] < 1
            or self.per_window[1] <= 0
        ):
            raise ValueError("per_window must be a tuple of (positive limit, positive seconds)")


@dataclass(frozen=True)
class AuthConfig:
    header: str = "Authorization"
    scheme: str = "Bearer"
    in_: Literal["header", "query"] = "header"
    query_param: str = "api_key"

    def __post_init__(self):
        if self.in_ not in {"header", "query"}:
            raise ValueError("AuthConfig.in_ must be 'header' or 'query'")
        if not self.header or not self.query_param:
            raise ValueError("AuthConfig header and query_param must be non-empty")


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

    def __post_init__(self):
        for name in (
            "retry_after_base",
            "retry_after_growth",
            "retry_after_cap",
            "error_base",
            "error_growth",
            "error_cap",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a finite non-negative number")
        if self.retry_after_growth < 1 or self.error_growth < 1:
            raise ValueError("retry growth values must be at least 1")
        if (
            not isinstance(self.retry_attempts, int)
            or isinstance(self.retry_attempts, bool)
            or self.retry_attempts < 1
        ):
            raise ValueError("retry_attempts must be a positive integer")
        if isinstance(self.retry_for_methods, str):
            raise ValueError("retry_for_methods must be an iterable of method names")
        if any(
            not isinstance(method, str) or not method.strip()
            for method in self.retry_for_methods
        ):
            raise ValueError("retry_for_methods must contain non-empty strings")
        object.__setattr__(
            self,
            "retry_for_methods",
            [str(method).upper() for method in self.retry_for_methods],
        )
