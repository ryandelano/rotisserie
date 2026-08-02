import asyncio
import contextlib
import importlib
import logging
import math
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Literal, Union

from .env import load_keyconfigs_from_env
from .policies import AllocationPolicy, EvenSplitPolicy, coerce_policy
from .state import KeyState
from .types import AuthConfig, KeyConfig, RetryConfig
from .universal_auth import UniversalAuth

# Advanced: default max retry attempts for built-in clients
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_TOKEN_NAME = "key"
_POOL_OPTION_NAMES = frozenset(
    {
        "policy",
        "distribute",
        "log_level",
        "retry_config",
        "retry_attempts",
        "retry_for_methods",
        "auth_config",
        "auth_header",
        "auth_scheme",
        "auth_in",
        "auth_query_param",
    }
)

# ---------- Common helpers ----------


def _parse_retry_after(headers: dict[str, str], now: float) -> float:
    ra = None
    for k, v in headers.items():
        if k.lower() == "retry-after":
            ra = v
            break
    if ra is None:
        return 0.0
    try:
        delay = float(ra)
        return max(0.0, delay) if math.isfinite(delay) else 0.0
    except (TypeError, ValueError):
        # Try HTTP-date per RFC7231
        import email.utils as eut  # noqa: PLC0415

        try:
            ts = eut.parsedate_to_datetime(str(ra))
        except (TypeError, ValueError, IndexError, OverflowError):
            ts = None
        if ts is not None:
            # Round up to the next whole second to avoid truncation
            # making short delays appear too short
            return max(0.0, float(math.ceil(ts.timestamp() - now)))
        return 1.0


def _normalise_methods(methods) -> list[str]:
    if isinstance(methods, str):
        methods = [methods]
    try:
        normalised = [method.strip().upper() for method in methods]
        if any(not method for method in normalised):
            raise ValueError("retry_for_methods must contain non-empty method names")
        return sorted(set(normalised))
    except (TypeError, AttributeError) as exc:
        raise TypeError("retry_for_methods must be an iterable of method names") from exc


def _resolve_retry_config(kwargs: dict) -> RetryConfig:
    configured = kwargs.get("retry_config")
    if configured is None:
        configured = RetryConfig()
    elif not isinstance(configured, RetryConfig):
        raise TypeError("retry_config must be a RetryConfig instance")

    overrides = {}
    if "retry_attempts" in kwargs:
        overrides["retry_attempts"] = kwargs["retry_attempts"]
    if "retry_for_methods" in kwargs:
        overrides["retry_for_methods"] = _normalise_methods(kwargs["retry_for_methods"])
    elif configured.retry_for_methods != _normalise_methods(configured.retry_for_methods):
        overrides["retry_for_methods"] = _normalise_methods(configured.retry_for_methods)
    if "retry_attempts" in overrides:
        try:
            if (
                isinstance(overrides["retry_attempts"], bool)
                or not isinstance(overrides["retry_attempts"], int)
            ):
                raise ValueError
            overrides["retry_attempts"] = int(overrides["retry_attempts"])
            if overrides["retry_attempts"] < 1:
                raise ValueError
        except (TypeError, ValueError, OverflowError):
            raise ValueError("retry_attempts must be a positive integer") from None
    if not overrides:
        return configured
    return replace(configured, **overrides)


def _resolve_auth_config(pool_config: AuthConfig, kwargs: dict) -> AuthConfig:
    configured = kwargs.get("auth_config")
    if configured is not None:
        if not isinstance(configured, AuthConfig):
            raise TypeError("auth_config must be an AuthConfig instance")
        return configured
    return replace(
        pool_config,
        header=kwargs.get("auth_header", pool_config.header),
        scheme=kwargs.get("auth_scheme", pool_config.scheme),
        in_=kwargs.get("auth_in", pool_config.in_),
        query_param=kwargs.get("auth_query_param", pool_config.query_param),
    )


def _require_dependency(module_name: str, extra_name: str):
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(
            f"{module_name} integration requires the optional dependency; "
            f"install it with 'pip install rotisserie[{extra_name}]'."
        ) from exc


def _collect_pool_options(explicit: dict, remaining: dict) -> dict:
    options = {name: value for name, value in explicit.items() if value is not None}
    if remaining:
        names = ", ".join(sorted(remaining))
        raise TypeError(f"unexpected pool option(s): {names}")
    return options


def _validate_factory_options(options: dict) -> None:
    unknown = sorted(set(options) - _POOL_OPTION_NAMES)
    if unknown:
        names = ", ".join(unknown)
        raise TypeError(f"unexpected pool option(s): {names}")


def _keyconfigs_from_tokens(tokens, names=None, per_window=None) -> list[KeyConfig]:
    if isinstance(tokens, Mapping):
        if names is not None:
            raise ValueError("names cannot be provided when tokens is a mapping")
        pairs = list(tokens.items())
    else:
        if isinstance(tokens, str):
            tokens = [tokens]
        try:
            token_values = list(tokens)
        except TypeError as exc:
            raise TypeError("tokens must be a string, iterable, or mapping") from exc
        if names is None:
            name_values = [f"{DEFAULT_TOKEN_NAME}_{idx}" for idx in range(1, len(token_values) + 1)]
        else:
            if isinstance(names, str):
                names = [names]
            name_values = list(names)
            if len(name_values) != len(token_values):
                raise ValueError("names and tokens must contain the same number of items")
        pairs = list(zip(name_values, token_values))
    return [KeyConfig(name=name, token=token, per_window=per_window) for name, token in pairs]


@dataclass
class _EndpointInfo:
    name: str
    priority: int = 1
    reserve: int = 1

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("endpoint name must be a non-empty string")
        if not isinstance(self.reserve, int) or isinstance(self.reserve, bool) or self.reserve < 0:
            raise ValueError("endpoint reserve must be a non-negative integer")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise ValueError("endpoint priority must be an integer")


def _validate_endpoint_args(name: str, reserve: int, priority: int) -> None:
    _EndpointInfo(name=name, priority=priority, reserve=reserve)


# ---------- Base scheduler (shared logic; synchronization handled by subclasses) ----------


class _Scheduler:
    def __init__(
        self,
        keys: list[KeyConfig],
        policy: Union[AllocationPolicy, None],
        distribute: bool,
        retry_config: Union[RetryConfig, None],
    ):
        """Initialize a _Scheduler.

        Args:
            keys (list[KeyConfig]): list of KeyConfig objects
            policy (AllocationPolicy | None): policy object or string ("even" | "weighted")
            distribute (bool): distribute keys across priorities
            retry_config (RetryConfig | None): retry configuration

        Raises:
            ValueError: if policy is not a valid AllocationPolicy object or string
        """
        self._policy = policy or EvenSplitPolicy()
        self.distribute = distribute
        self.retry_config = retry_config
        if retry_config is not None:
            self._retry_429_base = retry_config.retry_after_base
            self._retry_429_growth = retry_config.retry_after_growth
            self._retry_429_cap = retry_config.retry_after_cap
            self._error_retry_base = retry_config.error_base
            self._error_retry_growth = retry_config.error_growth
            self._error_retry_cap = retry_config.error_cap
        else:
            self._retry_429_base = 0.5
            self._retry_429_growth = 2.0
            self._retry_429_cap = 30.0
            self._error_retry_base = 0.25
            self._error_retry_growth = 2.0
            self._error_retry_cap = 5.0
        self._keys: list[KeyState] = [
            KeyState(
                name=k.name,
                token=k.token,
                remaining=(k.per_window[0] if k.per_window else None),
            )
            for k in keys
        ]
        if len({k.name for k in keys}) != len(keys):
            raise ValueError("KeyConfig names must be unique")
        self._per_window: dict[str, tuple[int, int]] = {
            k.name: k.per_window for k in keys if k.per_window
        }
        self._endpoints: dict[str, _EndpointInfo] = {}
        self._endpoint_refcounts: dict[str, int] = {}
        self._closed = False
        self._logger = logging.getLogger("rotisserie")
        # Throttle sleep logs per-endpoint
        self._sleep_notice: dict[str, float] = {}

    def _now(self) -> float:
        return time.time()

    def _reset_window_if_needed(self, ks: KeyState, now: float):
        cfg = self._per_window.get(ks.name)
        if not cfg:
            return
        limit, seconds = cfg
        if now >= ks.window_ends_at:
            ks.remaining = limit
            ks.window_ends_at = now + seconds

    # --- priority-aware rebalance of *free* keys only ---
    def _group_endpoints_by_priority(self) -> dict[int, list[_EndpointInfo]]:
        by_pri: dict[int, list[_EndpointInfo]] = {}
        for e in self._endpoints.values():
            by_pri.setdefault(e.priority, []).append(e)
        return by_pri

    def _assign_reserves(
        self, free_keys: list[KeyState], by_pri: dict[int, list[_EndpointInfo]]
    ) -> None:
        for pri in sorted(by_pri.keys()):
            group = by_pri[pri]
            unmet = {
                e.name: max(0, e.reserve - sum(1 for k in self._keys if k.assigned_to == e.name))
                for e in group
            }
            total_unmet = sum(unmet.values())
            while free_keys and total_unmet > 0:
                for e in group:
                    if not free_keys or total_unmet == 0:
                        break
                    if unmet[e.name] > 0:
                        k = free_keys.pop()
                        k.assigned_to = e.name
                        unmet[e.name] -= 1
                        total_unmet -= 1

    def _assign_extras_round_robin(
        self, free_keys: list[KeyState], by_pri: dict[int, list[_EndpointInfo]]
    ) -> None:
        if not free_keys:
            return
        pri_levels = sorted(by_pri.keys())
        idx = 0
        while free_keys:
            pri = pri_levels[idx % len(pri_levels)]
            for e in by_pri[pri]:
                if not free_keys:
                    break
                k = free_keys.pop()
                k.assigned_to = e.name
            idx += 1

    def _assign_extras_funnel(
        self, free_keys: list[KeyState], by_pri: dict[int, list[_EndpointInfo]]
    ) -> None:
        for pri in sorted(by_pri.keys()):
            for e in by_pri[pri]:
                while free_keys:
                    k = free_keys.pop()
                    k.assigned_to = e.name

    def _rebalance(self):
        # Free keys only (not currently in use)
        now = self._now()
        free_keys = [
            k for k in self._keys if k.in_use_by is None and k.next_available_at(now) <= now
        ]
        if not free_keys or not self._endpoints:
            return

        # Unassign all free keys so we can recompute a fresh distribution
        for k in free_keys:
            k.assigned_to = None

        by_pri = self._group_endpoints_by_priority()
        # Step 1: ensure each endpoint meets its 'reserve' if possible
        self._assign_reserves(free_keys, by_pri)
        if not free_keys:
            return
        # Step 2: allocate extras (beyond reserve) using only truly unassigned keys
        extra_pool = [k for k in free_keys if k.assigned_to is None]
        if not extra_pool:
            return
        if self.distribute:
            self._assign_extras_round_robin(extra_pool, by_pri)
        else:
            self._assign_extras_funnel(extra_pool, by_pri)

    # --- select a key for an endpoint (non-blocking) ---
    def _pick_candidate(self, endpoint: str) -> Union[KeyState, None]:
        now = self._now()
        for k in self._keys:
            self._reset_window_if_needed(k, now)

        def ok(k):
            return (
                k.next_available_at(now) <= now
                and (k.remaining is None or k.remaining > 0)
                and k.in_use_by is None
            )

        assigned = [k for k in self._keys if k.assigned_to == endpoint and ok(k)]
        pool = assigned or [k for k in self._keys if ok(k)]
        if not pool:
            return None
        selected = self._policy.select_keys(pool, 1, endpoint)
        if not isinstance(selected, list) or not selected:
            raise ValueError("Allocation policy returned no key for a non-empty candidate pool")
        if len(selected) > 1 or not any(selected[0] is candidate for candidate in pool):
            raise ValueError("Allocation policy returned an invalid key selection")
        return selected[0]

    def _mark_result(
        self,
        key: KeyState,
        status_code: Union[int, None],
        headers: dict[str, str],
        error: Union[BaseException, None],
    ):
        if not any(candidate is key for candidate in self._keys):
            raise ValueError("key does not belong to this pool")
        if not isinstance(headers, dict):
            headers = dict(headers or {})
        now = self._now()
        if status_code == 429:  # noqa: PLR2004, http status code can be constant
            parse_now = self._now()
            retry_after = _parse_retry_after(headers, parse_now)
            if retry_after <= 0:
                key.failures += 1
                retry_after = min(
                    self._retry_429_cap,
                    self._retry_429_base * (self._retry_429_growth ** min(6, key.failures)),
                )
            key.cooldown_until = max(key.cooldown_until, parse_now + retry_after)
        elif error is not None:
            key.failures += 1
            key.cooldown_until = max(
                key.cooldown_until,
                now
                + min(
                    self._error_retry_cap,
                    self._error_retry_base * (self._error_retry_growth ** min(5, key.failures)),
                ),
            )
        else:
            key.failures = 0
            key.successes += 1
        key.in_use_by = None

    def _release_key(self, key: KeyState) -> None:
        if not any(candidate is key for candidate in self._keys):
            raise ValueError("key does not belong to this pool")
        key.in_use_by = None


# ---------- Sync pool (requests) ----------


class KeyPool(_Scheduler):
    def __init__(  # noqa: PLR0913
        self,
        keys: list[KeyConfig],
        policy: Union[object, None] = None,
        distribute: Union[bool, None] = None,
        log_level: Union[int, None] = None,
        *,
        retry_config: Union[RetryConfig, None] = None,
        retry_attempts: Union[int, None] = None,
        retry_for_methods: Union[Iterable[str], None] = None,
        auth_config: Union[AuthConfig, None] = None,
        auth_header: Union[str, None] = None,
        auth_scheme: Union[str, None] = None,
        auth_in: Union[Literal["header", "query"], None] = None,
        auth_query_param: Union[str, None] = None,
    ):
        """Initialize a synchronous key pool with optional auth and retry defaults."""
        config_options = _collect_pool_options(
            {
                "retry_config": retry_config,
                "retry_attempts": retry_attempts,
                "retry_for_methods": retry_for_methods,
                "auth_config": auth_config,
                "auth_header": auth_header,
                "auth_scheme": auth_scheme,
                "auth_in": auth_in,
                "auth_query_param": auth_query_param,
            },
            {},
        )
        # allow string/flag or callable/instance for policy
        policy_obj = coerce_policy(policy)
        # Auto behavior by policy if not explicitly set:
        # - Even → distribute across priorities (round-robin)
        # - Weighted → funnel to highest priority first
        if distribute is None:
            auto_dp = bool(isinstance(policy_obj, EvenSplitPolicy))
        else:
            auto_dp = distribute
        # Resolve retry/backoff settings once and keep the complete config intact.
        self._retry_config = _resolve_retry_config(config_options)
        super().__init__(keys, policy_obj, auto_dp, self._retry_config)
        self._lock = threading.Lock()
        # Resolve auth settings (prefer AuthConfig if provided)
        self._auth_config = _resolve_auth_config(AuthConfig(), config_options)
        # Single source of truth for adapters.
        self._max_attempts = self._retry_config.retry_attempts
        self._retry_methods = set(self._retry_config.retry_for_methods)
        if log_level is not None:
            with contextlib.suppress(Exception):
                self._logger.setLevel(log_level)

    def close(self):
        self._closed = True

    # public API
    def endpoint(self, name: str, reserve: int = 2, priority: int = 1):
        _validate_endpoint_args(name, reserve, priority)
        return _SyncEndpointLease(self, name, reserve, priority)

    def auth(  # noqa: PLR0913
        self,
        endpoint: str,
        reserve: int = 2,
        priority: int = 1,
        *,
        auth_config: Union[AuthConfig, None] = None,
        auth_header: Union[str, None] = None,
        auth_scheme: Union[str, None] = None,
        auth_in: Union[Literal["header", "query"], None] = None,
        auth_query_param: Union[str, None] = None,
        retry_config: Union[RetryConfig, None] = None,
        retry_attempts: Union[int, None] = None,
        retry_for_methods: Union[Iterable[str], None] = None,
    ):
        """Return a UniversalAuth usable with requests + httpx; aiohttp via helpers."""
        _validate_endpoint_args(endpoint, reserve, priority)
        options = _collect_pool_options(
            {
                "auth_config": auth_config,
                "auth_header": auth_header,
                "auth_scheme": auth_scheme,
                "auth_in": auth_in,
                "auth_query_param": auth_query_param,
                "retry_config": retry_config,
                "retry_attempts": retry_attempts,
                "retry_for_methods": retry_for_methods,
            },
            {},
        )
        return UniversalAuth(self, endpoint, reserve, priority, **options)

    def requests_client(self, endpoint: str, reserve: int = 2, priority: int = 1, session=None):
        from .adapters import RequestsClientContext  # noqa: PLC0415

        _validate_endpoint_args(endpoint, reserve, priority)
        return RequestsClientContext(self, endpoint, reserve, priority, session=session)

    @classmethod
    def from_tokens(
        cls,
        tokens: Union[str, Iterable[str], Mapping[str, str]],
        names: Union[str, Iterable[str], None] = None,
        per_window: Union[tuple[int, int], None] = None,
        **kwargs,
    ):
        """Build a pool from token values or a ``{name: token}`` mapping."""
        keys = _keyconfigs_from_tokens(tokens, names=names, per_window=per_window)
        _validate_factory_options(kwargs)
        return cls(keys, **kwargs)

    # ---------- direct integration (public wrappers) ----------
    def take_key(self, endpoint: str) -> KeyState:
        return self._take_key(endpoint)

    def mark_result(
        self,
        key: KeyState,
        status_code: Union[int, None],
        headers: dict[str, str],
        error: Union[BaseException, None],
    ) -> None:
        self._mark_result(key, status_code, headers, error)

    # ---------- convenience: build keys from env ----------
    @classmethod
    def from_env(
        cls,
        names=None,
        prefix: Union[str, None] = None,
        per_window: Union[tuple[int, int], None] = None,
        env_path: Union[str, None] = None,
        **kwargs,
    ):
        """from_env is a convenience method to create a KeyPool from environment variables.

        Args:
            names (_type_, optional): _description_. Defaults to None.
            prefix (Union[str, None], optional): _description_. Defaults to None.
            env_path (Union[str, None], optional): _description_. Defaults to None.

            kwargs keywords:
            to_lower_names: make names lowercase
            split_commas: split comma-separated values
            strip_prefix: strip prefix from names
        """
        # Only forward explicit loader flags; loader has sensible defaults
        loader_keys = {
            k: kwargs.pop(k)
            for k in list(kwargs.keys())
            if k in {"to_lower_names", "split_commas", "strip_prefix"}
        }
        pool_kwargs = kwargs
        keys = load_keyconfigs_from_env(
            names=names, prefix=prefix, per_window=per_window, env_path=env_path, **loader_keys
        )
        _validate_factory_options(pool_kwargs)
        return cls(keys, **pool_kwargs)

    def rebalance(self):
        with self._lock:
            self._rebalance()

    # internal
    def _take_key(self, endpoint: str) -> KeyState:
        if not self._keys:
            raise RuntimeError("rotisserie: cannot acquire a key from an empty pool")
        while True:
            if self._closed:
                raise RuntimeError("rotisserie: key pool is closed")
            with self._lock:
                key = self._pick_candidate(endpoint)
                if key is not None:
                    if key.remaining is not None:
                        key.remaining -= 1
                    key.in_use_by = endpoint
                    return key
                # compute earliest wake time
                now = self._now()
                wake = min(
                    now if k.in_use_by is not None else k.next_available_at(now)
                    for k in self._keys
                )
            delay = min(0.5, max(0.01, wake - self._now()))
            try:
                if self._sleep_notice.get(endpoint, 0.0) <= self._now():
                    self._logger.info(
                        f"endpoint={endpoint} all keys unavailable; sleeping ~{delay:.2f}s"
                    )
                    self._sleep_notice[endpoint] = self._now() + 5.0
            except Exception:
                pass
            time.sleep(delay)


# ---------- Async pool (httpx/aiohttp) ----------


class AsyncKeyPool(_Scheduler):
    def __init__(  # noqa: PLR0913
        self,
        keys: list[KeyConfig],
        policy: Union[object, None] = None,
        distribute: Union[bool, None] = None,
        log_level: Union[int, None] = None,
        *,
        retry_config: Union[RetryConfig, None] = None,
        retry_attempts: Union[int, None] = None,
        retry_for_methods: Union[Iterable[str], None] = None,
        auth_config: Union[AuthConfig, None] = None,
        auth_header: Union[str, None] = None,
        auth_scheme: Union[str, None] = None,
        auth_in: Union[Literal["header", "query"], None] = None,
        auth_query_param: Union[str, None] = None,
    ):
        """Initialize an asynchronous key pool with auth and retry defaults."""
        config_options = _collect_pool_options(
            {
                "retry_config": retry_config,
                "retry_attempts": retry_attempts,
                "retry_for_methods": retry_for_methods,
                "auth_config": auth_config,
                "auth_header": auth_header,
                "auth_scheme": auth_scheme,
                "auth_in": auth_in,
                "auth_query_param": auth_query_param,
            },
            {},
        )
        policy_obj = coerce_policy(policy)
        if distribute is None:
            auto_dp = bool(isinstance(policy_obj, EvenSplitPolicy))
        else:
            auto_dp = distribute
        self._retry_config = _resolve_retry_config(config_options)
        super().__init__(
            keys,
            policy_obj,
            auto_dp,
            self._retry_config,
        )
        self._lock = asyncio.Lock()
        self._auth_config = _resolve_auth_config(AuthConfig(), config_options)
        # Keep a single source of truth for adapters.
        self._max_attempts = self._retry_config.retry_attempts
        self._retry_methods = set(self._retry_config.retry_for_methods)
        if log_level is not None:
            with contextlib.suppress(Exception):
                self._logger.setLevel(log_level)

    async def close(self):
        self._closed = True

    def aendpoint(self, name: str, reserve: int = 2, priority: int = 1):
        _validate_endpoint_args(name, reserve, priority)
        return _AsyncEndpointLease(self, name, reserve, priority)

    def endpoint(self, name: str, reserve: int = 2, priority: int = 1):
        """Async equivalent of ``KeyPool.endpoint()``; use it with ``async with``."""
        return self.aendpoint(name, reserve=reserve, priority=priority)

    def auth(  # noqa: PLR0913
        self,
        endpoint: str,
        reserve: int = 2,
        priority: int = 1,
        *,
        auth_config: Union[AuthConfig, None] = None,
        auth_header: Union[str, None] = None,
        auth_scheme: Union[str, None] = None,
        auth_in: Union[Literal["header", "query"], None] = None,
        auth_query_param: Union[str, None] = None,
        retry_config: Union[RetryConfig, None] = None,
        retry_attempts: Union[int, None] = None,
        retry_for_methods: Union[Iterable[str], None] = None,
    ):
        _validate_endpoint_args(endpoint, reserve, priority)
        options = _collect_pool_options(
            {
                "auth_config": auth_config,
                "auth_header": auth_header,
                "auth_scheme": auth_scheme,
                "auth_in": auth_in,
                "auth_query_param": auth_query_param,
                "retry_config": retry_config,
                "retry_attempts": retry_attempts,
                "retry_for_methods": retry_for_methods,
            },
            {},
        )
        return UniversalAuth(self, endpoint, reserve, priority, **options)

    @classmethod
    def from_tokens(
        cls,
        tokens: Union[str, Iterable[str], Mapping[str, str]],
        names: Union[str, Iterable[str], None] = None,
        per_window: Union[tuple[int, int], None] = None,
        **kwargs,
    ):
        """Build an async pool from token values or a ``{name: token}`` mapping."""
        keys = _keyconfigs_from_tokens(tokens, names=names, per_window=per_window)
        _validate_factory_options(kwargs)
        return cls(keys, **kwargs)

    def httpx_client(self, endpoint: str, reserve: int = 2, priority: int = 1, client=None):
        from .adapters import HttpxClientContext  # noqa: PLC0415

        _validate_endpoint_args(endpoint, reserve, priority)
        return HttpxClientContext(self, endpoint, reserve, priority, client=client)

    def aiohttp_client(self, endpoint: str, reserve: int = 2, priority: int = 1, session=None):
        from .adapters import AiohttpClientContext  # noqa: PLC0415

        _validate_endpoint_args(endpoint, reserve, priority)
        return AiohttpClientContext(self, endpoint, reserve, priority, session=session)

    # ---------- direct integration (public wrappers) ----------
    async def take_key(self, endpoint: str) -> KeyState:
        return await self._take_key(endpoint)

    def mark_result(
        self,
        key: KeyState,
        status_code: Union[int, None],
        headers: dict[str, str],
        error: Union[BaseException, None],
    ) -> None:
        self._mark_result(key, status_code, headers, error)

    @classmethod
    def from_env(
        cls,
        names=None,
        prefix: Union[str, None] = None,
        per_window: Union[tuple[int, int], None] = None,
        env_path: Union[str, None] = None,
        **kwargs,
    ):
        from .env import load_keyconfigs_from_env  # noqa: PLC0415

        loader_keys = {
            k: kwargs.pop(k)
            for k in list(kwargs.keys())
            if k in {"to_lower_names", "split_commas", "strip_prefix"}
        }
        pool_kwargs = kwargs
        keys = load_keyconfigs_from_env(
            names=names,
            prefix=prefix,
            per_window=per_window,
            env_path=env_path,
            **loader_keys,
        )
        _validate_factory_options(pool_kwargs)
        return cls(keys, **pool_kwargs)

    async def rebalance(self):
        async with self._lock:
            self._rebalance()

    async def _take_key(self, endpoint: str) -> KeyState:
        if not self._keys:
            raise RuntimeError("rotisserie: cannot acquire a key from an empty pool")
        while True:
            if self._closed:
                raise RuntimeError("rotisserie: key pool is closed")
            async with self._lock:
                key = self._pick_candidate(endpoint)
                if key is not None:
                    if key.remaining is not None:
                        key.remaining -= 1
                    key.in_use_by = endpoint
                    return key
                now = self._now()
                wake = min(
                    now if k.in_use_by is not None else k.next_available_at(now)
                    for k in self._keys
                )
            delay = min(0.5, max(0.01, wake - self._now()))
            try:
                if self._sleep_notice.get(endpoint, 0.0) <= self._now():
                    self._logger.info(
                        f"endpoint={endpoint} all keys unavailable; sleeping ~{delay:.2f}s"
                    )
                    self._sleep_notice[endpoint] = self._now() + 5.0
            except Exception:
                pass
            await asyncio.sleep(delay)


# ---------- Endpoint leases (lifecycle + priority-aware reservation) ----------


class _SyncEndpointLease:
    def __init__(self, pool: KeyPool, name: str, reserve: int, priority: int):
        self.pool = pool
        self.name = name
        self.reserve = reserve
        self.priority = priority

    def __enter__(self):
        with self.pool._lock:
            current = self.pool._endpoints.get(self.name)
            requested = _EndpointInfo(self.name, self.priority, self.reserve)
            if current is not None and current != requested:
                raise ValueError(
                    f"endpoint {self.name!r} is already leased with different settings"
                )
            self.pool._endpoints[self.name] = requested
            self.pool._endpoint_refcounts[self.name] = (
                self.pool._endpoint_refcounts.get(self.name, 0) + 1
            )
            self.pool._rebalance()
        self._active = True
        return _SyncEndpointClient(self.pool, self.name)

    def __exit__(self, exc_type, exc, tb):
        if not getattr(self, "_active", False):
            return False
        with self.pool._lock:
            count = self.pool._endpoint_refcounts.get(self.name, 1) - 1
            if count <= 0:
                self.pool._endpoint_refcounts.pop(self.name, None)
                self.pool._endpoints.pop(self.name, None)
                # release assigned keys
                for k in self.pool._keys:
                    if k.assigned_to == self.name and k.in_use_by is None:
                        k.assigned_to = None
                self.pool._rebalance()
            else:
                self.pool._endpoint_refcounts[self.name] = count
        self._active = False
        return False


class _AsyncEndpointLease:
    def __init__(self, pool: AsyncKeyPool, name: str, reserve: int, priority: int):
        self.pool = pool
        self.name = name
        self.reserve = reserve
        self.priority = priority
        self.client = _AsyncEndpointClient(self.pool, self.name)

    async def __aenter__(self):
        async with self.pool._lock:
            current = self.pool._endpoints.get(self.name)
            requested = _EndpointInfo(self.name, self.priority, self.reserve)
            if current is not None and current != requested:
                raise ValueError(
                    f"endpoint {self.name!r} is already leased with different settings"
                )
            self.pool._endpoints[self.name] = requested
            self.pool._endpoint_refcounts[self.name] = (
                self.pool._endpoint_refcounts.get(self.name, 0) + 1
            )
            self.pool._rebalance()
        self._active = True
        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        if not getattr(self, "_active", False):
            return False
        async with self.pool._lock:
            count = self.pool._endpoint_refcounts.get(self.name, 1) - 1
            if count <= 0:
                self.pool._endpoint_refcounts.pop(self.name, None)
                self.pool._endpoints.pop(self.name, None)
                for k in self.pool._keys:
                    if k.assigned_to == self.name and k.in_use_by is None:
                        k.assigned_to = None
                self.pool._rebalance()
            else:
                self.pool._endpoint_refcounts[self.name] = count
        self._active = False
        return False


# ---------- Thin endpoint clients (perform requests, mark results) ----------


class _SyncEndpointClient:
    def __init__(self, pool: KeyPool, endpoint: str):
        self.pool = pool
        self.endpoint = endpoint

    def request(self, method: str, url: str, **kwargs):  # noqa: PLR0912, PLR0915
        requests = _require_dependency("requests", "requests")

        sess = kwargs.pop("session", None)
        if sess is None:
            sess = requests.Session()
        method = method.upper()
        base_kwargs = dict(kwargs)
        supplied_headers = dict(base_kwargs.pop("headers", {}) or {})
        supplied_params = base_kwargs.pop("params", {}) or {}
        if not isinstance(supplied_params, Mapping):
            supplied_params = list(supplied_params)
        retry_methods = getattr(self.pool, "_retry_methods", {"GET", "HEAD", "OPTIONS"})
        attempts, last_err, last_resp = 0, None, None
        while attempts < getattr(self.pool, "_max_attempts", 8):
            key = self.pool._take_key(self.endpoint)
            headers = dict(supplied_headers)
            params = (
                dict(supplied_params)
                if isinstance(supplied_params, Mapping)
                else list(supplied_params)
            )
            ac = getattr(self.pool, "_auth_config", None)
            if ac and ac.in_ == "query":
                if isinstance(params, Mapping):
                    params[ac.query_param] = key.token
                else:
                    params[:] = [(name, value) for name, value in params if name != ac.query_param]
                    params.append((ac.query_param, key.token))
            else:
                h = ac.header if ac else getattr(self.pool, "_auth_header", "Authorization")
                scheme_str = ac.scheme if ac else getattr(self.pool, "_auth_scheme", "Bearer")
                headers[h] = f"{scheme_str} {key.token}".strip()
            try:
                with contextlib.suppress(Exception):
                    self.pool._logger.debug(
                        f"""req start method={method} endpoint={self.endpoint} key={key.name} 
                        url={url}"""
                    )
                resp = sess.request(method, url, headers=headers, params=params, **base_kwargs)
                self.pool._mark_result(key, resp.status_code, dict(resp.headers), None)
                last_resp = resp
                with contextlib.suppress(Exception):
                    self.pool._logger.debug(
                        f"""req done method={method} endpoint={self.endpoint} key={key.name}
                        status={resp.status_code}"""
                    )
                if (
                    method not in retry_methods
                    or resp.status_code != 429  # noqa: PLR2004, http status code can be constant
                ):
                    return resp
                attempts += 1
                if attempts >= getattr(self.pool, "_max_attempts", 8):
                    return resp
                with contextlib.suppress(Exception):
                    resp.close()
                if method in retry_methods:
                    with contextlib.suppress(Exception):
                        self.pool._logger.info(
                            f"429 on endpoint={self.endpoint} key={key.name}; rotating"
                        )
                    continue
                return resp
            except requests.RequestException as e:
                with contextlib.suppress(Exception):
                    self.pool._logger.warning(
                        f"request error on endpoint={self.endpoint} key={key.name}: {e}"
                    )
                self.pool._mark_result(key, None, {}, e)
                attempts += 1
                last_err = e
                if method not in retry_methods:
                    raise
                continue
            except Exception as exc:
                if key.in_use_by is not None:
                    self.pool._mark_result(key, None, {}, exc)
                raise
        if last_err:
            raise last_err
        if last_resp is not None:
            return last_resp
        raise RuntimeError("rotisserie: failed after retries")

    # sugar
    def get(self, url: str, **kw):
        return self.request("GET", url, **kw)

    def post(self, url: str, **kw):
        return self.request("POST", url, **kw)

    def put(self, url: str, **kw):
        return self.request("PUT", url, **kw)

    def delete(self, url: str, **kw):
        return self.request("DELETE", url, **kw)


class _AsyncEndpointClient:
    def __init__(self, pool: AsyncKeyPool, endpoint: str):
        self.pool = pool
        self.endpoint = endpoint

    async def request(self, method: str, url: str, **kwargs):
        # Default: httpx if no explicit client/session is passed, handled by adapters
        raise NotImplementedError(
            "Use httpx_client(...) or aiohttp_client(...) contexts to perform requests."
        )
