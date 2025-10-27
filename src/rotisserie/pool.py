import asyncio
import contextlib
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Union

from .env import load_keyconfigs_from_env
from .policies import AllocationPolicy, EvenSplitPolicy, coerce_policy
from .state import KeyState
from .types import AuthConfig, KeyConfig, RetryConfig
from .universal_auth import UniversalAuth

# Advanced: default max retry attempts for built-in clients
DEFAULT_MAX_ATTEMPTS = 8

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
        return float(ra)
    except ValueError:
        # Try HTTP-date per RFC7231
        import email.utils as eut  # noqa: PLC0415

        ts = eut.parsedate_to_datetime(ra)
        if ts is not None:
            # Round up to the next whole second to avoid truncation
            # making short delays appear too short
            return max(0.0, float(math.ceil(ts.timestamp() - now)))
        return 1.0


@dataclass
class _EndpointInfo:
    name: str
    priority: int = 1
    reserve: int = 1


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
        self._per_window: dict[str, tuple[int, int]] = {
            k.name: k.per_window for k in keys if k.per_window
        }
        self._endpoints: dict[str, _EndpointInfo] = {}
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
        return self._policy.select_keys(pool, 1, endpoint)[0]

    def _mark_result(
        self,
        key: KeyState,
        status_code: Union[int, None],
        headers: dict[str, str],
        error: Union[BaseException, None],
    ):
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


# ---------- Sync pool (requests) ----------


class KeyPool(_Scheduler):
    def __init__(
        self,
        keys: list[KeyConfig],
        policy: Union[object, None] = None,
        distribute: Union[bool, None] = None,
        log_level: Union[int, None] = None,
        **kwargs,
    ):
        """Initialize a KeyPool.

        Args:
            keys (list[KeyConfig]): list of KeyConfig objects
            policy (Union[object, None], optional): policy object or string ("even" | "weighted")
            distribute (Union[bool, None], optional): distribute keys across priorities
            log_level (Union[int, None], optional): log level
            kwargs:
            - retry_config: RetryConfig object
            - retry_attempts: int
            - retry_for_methods: list[str]
            - auth_config: AuthConfig object
            - auth_header: str
            - auth_scheme: str
            - auth_in: str
            - auth_query_param: str
        """
        # allow string/flag or callable/instance for policy
        policy_obj = coerce_policy(policy)
        # Auto behavior by policy if not explicitly set:
        # - Even → distribute across priorities (round-robin)
        # - Weighted → funnel to highest priority first
        if distribute is None:
            auto_dp = bool(isinstance(policy_obj, EvenSplitPolicy))
        else:
            auto_dp = distribute
        # Resolve retry/backoff settings (prefer RetryConfig if provided)
        rconf = kwargs.get("retry_config")
        if rconf is not None:
            self._retry_config = rconf
        else:
            self._retry_config = RetryConfig(
                retry_attempts=kwargs.get("retry_attempts", DEFAULT_MAX_ATTEMPTS),
                retry_for_methods=kwargs.get("retry_for_methods", ["GET", "HEAD", "OPTIONS"]),
            )
        super().__init__(keys, policy_obj, auto_dp, self._retry_config)
        self._lock = threading.Lock()
        # Resolve auth settings (prefer AuthConfig if provided)
        if kwargs.get("auth_config") is not None:
            self._auth_config = kwargs.get("auth_config")
        else:
            self._auth_config = AuthConfig(
                header=kwargs.get("auth_header", "Authorization"),
                scheme=kwargs.get("auth_scheme", "Bearer"),
                in_=kwargs.get("auth_in", "header"),
                query_param=kwargs.get("auth_query_param", "api_key"),
            )
        # Single source of truth for adapters: _auth_config only
        # Advanced: per-pool maximum retry attempts used by adapters
        if kwargs.get("retry_config") is not None:
            try:
                self._max_attempts = max(1, int(kwargs.get("retry_attempts", DEFAULT_MAX_ATTEMPTS)))
            except Exception:
                self._max_attempts = DEFAULT_MAX_ATTEMPTS
            self._retry_methods = {
                m.upper() for m in (kwargs.get("retry_for_methods", ["GET", "HEAD", "OPTIONS"]))
            }
            self._retry_config = RetryConfig(
                retry_attempts=self._max_attempts,
                retry_for_methods=list(self._retry_methods),
            )
        else:
            try:
                self._max_attempts = max(1, int(kwargs.get("retry_attempts", DEFAULT_MAX_ATTEMPTS)))
            except Exception:
                self._max_attempts = DEFAULT_MAX_ATTEMPTS
            self._retry_methods = {"GET", "HEAD", "OPTIONS"}
            self._retry_config = RetryConfig(
                retry_attempts=self._max_attempts,
                retry_for_methods=list(self._retry_methods),
            )
        if log_level is not None:
            with contextlib.suppress(Exception):
                self._logger.setLevel(log_level)

    def close(self):
        self._closed = True

    # public API
    def endpoint(self, name: str, reserve: int = 2, priority: int = 1):
        return _SyncEndpointLease(self, name, reserve, priority)

    def auth(self, endpoint: str, reserve: int = 2, priority: int = 1, **kwargs):
        """Return a UniversalAuth usable with requests + httpx; aiohttp via helpers."""
        return UniversalAuth(self, endpoint, reserve, priority, **kwargs)

    def requests_client(self, endpoint: str, reserve: int = 2, priority: int = 1, session=None):
        from .adapters import RequestsClientContext  # noqa: PLC0415

        return RequestsClientContext(self, endpoint, reserve, priority, session=session)

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
        return cls(keys, **pool_kwargs)

    def rebalance(self):
        with self._lock:
            self._rebalance()

    # internal
    def _take_key(self, endpoint: str) -> KeyState:
        while True:
            with self._lock:
                key = self._pick_candidate(endpoint)
                if key is not None:
                    if key.remaining is not None:
                        key.remaining -= 1
                    key.in_use_by = endpoint
                    return key
                # compute earliest wake time
                now = self._now()
                wake = min(k.next_available_at(now) for k in self._keys)
            delay = max(0.01, wake - self._now())
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
    def __init__(
        self,
        keys: list[KeyConfig],
        policy: Union[object, None] = None,
        distribute: Union[bool, None] = None,
        log_level: Union[int, None] = None,
        **kwargs,
    ):
        """Initialize an AsyncKeyPool.

        Other keywords for kwargs:
        - auth_config: AuthConfig object
        - auth_header: str
        - auth_scheme: str
        - auth_in: str
        - auth_query_param: str
        - retry_config: RetryConfig object
        - retry_attempts: int
        - retry_for_methods: Iterable[str]
        - distribute: bool
        """
        policy_obj = coerce_policy(policy)
        if distribute is None:
            auto_dp = bool(isinstance(policy_obj, EvenSplitPolicy))
        else:
            auto_dp = distribute
        rconf = kwargs.get("retry_config")
        super().__init__(
            keys,
            policy_obj,
            auto_dp,
            rconf,
        )
        self._lock = asyncio.Lock()
        if kwargs.get("auth_config") is not None:
            self._auth_config = kwargs.get("auth_config")
        else:
            self._auth_config = AuthConfig(
                header=kwargs.get("auth_header", "Authorization"),
                scheme=kwargs.get("auth_scheme", "Bearer"),
                in_=kwargs.get("auth_in", "header"),
                query_param=kwargs.get("auth_query_param", "api_key"),
            )
        # keep a single source of truth for adapters
        if kwargs.get("retry_config") is not None:
            try:
                self._max_attempts = max(1, int(kwargs.get("retry_attempts", DEFAULT_MAX_ATTEMPTS)))
            except Exception:
                self._max_attempts = DEFAULT_MAX_ATTEMPTS
            self._retry_methods = {
                m.upper() for m in (kwargs.get("retry_for_methods", ["GET", "HEAD", "OPTIONS"]))
            }
            self._retry_config = RetryConfig(
                retry_attempts=self._max_attempts,
                retry_for_methods=list(self._retry_methods),
            )
        else:
            try:
                self._max_attempts = max(1, int(kwargs.get("retry_attempts", DEFAULT_MAX_ATTEMPTS)))
            except Exception:
                self._max_attempts = DEFAULT_MAX_ATTEMPTS
            self._retry_methods = {"GET", "HEAD", "OPTIONS"}
            self._retry_config = RetryConfig(
                retry_attempts=self._max_attempts,
                retry_for_methods=list(self._retry_methods),
            )
        if log_level is not None:
            with contextlib.suppress(Exception):
                self._logger.setLevel(log_level)

    async def close(self):
        self._closed = True

    def aendpoint(self, name: str, reserve: int = 2, priority: int = 1):
        return _AsyncEndpointLease(self, name, reserve, priority)

    def auth(self, endpoint: str, reserve: int = 2, priority: int = 1, **kwargs):
        return UniversalAuth(self, endpoint, reserve, priority, **kwargs)

    def httpx_client(self, endpoint: str, reserve: int = 2, priority: int = 1, client=None):
        from .adapters import HttpxClientContext  # noqa: PLC0415

        return HttpxClientContext(self, endpoint, reserve, priority, client=client)

    def aiohttp_client(self, endpoint: str, reserve: int = 2, priority: int = 1, session=None):
        from .adapters import AiohttpClientContext  # noqa: PLC0415

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
            env_path=env_path,
            **loader_keys,
        )
        return cls(keys, **pool_kwargs)

    async def rebalance(self):
        async with self._lock:
            self._rebalance()

    async def _take_key(self, endpoint: str) -> KeyState:
        while True:
            async with self._lock:
                key = self._pick_candidate(endpoint)
                if key is not None:
                    if key.remaining is not None:
                        key.remaining -= 1
                    key.in_use_by = endpoint
                    return key
                now = self._now()
                wake = min(k.next_available_at(now) for k in self._keys)
            delay = max(0.01, wake - self._now())
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
            self.pool._endpoints[self.name] = _EndpointInfo(self.name, self.priority, self.reserve)
            self.pool._rebalance()
        return _SyncEndpointClient(self.pool, self.name)

    def __exit__(self, exc_type, exc, tb):
        with self.pool._lock:
            self.pool._endpoints.pop(self.name, None)
            # release assigned keys
            for k in self.pool._keys:
                if k.assigned_to == self.name and k.in_use_by is None:
                    k.assigned_to = None
            self.pool._rebalance()
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
            self.pool._endpoints[self.name] = _EndpointInfo(self.name, self.priority, self.reserve)
            self.pool._rebalance()
        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        async with self.pool._lock:
            self.pool._endpoints.pop(self.name, None)
            for k in self.pool._keys:
                if k.assigned_to == self.name and k.in_use_by is None:
                    k.assigned_to = None
            self.pool._rebalance()
        return False


# ---------- Thin endpoint clients (perform requests, mark results) ----------


class _SyncEndpointClient:
    def __init__(self, pool: KeyPool, endpoint: str):
        self.pool = pool
        self.endpoint = endpoint

    def request(self, method: str, url: str, **kwargs):
        import requests  # noqa: PLC0415

        sess = kwargs.pop("session", None) or requests.Session()
        attempts, last_err = 0, None
        while attempts < getattr(self.pool, "_max_attempts", 8):
            key = self.pool._take_key(self.endpoint)
            headers = {**kwargs.pop("headers", {})}
            params = {**kwargs.pop("params", {})}
            ac = getattr(self.pool, "_auth_config", None)
            if ac and ac.in_ == "query":
                params[ac.query_param] = key.token
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
                resp = sess.request(method, url, headers=headers, params=params, **kwargs)
                self.pool._mark_result(key, resp.status_code, dict(resp.headers), None)
                with contextlib.suppress(Exception):
                    self.pool._logger.debug(
                        f"""req done method={method} endpoint={self.endpoint} key={key.name}
                        status={resp.status_code}"""
                    )
                if resp.status_code == 429:  # noqa: PLR2004, http status code can be constant
                    with contextlib.suppress(Exception):
                        self.pool._logger.info(
                            f"429 on endpoint={self.endpoint} key={key.name}; rotating"
                        )
                    attempts += 1
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
                continue
        if last_err:
            raise last_err
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
