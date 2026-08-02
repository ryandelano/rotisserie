import asyncio
import contextlib
import inspect
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any, Literal, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .types import AuthConfig, RetryConfig


class _HttpxFallback:
    pass


_HttpxAuth: Any = _HttpxFallback
try:  # httpx is an optional dependency
    import httpx
except ImportError:
    pass
else:
    _HttpxAuth = httpx.Auth


class UniversalAuth(_HttpxAuth):
    """One object that plugs into requests + httpx directly, with helpers for aiohttp.

    - requests: uses __call__(request) protocol + response hook to auto-rotate/retry on 429.
    - httpx: implements auth_flow / async_auth_flow to reissue on 429 inside the auth layer.
    - aiohttp: provide trace_config() (for accounting) and wrap_session(session) to implement
        transparent 429 rotation using the SAME session/connector (so pool reuse is preserved).

    """

    @staticmethod
    def _normalise_methods(methods) -> set[str]:
        if isinstance(methods, str):
            methods = [methods]
        try:
            normalised = {method.strip().upper() for method in methods}
        except (TypeError, AttributeError) as exc:
            raise TypeError("retry_for_methods must be an iterable of method names") from exc
        if any(not method for method in normalised):
            raise ValueError("retry_for_methods must contain non-empty method names")
        return normalised

    def __init__(  # noqa: PLR0913
        self,
        pool,
        endpoint: str,
        reserve: int,
        priority: int,
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
        self.pool = pool
        self.endpoint = endpoint
        self.reserve = reserve
        self.priority = priority
        # Prefer config objects, then inherit from pool, finally fall back to args
        pool_auth: Union[AuthConfig, None] = getattr(pool, "_auth_config", None)
        use_auth: Union[AuthConfig, None] = auth_config
        if use_auth is not None and not isinstance(use_auth, AuthConfig):
            raise TypeError("auth_config must be an AuthConfig instance")
        base_auth = use_auth or pool_auth or AuthConfig()
        if use_auth is None:
            base_auth = replace(
                base_auth,
                header=auth_header if auth_header is not None else base_auth.header,
                scheme=auth_scheme if auth_scheme is not None else base_auth.scheme,
                in_=auth_in if auth_in is not None else base_auth.in_,
                query_param=(
                    auth_query_param
                    if auth_query_param is not None
                    else base_auth.query_param
                ),
            )
        self._auth_config = base_auth
        self.auth_header = base_auth.header
        self.auth_scheme = base_auth.scheme
        self.auth_in = base_auth.in_
        self.auth_query_param = base_auth.query_param
        pool_retry: Union[RetryConfig, None] = getattr(pool, "_retry_config", None)
        if retry_config is not None and not isinstance(retry_config, RetryConfig):
            raise TypeError("retry_config must be a RetryConfig instance")
        use_retry: Union[RetryConfig, None] = (
            retry_config if retry_config is not None else pool_retry
        )
        if use_retry is not None:
            self.retry_attempts = use_retry.retry_attempts
            self.retry_for_methods = self._normalise_methods(use_retry.retry_for_methods)
        else:
            try:
                self.retry_attempts = int(retry_attempts) if retry_attempts is not None else 8
                if self.retry_attempts < 1:
                    raise ValueError("retry_attempts must be a positive integer")
            except (TypeError, ValueError, OverflowError):
                raise ValueError("retry_attempts must be a positive integer") from None
            # By default we retry idempotent methods only; caller can extend to POST, etc.
            self.retry_for_methods = self._normalise_methods(
                retry_for_methods
                if retry_for_methods is not None
                else ["GET", "HEAD", "OPTIONS"]
            )
        self._lease = None
        self._async = hasattr(pool, "_lock") and asyncio.iscoroutinefunction(
            getattr(pool, "_take_key", None)
        )

    def _inject(self, request, key):
        """Inject the configured credential into a requests/httpx-style request."""
        if self.auth_in == "query":
            if hasattr(request.url, "copy_merge_params"):
                request.url = request.url.copy_merge_params({self.auth_query_param: key.token})
                return
            url = str(request.url)
            parts = urlsplit(url)
            query = parse_qsl(parts.query, keep_blank_values=True)
            query = [(name, value) for name, value in query if name != self.auth_query_param]
            query.append((self.auth_query_param, key.token))
            new_url = urlunsplit((*parts[:3], urlencode(query), parts.fragment))
            request.url = new_url
        else:
            request.headers[self.auth_header] = f"{self.auth_scheme} {key.token}".strip()

    @staticmethod
    async def _release_response(response):
        for name in ("aclose", "close", "release"):
            method = getattr(response, name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                await result
            return

    @staticmethod
    def _add_query_param(params, name: str, token: str):
        if isinstance(params, Mapping):
            updated = dict(params)
            updated[name] = token
            return updated
        params = [(key, value) for key, value in params if key != name]
        params.append((name, token))
        return params

    # -------- context mgmt ties lifetime to endpoint presence (priority + reserves) --------
    def __enter__(self):
        if self._async:
            raise RuntimeError("Use 'async with' for an AsyncKeyPool.")
        self._lease = self.pool.endpoint(self.endpoint, self.reserve, self.priority)
        self._client = self._lease.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._lease is None:
            return False
        return self._lease.__exit__(exc_type, exc, tb)

    async def __aenter__(self):
        if not self._async:
            # sync pool can still be used from async code, but leasing is sync:
            # For simplicity, disallow to avoid confusion.
            raise RuntimeError("Use 'with' (not 'async with') for a sync KeyPool.")
        self._lease = self.pool.aendpoint(self.endpoint, self.reserve, self.priority)
        self._client = await self._lease.__aenter__()
        return self

    # __await__ was previously implemented incorrectly and is not part of the public API.
    # Remove coroutine-style usage to avoid confusion.

    async def __aexit__(self, exc_type, exc, tb):
        if self._lease is None:
            return False
        return await self._lease.__aexit__(exc_type, exc, tb)

    # ------------------------ requests auth protocol ------------------------
    def __call__(self, r):
        """Inject header and attach a response hook that may transparently resend on 429.

        This runs on the same requests.Session and reuses its connection pool.
        """
        if self._async:
            raise RuntimeError("Use an async HTTP client with an AsyncKeyPool.")
        try:
            key = self.pool._take_key(self.endpoint)
            self._inject(r, key)
        except Exception:
            raise

        def _hook(resp, *args, **kwargs):
            # Mark the result for the original attempt
            self.pool._mark_result(key, resp.status_code, dict(resp.headers or {}), None)

            request = getattr(resp, "request", None)
            method = getattr(request, "method", "GET").upper()
            if method not in self.retry_for_methods:
                return resp

            if resp.status_code != 429:  # noqa: PLR2004, http status code can be constant
                return resp

            # Retry loop using same session/adapter to preserve pools
            attempts = 1
            conn = getattr(resp, "connection", None)
            # Close/consume original response to free the connection
            with contextlib.suppress(Exception):
                resp.close()

            if conn is None:
                return resp
            if request is None or not hasattr(request, "copy"):
                return resp
            last_error = None
            while attempts < self.retry_attempts:
                new_key = self.pool._take_key(self.endpoint)
                req = request.copy()
                self._inject(req, new_key)
                try:
                    new_resp = conn.send(req, **kwargs)
                except Exception as exc:
                    self.pool._mark_result(new_key, None, {}, exc)
                    last_error = exc
                    attempts += 1
                    continue
                self.pool._mark_result(
                    new_key, new_resp.status_code, dict(new_resp.headers or {}), None
                )
                if new_resp.status_code != 429:  # noqa: PLR2004, http status code can be constant
                    return new_resp
                with contextlib.suppress(Exception):
                    new_resp.close()
                attempts += 1
            if last_error is not None:
                raise last_error
            return new_resp if "new_resp" in locals() else resp

        # Only requests' PreparedRequest supports register_hook
        if hasattr(r, "register_hook"):
            r.register_hook("response", _hook)
        return r

    # ------------------------ httpx sync ------------------------
    def auth_flow(self, request):
        if self._async:
            raise RuntimeError("Use an AsyncClient or async_auth_flow with an AsyncKeyPool.")
        attempts = 0
        while attempts < self.retry_attempts:
            key = self.pool._take_key(self.endpoint)
            req = request
            self._inject(req, key)
            try:
                response = yield req
            except Exception as exc:
                self.pool._mark_result(key, None, {}, exc)
                raise
            self.pool._mark_result(
                key,
                getattr(response, "status_code", None),
                dict(getattr(response, "headers", {}) or {}),
                None,
            )
            if request.method.upper() not in self.retry_for_methods:
                return
            if getattr(response, "status_code", None) != 429:  # noqa: PLR2004, http status code can be constant
                return
            # Close and retry with a fresh request copy
            with contextlib.suppress(Exception):
                response.close()
            attempts += 1
            continue

    # ------------------------ httpx async ------------------------
    async def async_auth_flow(self, request):
        if not self._async:
            raise RuntimeError("Use a Client or auth_flow with a KeyPool.")
        attempts = 0
        while attempts < self.retry_attempts:
            key = await self.pool._take_key(self.endpoint)
            req = request
            self._inject(req, key)
            try:
                response = yield req
            except Exception as exc:
                self.pool._mark_result(key, None, {}, exc)
                raise
            self.pool._mark_result(
                key,
                getattr(response, "status_code", None),
                dict(getattr(response, "headers", {}) or {}),
                None,
            )
            if request.method.upper() not in self.retry_for_methods:
                return
            if getattr(response, "status_code", None) != 429:  # noqa: PLR2004, http status code can be constant
                return
            # Close and retry with a fresh request copy
            with contextlib.suppress(Exception):
                await self._release_response(response)
            attempts += 1
            continue

    # ------------------------ aiohttp helpers ------------------------
    def headers(self) -> dict[str, str]:
        """Convenience: one-shot header injection (no automatic retry)."""
        if self._async:
            raise RuntimeError(
                "Use trace_config()/wrap_session() for automatic handling in aiohttp."
            )
        if self.auth_in == "query":
            raise RuntimeError(
                "Query auth cannot be represented by headers(); use a client request."
            )
        key = self.pool._take_key(self.endpoint)
        try:
            return {self.auth_header: f"{self.auth_scheme} {key.token}".strip()}
        finally:
            # A header-only helper has no response lifecycle through which to mark the key.
            self.pool._release_key(key)

    def trace_config(self):
        """Return an aiohttp.TraceConfig to mark successes/failures without auto-retry.

        Use together with wrap_session(session) for full transparent retries.
        """
        if self.auth_in == "query":
            raise RuntimeError(
                "aiohttp TraceConfig cannot add query parameters; use auth.request() "
                "or wrap_session() for query auth."
            )
        try:
            import aiohttp  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "aiohttp integration requires the optional dependency; "
                "install it with 'pip install rotisserie[aiohttp]'."
            ) from exc

        tc = aiohttp.TraceConfig()

        @tc.on_request_start.append
        async def _start(session, ctx, params):
            # if caller didn't pre-inject, inject here
            if self.auth_header not in params.headers:
                if self._async:
                    key = await self.pool._take_key(self.endpoint)
                else:
                    key = self.pool._take_key(self.endpoint)
                params.headers[self.auth_header] = f"{self.auth_scheme} {key.token}".strip()
                ctx._kd_key = key

        @tc.on_request_end.append
        async def _end(session, ctx, params):
            resp = params.response
            key = getattr(ctx, "_kd_key", None)
            if key is not None:
                self.pool._mark_result(
                    key,
                    getattr(resp, "status", None),
                    dict(getattr(resp, "headers", {})),
                    None,
                )

        return tc

    # --------- aiohttp decorator-based request helpers (no monkey-patch) ---------
    def request(self, session, method: str, url: str, **kwargs):
        """Return an async context manager that auto-rotates on 429 using the same session."""
        return _AiohttpRetryingContext(self, session, method, url, kwargs)

    def get(self, session, url: str, **kwargs):
        return self.request(session, "GET", url, **kwargs)

    def head(self, session, url: str, **kwargs):
        return self.request(session, "HEAD", url, **kwargs)

    def options(self, session, url: str, **kwargs):
        return self.request(session, "OPTIONS", url, **kwargs)

    def enable(self, session, methods=("get", "head", "options")):
        """Bind rotation-enabled methods onto an aiohttp session.
        After this, you can use:
            async with session.get(url) as resp: ...  # auto-rotates on 429
        or
            resp = await session.get(url)           # also supported
        Only the provided methods are wrapped. Call disable(session) to restore.
        """
        for m in methods:
            orig = getattr(session, m, None)
            if orig is None or getattr(session, f"_rotisserie_orig_{m}", None):
                continue
            setattr(session, f"_rotisserie_orig_{m}", orig)

            def _make(mname):
                def _wrapped(*args, **kwargs):
                    if not args:
                        raise TypeError(
                            f"aiohttp session.{mname} requires url as first positional argument"
                        )
                    url = args[0]
                    return self.request(session, mname.upper(), url, **kwargs)

                return _wrapped

            setattr(session, m, _make(m))
        return session

    def disable(self, session, methods=("get", "head", "options")):
        """Restore original aiohttp methods if previously enabled by enable_on."""
        for m in methods:
            orig = getattr(session, f"_rotisserie_orig_{m}", None)
            if orig is not None:
                setattr(session, m, orig)
                delattr(session, f"_rotisserie_orig_{m}")
        return session

    def wrap_session(self, session):
        """Patch session._request to auto-rotate/retry on 429 *using the same session pool*.

        This is opt-in because aiohttp lacks a first-class client-side auth flow.
        The patch is limited to this session instance and is reversible by restoring _request.
        """
        if hasattr(session, "_rotisserie_orig_request"):
            return session
        orig = session._request
        pool, endpoint = self.pool, self.endpoint
        auth_header, auth_scheme = self.auth_header, self.auth_scheme
        auth_in, auth_query_param = self.auth_in, self.auth_query_param
        retry_attempts = self.retry_attempts
        retry_for = self.retry_for_methods
        is_async = self._async
        release_response = self._release_response

        async def wrapped(method, url, **kw):
            attempt = 0
            last_resp = None
            last_error = None
            while attempt < retry_attempts:
                # inject/replace credential
                if is_async:
                    key = await pool._take_key(endpoint)
                else:
                    key = pool._take_key(endpoint)
                headers = {**kw.get("headers", {})}
                params = kw.get("params", {}) or {}
                params = dict(params) if isinstance(params, Mapping) else list(params)
                if auth_in == "query":
                    params = self._add_query_param(params, auth_query_param, key.token)
                else:
                    headers[auth_header] = f"{auth_scheme} {key.token}".strip()
                kw["headers"] = headers
                kw["params"] = params

                try:
                    underlying = getattr(session, "_rotisserie_orig_request", orig)
                    resp = await underlying(method, url, **kw)
                except Exception as exc:
                    pool._mark_result(key, None, {}, exc)
                    last_error = exc
                    if method.upper() not in retry_for:
                        raise
                    attempt += 1
                    continue
                # mark result & decide retry
                pool._mark_result(
                    key,
                    getattr(resp, "status", None),
                    dict(getattr(resp, "headers", {}) or {}),
                    None,
                )

                if method.upper() not in retry_for or resp.status != 429:  # noqa: PLR2004
                    return resp

                attempt += 1
                if attempt >= retry_attempts:
                    return resp
                # Release connection before retry
                with contextlib.suppress(Exception):
                    await release_response(resp)

                last_resp = resp
                continue

            if last_error is not None:
                raise last_error
            return last_resp

        session._rotisserie_orig_request = orig  # allow restore
        session._request = wrapped
        return session

    def unwrap_session(self, session):
        """Restore a session previously patched by wrap_session()."""
        orig = getattr(session, "_rotisserie_orig_request", None)
        if orig is not None:
            session._request = orig
            delattr(session, "_rotisserie_orig_request")
        return session

    # ------------------------ httpx helpers ------------------------
    def wrap_httpx(self, client):
        """Wrap httpx client's transport to retry on 429 using pool rotation.

        Supports both sync and async clients by intercepting transport methods.
        """
        transport = getattr(client, "_transport", None)
        if transport is None:  # nothing to do
            return client

        pool, endpoint = self.pool, self.endpoint
        auth_header, auth_scheme = self.auth_header, self.auth_scheme
        auth_in, auth_query_param = self.auth_in, self.auth_query_param
        retry_attempts = self.retry_attempts
        retry_for = {m.upper() for m in self.retry_for_methods}
        is_async_pool = self._async
        release_response = self._release_response

        # Sync wrapper
        if hasattr(transport, "handle_request"):
            if is_async_pool:
                raise RuntimeError("Use an AsyncClient with an AsyncKeyPool.")
            inner = transport

            class _RetryingTransport:
                def __init__(self, inner):
                    self._inner = inner

                # context management passthrough
                def __enter__(self):
                    if hasattr(self._inner, "__enter__"):
                        return self._inner.__enter__()
                    return self

                def __exit__(self, exc_type, exc, tb):
                    if hasattr(self._inner, "__exit__"):
                        return self._inner.__exit__(exc_type, exc, tb)
                    return False

                # transport API passthroughs used by httpx
                def close(self):
                    if hasattr(self._inner, "close"):
                        return self._inner.close()

                def handle_request(self, request):
                    attempts = 0
                    last = None
                    last_error = None
                    while attempts < retry_attempts:
                        # attach/replace header
                        key = pool._take_key(endpoint)
                        # Mutate request in place; httpx copies request internally per send.
                        if auth_in == "query":
                            request.url = request.url.copy_merge_params(
                                {auth_query_param: key.token}
                            )
                        else:
                            request.headers[auth_header] = f"{auth_scheme} {key.token}".strip()
                        try:
                            resp = self._inner.handle_request(request)
                        except Exception as exc:
                            pool._mark_result(key, None, {}, exc)
                            last_error = exc
                            if request.method.upper() not in retry_for:
                                raise
                            attempts += 1
                            continue
                        pool._mark_result(
                            key,
                            getattr(resp, "status_code", None),
                            getattr(resp, "headers", {}),
                            None,
                        )
                        if (
                            request.method.upper() not in retry_for
                            or getattr(resp, "status_code", None) != 429  # noqa: PLR2004, http status code can be constant
                        ):
                            return resp
                        attempts += 1
                        if attempts >= retry_attempts:
                            return resp
                        with contextlib.suppress(Exception):
                            resp.close()
                        last = resp
                    if last_error is not None and last is None:
                        raise last_error
                    return last

            client._transport = _RetryingTransport(inner)

        # Async wrapper
        if hasattr(transport, "handle_async_request"):
            inner_async = transport

            class _AsyncRetryingTransport:
                def __init__(self, inner):
                    self._inner = inner

                async def __aenter__(self):
                    if hasattr(self._inner, "__aenter__"):
                        return await self._inner.__aenter__()
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    if hasattr(self._inner, "__aexit__"):
                        return await self._inner.__aexit__(exc_type, exc, tb)
                    return False

                async def aclose(self):
                    if hasattr(self._inner, "aclose"):
                        return await self._inner.aclose()

                async def handle_async_request(self, request):
                    attempts = 0
                    last = None
                    last_error = None
                    while attempts < retry_attempts:
                        if is_async_pool:
                            key = await pool._take_key(endpoint)
                        else:
                            key = pool._take_key(endpoint)
                        # Mutate request in place; httpx copies request internally per send.
                        if auth_in == "query":
                            request.url = request.url.copy_merge_params(
                                {auth_query_param: key.token}
                            )
                        else:
                            request.headers[auth_header] = f"{auth_scheme} {key.token}".strip()
                        try:
                            resp = await self._inner.handle_async_request(request)
                        except Exception as exc:
                            pool._mark_result(key, None, {}, exc)
                            last_error = exc
                            if request.method.upper() not in retry_for:
                                raise
                            attempts += 1
                            continue
                        pool._mark_result(
                            key,
                            getattr(resp, "status_code", None),
                            getattr(resp, "headers", {}),
                            None,
                        )
                        if (
                            request.method.upper() not in retry_for
                            or getattr(resp, "status_code", None) != 429  # noqa: PLR2004, http status code can be constant
                        ):
                            return resp
                        attempts += 1
                        if attempts >= retry_attempts:
                            return resp
                        with contextlib.suppress(Exception):
                            await release_response(resp)
                        last = resp
                    if last_error is not None and last is None:
                        raise last_error
                    return last

            client._transport = _AsyncRetryingTransport(inner_async)

        return client


class _AiohttpRetryingContext:
    """Async context manager that performs an aiohttp request with automatic key rotation on 429.

    Usage:
        async with auth.request(session, "GET", url, **kwargs) as resp:
            data = await resp.text()
    """

    def __init__(self, auth, session, method: str, url: str, kwargs):
        self.auth = auth
        self.session = session
        self.method = method.upper()
        self.url = url
        self.kwargs = dict(kwargs)
        self._resp = None

    async def __aenter__(self):
        pool = self.auth.pool
        endpoint = self.auth.endpoint
        attempts = 0
        last_resp = None

        while attempts < self.auth.retry_attempts:
            # Acquire a key (async pool assumed for aiohttp usage)
            key = await pool._take_key(endpoint)
            # merge headers
            headers = {**self.kwargs.get("headers", {})}
            params = self.kwargs.get("params", {}) or {}
            params = dict(params) if isinstance(params, Mapping) else list(params)
            if self.auth.auth_in == "query":
                params = self.auth._add_query_param(
                    params, self.auth.auth_query_param, key.token
                )
            else:
                headers[self.auth.auth_header] = f"{self.auth.auth_scheme} {key.token}".strip()
            request_kwargs = {
                **self.kwargs,
                "headers": headers,
                "params": params,
            }

            try:
                resp = await self.session._request(self.method, self.url, **request_kwargs)
                pool._mark_result(
                    key,
                    getattr(resp, "status", None),
                    dict(getattr(resp, "headers", {}) or {}),
                    None,
                )
            except Exception as exc:
                pool._mark_result(key, None, {}, exc)
                raise

            if self.method not in self.auth.retry_for_methods or resp.status != 429:  # noqa: PLR2004, http status code can be constant
                self._resp = resp
                return resp

            attempts += 1
            if attempts >= self.auth.retry_attempts:
                self._resp = resp
                return resp
            # release and retry with a new key
            with contextlib.suppress(Exception):
                await self.auth._release_response(resp)
            last_resp = resp

        # exhausted retries; return the last response (likely 429)
        self._resp = last_resp
        return last_resp

    def __await__(self):
        async def _do():
            pool = self.auth.pool
            endpoint = self.auth.endpoint
            attempts = 0
            last_resp = None
            while attempts < self.auth.retry_attempts:
                key = await pool._take_key(endpoint)
                headers = {**self.kwargs.get("headers", {})}
                params = self.kwargs.get("params", {}) or {}
                params = dict(params) if isinstance(params, Mapping) else list(params)
                if self.auth.auth_in == "query":
                    params = self.auth._add_query_param(
                        params, self.auth.auth_query_param, key.token
                    )
                else:
                    headers[self.auth.auth_header] = f"{self.auth.auth_scheme} {key.token}".strip()
                resp = await self.session._request(
                    self.method,
                    self.url,
                    **{**self.kwargs, "headers": headers, "params": params},
                )
                pool._mark_result(
                    key,
                    getattr(resp, "status", None),
                    dict(getattr(resp, "headers", {})),
                    None,
                )
                if self.method not in self.auth.retry_for_methods or resp.status != 429:  # noqa: PLR2004, http status code can be constant
                    return resp
                attempts += 1
                if attempts >= self.auth.retry_attempts:
                    return resp
                with contextlib.suppress(Exception):
                    await self.auth._release_response(resp)
                last_resp = resp
            return last_resp

        return _do().__await__()

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._resp is not None and not getattr(self._resp, "closed", False):
                await self.auth._release_response(self._resp)
        except Exception:
            pass
        return False
