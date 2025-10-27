import asyncio
import contextlib

from .types import AuthConfig, RetryConfig


class UniversalAuth:
    """One object that plugs into requests + httpx directly, with helpers for aiohttp.

    - requests: uses __call__(request) protocol + response hook to auto-rotate/retry on 429.
    - httpx: implements auth_flow / async_auth_flow to reissue on 429 inside the auth layer.
    - aiohttp: provide trace_config() (for accounting) and wrap_session(session) to implement
        transparent 429 rotation using the SAME session/connector (so pool reuse is preserved).

    Other keywords for kwargs:
    - auth_config: AuthConfig object
    - auth_header: str
    - auth_scheme: str
    - retry_config: RetryConfig object
    - retry_after_base: float
    - retry_after_growth: float
    - retry_after_cap: float
    - error_base: float
    - error_growth: float
    - error_cap: float
    - retry_attempts: int
    - retry_for_methods: Iterable[str]

    """

    def __init__(self, pool, endpoint: str, reserve: int, priority: int, **kwargs):
        self.pool = pool
        self.endpoint = endpoint
        self.reserve = reserve
        self.priority = priority
        # Prefer config objects, then inherit from pool, finally fall back to args
        pool_auth: AuthConfig | None = getattr(pool, "_auth_config", None)
        use_auth: AuthConfig | None = kwargs.get("auth_config", pool_auth)
        if use_auth is not None:
            self.auth_header = use_auth.header
            self.auth_scheme = use_auth.scheme
        else:
            self.auth_header = kwargs.get("auth_header", "Authorization")
            self.auth_scheme = kwargs.get("auth_scheme", "Bearer")
        pool_retry: RetryConfig | None = getattr(pool, "_retry_config", None)
        use_retry: RetryConfig | None = kwargs.get("retry_config", pool_retry)
        if use_retry is not None:
            self.retry_attempts = use_retry.retry_attempts
            self.retry_for_methods = {m.upper() for m in use_retry.retry_for_methods}
        else:
            self.retry_attempts = kwargs.get("retry_attempts", 8)
            # By default we retry idempotent methods only; caller can extend to POST, etc.
            self.retry_for_methods = {
                m.upper() for m in (kwargs.get("retry_for_methods", ["GET", "HEAD", "OPTIONS"]))
            }
        self._lease = None
        self._async = hasattr(pool, "_lock") and asyncio.iscoroutinefunction(
            getattr(pool, "_take_key", None)
        )

    # -------- context mgmt ties lifetime to endpoint presence (priority + reserves) --------
    def __enter__(self):
        if self._async:
            raise RuntimeError("Use 'async with' for an AsyncKeyPool.")
        self._lease = self.pool.endpoint(self.endpoint, self.reserve, self.priority)
        self._client = self._lease.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
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
        return await self._lease.__aexit__(exc_type, exc, tb)

    # ------------------------ requests auth protocol ------------------------
    def __call__(self, r):
        """Inject header and attach a response hook that may transparently resend on 429.

        This runs on the same requests.Session and reuses its connection pool.
        """
        # Allow httpx to call into __call__ as part of auth_flow; don't block on async pool here.
        # Inject header. For async pools (httpx), we cannot await here, so use a best-effort pick.
        try:
            if self._async:
                # Non-blocking candidate; fall back to first key if none ready
                key = getattr(self.pool, "_pick_candidate", lambda ep: None)(self.endpoint)
                if key is None:
                    key = next(iter(self.pool._keys))
            else:
                key = self.pool._take_key(self.endpoint)
            r.headers[self.auth_header] = f"{self.auth_scheme} {key.token}".strip()
        except Exception:
            pass

        def _hook(resp, *args, **kwargs):
            # Mark the result for the original attempt
            self.pool._mark_result(key, resp.status_code, resp.headers, None)

            if resp.request.method.upper() not in self.retry_for_methods:
                return resp

            if resp.status_code != 429:  # noqa: PLR2004, http status code can be constant
                return resp

            # Retry loop using same session/adapter to preserve pools
            attempts = 1
            # Close/consume original response to free the connection
            with contextlib.suppress(Exception):
                resp.close()

            while attempts < self.retry_attempts:
                new_key = self.pool._take_key(self.endpoint)
                req = resp.request.copy()
                req.headers[self.auth_header] = f"{self.auth_scheme} {new_key.token}".strip()
                # Some clients (e.g., httpx when going through requests Auth path)
                # won't have connection
                conn = getattr(resp, "connection", None)
                if conn is None:
                    return resp
                new_resp = conn.send(req, **kwargs)
                self.pool._mark_result(new_key, new_resp.status_code, new_resp.headers, None)
                if new_resp.status_code != 429:  # noqa: PLR2004, http status code can be constant
                    return new_resp
                with contextlib.suppress(Exception):
                    new_resp.close()
                attempts += 1
            return new_resp

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
            req = request.copy()
            req.headers[self.auth_header] = f"{self.auth_scheme} {key.token}".strip()
            response = yield req
            self.pool._mark_result(
                key,
                getattr(response, "status_code", None),
                getattr(response, "headers", {}),
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
            req = request.copy()
            req.headers[self.auth_header] = f"{self.auth_scheme} {key.token}".strip()
            response = yield req
            self.pool._mark_result(
                key,
                getattr(response, "status_code", None),
                getattr(response, "headers", {}),
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

    # ------------------------ aiohttp helpers ------------------------
    def headers(self) -> dict[str, str]:
        """Convenience: one-shot header injection (no automatic retry)."""
        if self._async:
            raise RuntimeError(
                "Use trace_config()/wrap_session() for automatic handling in aiohttp."
            )
        key = self.pool._take_key(self.endpoint)
        # We *mark* this attempt once a response arrives via trace_config/wrap_session.
        return {self.auth_header: f"{self.auth_scheme} {key.token}".strip()}

    def trace_config(self):
        """Return an aiohttp.TraceConfig to mark successes/failures without auto-retry.

        Use together with wrap_session(session) for full transparent retries.
        """
        import aiohttp  # noqa: PLC0415

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
        orig = session._request
        pool, endpoint = self.pool, self.endpoint
        auth_header, auth_scheme = self.auth_header, self.auth_scheme
        retry_attempts = self.retry_attempts
        retry_for = self.retry_for_methods
        is_async = self._async

        async def wrapped(method, url, **kw):
            attempt = 0
            last_resp = None
            while attempt < retry_attempts:
                # inject/replace header
                if is_async:
                    key = await pool._take_key(endpoint)
                else:
                    key = pool._take_key(endpoint)
                headers = {**kw.get("headers", {})}
                headers[auth_header] = f"{auth_scheme} {key.token}".strip()
                kw["headers"] = headers

                resp = await orig(method, url, **kw)
                # mark result & decide retry
                pool._mark_result(
                    key,
                    getattr(resp, "status", None),
                    dict(getattr(resp, "headers", {})),
                    None,
                )

                if method.upper() not in retry_for or resp.status != 429:  # noqa: PLR2004, http status code can be constant
                    return resp

                # Release connection before retry
                with contextlib.suppress(Exception):
                    await resp.release()

                last_resp = resp
                attempt += 1
                continue

            return last_resp or resp

        session._rotisserie_orig_request = orig  # allow restore
        session._request = wrapped
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
        retry_attempts = self.retry_attempts
        retry_for = {m.upper() for m in self.retry_for_methods}
        is_async_pool = self._async

        # Sync wrapper
        if hasattr(transport, "handle_request"):
            inner = transport

            class _RetryingTransport:
                def __init__(self, inner):
                    self._inner = inner

                # context management passthrough
                def __enter__(self):
                    if hasattr(self._inner, "__enter"):
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
                    while attempts < retry_attempts:
                        # attach/replace header
                        if is_async_pool:
                            # best effort: pick candidate without blocking
                            key = getattr(pool, "_pick_candidate", lambda ep: None)(endpoint)
                            if key is None:
                                key = next(iter(pool._keys))
                        else:
                            key = pool._take_key(endpoint)
                        # Mutate header in place; httpx copies request internally per send
                        request.headers[auth_header] = f"{auth_scheme} {key.token}".strip()
                        resp = self._inner.handle_request(request)
                        pool._mark_result(
                            key,
                            getattr(resp, "status_code", None),
                            getattr(resp, "headers", {}),
                            None,
                        )
                        if (
                            request.method.upper() not in retry_for
                            or getattr(resp, "status_code", None) != 429 # noqa: PLR2004, http status code can be constant
                        ):
                            return resp
                        with contextlib.suppress(Exception):
                            resp.close()
                        attempts += 1
                        last = resp
                    return last or resp

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
                    while attempts < retry_attempts:
                        if is_async_pool:
                            key = await pool._take_key(endpoint)
                        else:
                            key = pool._take_key(endpoint)
                        # Mutate header in place; httpx copies request internally per send
                        request.headers[auth_header] = f"{auth_scheme} {key.token}".strip()
                        resp = await self._inner.handle_async_request(request)
                        pool._mark_result(
                            key,
                            getattr(resp, "status_code", None),
                            getattr(resp, "headers", {}),
                            None,
                        )
                        if (
                            request.method.upper() not in retry_for
                            or getattr(resp, "status_code", None) != 429 # noqa: PLR2004, http status code can be constant
                        ):
                            return resp
                        with contextlib.suppress(Exception):
                            await resp.aclose() if hasattr(resp, "aclose") else resp.close()
                        attempts += 1
                        last = resp
                    return last or resp

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
            headers[self.auth.auth_header] = f"{self.auth.auth_scheme} {key.token}".strip()
            self.kwargs["headers"] = headers

            resp = await self.session._request(self.method, self.url, **self.kwargs)
            pool._mark_result(
                key,
                getattr(resp, "status", None),
                dict(getattr(resp, "headers", {})),
                None,
            )

            if self.method not in self.auth.retry_for_methods or resp.status != 429:  # noqa: PLR2004, http status code can be constant
                self._resp = resp
                return resp

            # release and retry with a new key
            with contextlib.suppress(Exception):
                await resp.release()
            last_resp = resp
            attempts += 1

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
                headers[self.auth.auth_header] = f"{self.auth.auth_scheme} {key.token}".strip()
                self.kwargs["headers"] = headers
                resp = await self.session._request(self.method, self.url, **self.kwargs)
                pool._mark_result(
                    key,
                    getattr(resp, "status", None),
                    dict(getattr(resp, "headers", {})),
                    None,
                )
                if self.method not in self.auth.retry_for_methods or resp.status != 429:  # noqa: PLR2004, http status code can be constant
                    return resp
                with contextlib.suppress(Exception):
                    await resp.release()
                last_resp = resp
                attempts += 1
            return last_resp

        return _do().__await__()

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._resp is not None and not self._resp.closed:
                await self._resp.release()
        except Exception:
            pass
        return False
