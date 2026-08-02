import contextlib
import inspect
from collections.abc import Mapping

from .pool import AsyncKeyPool, KeyPool, _require_dependency


async def _release_response(response):
    """Release a response whether the client library exposes sync or async release()."""
    for name in ("aclose", "release", "close"):
        release = getattr(response, name, None)
        if release is None:
            continue
        result = release()
        if inspect.isawaitable(result):
            await result
        return


def _auth_values(pool, key, headers, params):
    config = getattr(pool, "_auth_config", None)
    if config is not None and config.in_ == "query":
        if isinstance(params, Mapping):
            params[config.query_param] = key.token
        else:
            params[:] = [
                (name, value) for name, value in params if name != config.query_param
            ]
            params.append((config.query_param, key.token))
    else:
        header = config.header if config else "Authorization"
        scheme = config.scheme if config else "Bearer"
        headers[header] = f"{scheme} {key.token}".strip()


# ---------- requests (sync) ----------
class RequestsClientContext:
    def __init__(self, pool: KeyPool, endpoint: str, reserve: int, priority: int, session=None):
        self.pool = pool
        self.endpoint = endpoint
        self.reserve = reserve
        self.priority = priority
        self.session = session

    def __enter__(self):
        self._lease = self.pool.endpoint(
            self.endpoint, reserve=self.reserve, priority=self.priority
        )
        client = self._lease.__enter__()
        # bind session to client so it can reuse
        self.client = client
        if self.session is None:
            requests = _require_dependency("requests", "requests")
            self.client._session = requests.Session()
            self._own_session = True
        else:
            self.client._session = self.session
            self._own_session = False
        return self

    def __exit__(self, exc_type, exc, tb):
        r = self._lease.__exit__(exc_type, exc, tb)
        if getattr(self, "_own_session", False):
            with contextlib.suppress(Exception):
                self.client._session.close()
        return r

    # Delegate request methods but inject the session if provided
    def request(self, method, url, **kwargs):
        # Prefer explicitly provided session arg from caller; else use bound session
        if getattr(self.client, "_session", None) is not None and "session" not in kwargs:
            kwargs["session"] = self.client._session
        return self.client.request(method, url, **kwargs)

    def get(self, url, **kw):
        return self.request("GET", url, **kw)

    def post(self, url, **kw):
        return self.request("POST", url, **kw)

    def put(self, url, **kw):
        return self.request("PUT", url, **kw)

    def delete(self, url, **kw):
        return self.request("DELETE", url, **kw)


# ---------- httpx (async) ----------
class HttpxClientContext:
    def __init__(
        self,
        pool: AsyncKeyPool,
        endpoint: str,
        reserve: int,
        priority: int,
        client=None,
    ):
        self.pool = pool
        self.endpoint = endpoint
        self.reserve = reserve
        self.priority = priority
        self.client = client

    async def __aenter__(self):
        self._lease = self.pool.aendpoint(
            self.endpoint, reserve=self.reserve, priority=self.priority
        )
        self._client = await self._lease.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        r = await self._lease.__aexit__(exc_type, exc, tb)
        if getattr(self, "_internal_client", None) is not None:
            with contextlib.suppress(Exception):
                await self._internal_client.aclose()
            self._internal_client = None
        return r

    async def request(self, method, url, **kwargs):
        httpx = _require_dependency("httpx", "httpx")

        client = self.client if self.client is not None else getattr(self, "_internal_client", None)
        if client is None:
            self._internal_client = client = httpx.AsyncClient()
        method = method.upper()
        base_kwargs = dict(kwargs)
        supplied_headers = dict(base_kwargs.pop("headers", {}) or {})
        supplied_params = base_kwargs.pop("params", {}) or {}
        if not isinstance(supplied_params, Mapping):
            supplied_params = list(supplied_params)
        attempts, last_err, last_resp = 0, None, None
        max_attempts = getattr(self.pool, "_max_attempts", 8)
        retry_methods = getattr(self.pool, "_retry_methods", {"GET", "HEAD", "OPTIONS"})
        while attempts < max_attempts:
            key = await self.pool._take_key(self.endpoint)
            headers = dict(supplied_headers)
            params = (
                dict(supplied_params)
                if isinstance(supplied_params, Mapping)
                else list(supplied_params)
            )
            _auth_values(self.pool, key, headers, params)
            try:
                resp = await client.request(
                    method, url, headers=headers, params=params, **base_kwargs
                )
                self.pool._mark_result(key, resp.status_code, resp.headers, None)
                last_resp = resp
                if (
                    method not in retry_methods
                    or resp.status_code != 429  # noqa: PLR2004, http status code can be constant
                ):
                    return resp
                attempts += 1
                if attempts >= max_attempts:
                    return resp
                with contextlib.suppress(Exception):
                    await _release_response(resp)
                continue
            except httpx.TransportError as e:
                self.pool._mark_result(key, None, {}, e)
                attempts += 1
                last_err = e
                if method not in retry_methods:
                    raise
                continue
            except Exception as exc:
                # Always release the key before surfacing programming/client errors.
                if key.in_use_by is not None:
                    self.pool._mark_result(key, None, {}, exc)
                raise
        if last_err:
            raise last_err
        if last_resp is not None:
            return last_resp
        raise RuntimeError("rotisserie: failed after retries")

    async def get(self, url, **kw):
        return await self.request("GET", url, **kw)

    async def post(self, url, **kw):
        return await self.request("POST", url, **kw)

    async def put(self, url, **kw):
        return await self.request("PUT", url, **kw)

    async def delete(self, url, **kw):
        return await self.request("DELETE", url, **kw)


# ---------- aiohttp (async) ----------
# We provide a wrapper that preserves the 'async with ... as resp'
# pattern by returning a context async manager per request.
class _AiohttpRequestCtx:
    def __init__(self, outer, method, url, kwargs):
        self.outer = outer
        self.method = method
        self.url = url
        self.kwargs = kwargs
        self._resp = None
        self._key = None
        self._marked = False

    async def __aenter__(self):
        method = self.method.upper()
        base_kwargs = dict(self.kwargs)
        supplied_headers = dict(base_kwargs.pop("headers", {}) or {})
        supplied_params = base_kwargs.pop("params", {}) or {}
        if not isinstance(supplied_params, Mapping):
            supplied_params = list(supplied_params)
        retry_methods = getattr(self.outer.pool, "_retry_methods", {"GET", "HEAD", "OPTIONS"})
        attempts = 0
        while attempts < getattr(self.outer.pool, "_max_attempts", 8):
            self._key = await self.outer.pool._take_key(self.outer.endpoint)
            headers = dict(supplied_headers)
            params = (
                dict(supplied_params)
                if isinstance(supplied_params, Mapping)
                else list(supplied_params)
            )
            _auth_values(self.outer.pool, self._key, headers, params)
            try:
                self._resp = await self.outer.session.request(
                    method, self.url, headers=headers, params=params, **base_kwargs
                )
                status = getattr(self._resp, "status", None)
                self.outer.pool._mark_result(
                    self._key,
                    status,
                    dict(getattr(self._resp, "headers", {}) or {}),
                    None,
                )
                self._marked = True
                if method not in retry_methods or status != 429:  # noqa: PLR2004
                    return self._resp
                attempts += 1
                if attempts >= getattr(self.outer.pool, "_max_attempts", 8):
                    return self._resp
                with contextlib.suppress(Exception):
                    await _release_response(self._resp)
            except Exception as exc:
                if self._key.in_use_by is not None:
                    self.outer.pool._mark_result(self._key, None, {}, exc)
                raise
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        # Ensure response is released/closed
        try:
            if self._resp is not None and not getattr(self._resp, "closed", False):
                await _release_response(self._resp)
        finally:
            status = getattr(self._resp, "status", None) if self._resp else None
            headers = dict(getattr(self._resp, "headers", {}) or {})
            if not self._marked and self._key is not None:
                self.outer.pool._mark_result(self._key, status, headers, exc)
        return False


class AiohttpClientContext:
    def __init__(
        self,
        pool: AsyncKeyPool,
        endpoint: str,
        reserve: int,
        priority: int,
        session=None,
    ):
        self.pool = pool
        self.endpoint = endpoint
        self.reserve = reserve
        self.priority = priority
        self.session = session

    async def __aenter__(self):
        if self.session is None:
            aiohttp = _require_dependency("aiohttp", "aiohttp")
            self.session = aiohttp.ClientSession()
            self._own_session = True
        else:
            self._own_session = False
        self._lease = self.pool.aendpoint(
            self.endpoint, reserve=self.reserve, priority=self.priority
        )
        await self._lease.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        r = await self._lease.__aexit__(exc_type, exc, tb)
        if self._own_session:
            await self.session.close()
        return r

    # Return an async context manager per call, preserving aiohttp's pattern
    def request(self, method, url, **kwargs):
        return _AiohttpRequestCtx(self, method, url, kwargs)

    def get(self, url, **kw):
        return self.request("GET", url, **kw)

    def post(self, url, **kw):
        return self.request("POST", url, **kw)

    def put(self, url, **kw):
        return self.request("PUT", url, **kw)

    def delete(self, url, **kw):
        return self.request("DELETE", url, **kw)
