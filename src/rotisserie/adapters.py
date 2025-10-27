import contextlib

from .pool import AsyncKeyPool, KeyPool


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
            import requests  # noqa: PLC0415

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
        import httpx  # noqa: PLC0415

        client = self.client or getattr(self, "_internal_client", None)
        if client is None:
            self._internal_client = client = httpx.AsyncClient()
        attempts, last_err = 0, None
        while attempts < getattr(self.pool, "_max_attempts", 8):
            key = await self.pool._take_key(self.endpoint)
            headers = {**kwargs.pop("headers", {})}
            params = {**kwargs.pop("params", {})}
            ac = getattr(self.pool, "_auth_config", None)
            if ac and ac.in_ == "query":
                params[ac.query_param] = key.token
            else:
                h = ac.header if ac else getattr(self.pool, "_auth_header", "Authorization")
                s = ac.scheme if ac else getattr(self.pool, "_auth_scheme", "Bearer")
                headers[h] = f"{s} {key.token}".strip()
            try:
                resp = await client.request(method, url, headers=headers, params=params, **kwargs)
                self.pool._mark_result(key, resp.status_code, resp.headers, None)
                if resp.status_code == 429:  # noqa: PLR2004, http status code can be constant
                    with contextlib.suppress(Exception):
                        self.pool._logger.info(
                            f"429 on endpoint={self.endpoint} key={key.name}; rotating"
                        )
                    attempts += 1
                    continue
                return resp
            except (httpx.TransportError, httpx.TimeoutException) as e:
                self.pool._mark_result(key, None, {}, e)
                attempts += 1
                last_err = e
                continue
        if last_err:
            raise last_err
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

    async def __aenter__(self):
        self._key = await self.outer.pool._take_key(self.outer.endpoint)
        headers = {**self.kwargs.pop("headers", {})}
        params = {**self.kwargs.pop("params", {})}
        ac = getattr(self.outer.pool, "_auth_config", None)
        if ac and ac.in_ == "query":
            params[ac.query_param] = self._key.token
        else:
            h = ac.header if ac else getattr(self.outer.pool, "_auth_header", "Authorization")
            s = ac.scheme if ac else getattr(self.outer.pool, "_auth_scheme", "Bearer")
            headers[h] = f"{s} {self._key.token}".strip()
        self._resp = await self.outer.session.request(
            self.method, self.url, headers=headers, params=params, **self.kwargs
        )
        # user will await/read and exit; mark result on exit
        return self._resp

    async def __aexit__(self, exc_type, exc, tb):
        # Ensure response is released/closed
        try:
            if self._resp is not None and not self._resp.closed:
                await self._resp.release()
        finally:
            status = getattr(self._resp, "status", None) if self._resp else None
            headers = dict(getattr(self._resp, "headers", {}) or {})
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
            import aiohttp  # noqa: PLC0415

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
